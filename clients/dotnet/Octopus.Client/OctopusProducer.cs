using Octopus.V1;

namespace Octopus.Client;

/// <summary>
/// What Octopus answered for one published item. <see cref="MissingBelow"/>: positions directly below it (same epoch)
/// that are neither stored nor applied. If the producer still has them in flight, nothing to do; if it lost them, it
/// replays them or publishes a skip marker (an item with <c>SkipTo</c> set).
/// </summary>
public readonly record struct PublishAck(PublishStatus Status, long MissingBelow);

/// <summary>
/// Publishes events. A call returns only after the events are committed to the database; until then it is retried.
/// Retries are safe: publishing is idempotent per (source, seq). Concurrent calls on one connection travel together
/// (one PublishBatch per send), so callers can publish each event the moment it exists.
/// </summary>
public sealed class OctopusProducer(OctopusConnection conn)
{
    public TimeSpan ReplyTimeout { get; set; } = TimeSpan.FromSeconds(5);
    /// <summary>
    /// 0 (the default): retry until acknowledged. Giving up leaves a gap in the source's sequence, which holds back
    /// every later event of the source, so only tests should set a limit.
    /// </summary>
    public int MaxAttempts { get; set; } = 0;
    /// <summary>A publish not acknowledged after this long is reported through <see cref="Stuck"/>.</summary>
    public TimeSpan AlertAfter { get; set; } = TimeSpan.FromSeconds(30);
    /// <summary>Raised once per publish that is still not acknowledged after <see cref="AlertAfter"/> (it keeps retrying).</summary>
    public event Action<IReadOnlyList<PublishItem>, TimeSpan>? Stuck;
    public long Retries;
    /// <summary>Publishes currently retrying for longer than <see cref="AlertAfter"/> (a gauge for alerting).</summary>
    public long StuckNow => Interlocked.Read(ref _stuck);
    private long _stuck;

    // positions published by this producer and not acknowledged yet
    private readonly System.Collections.Concurrent.ConcurrentDictionary<(string, long, long), byte> _inFlight = new();

    /// <summary>Publishes and returns one answer per item, in order, once committed.</summary>
    public async Task<IReadOnlyList<PublishAck>> PublishAsync(IReadOnlyList<PublishItem> items, CancellationToken ct = default)
    {
        var keys = items.Where(i => !i.HighWater).Select(i => (i.Source, i.Epoch, i.Seq)).ToList();
        foreach (var k in keys) _inFlight[k] = 0;
        try { return await PublishOnceAsync(items, ct); }
        finally { foreach (var k in keys) _inFlight.TryRemove(k, out _); }
    }

    /// <summary>
    /// The source's last position is (epoch, seq): returns how many positions directly up to and including it are
    /// neither stored nor applied (a lost last event shows up here; no later publish would reveal it).
    /// </summary>
    public async Task<long> AnnounceAsync(string group, string source, long epoch, long seq, CancellationToken ct = default)
    {
        var acks = await PublishAsync([new PublishItem { Group = group, Source = source, Epoch = epoch, Seq = seq, HighWater = true }], ct);
        return acks.Count > 0 ? acks[0].MissingBelow : 0;
    }

    /// <summary>
    /// Repairs positions <paramref name="fromSeq"/>..<paramref name="toSeq"/> of an epoch that Octopus reported
    /// missing (a <see cref="PublishAck.MissingBelow"/>, or <see cref="AnnounceAsync"/>): positions this producer
    /// still has in flight are left alone (they arrive on their own); <paramref name="replay"/> supplies the body of
    /// each other one it can (e.g. from an outbox), and every run it cannot is declared lost with one skip marker.
    /// Returns (replayed, skipped).
    /// </summary>
    public async Task<(long Replayed, long Skipped)> RepairAsync(string group, string source, long epoch, long fromSeq, long toSeq,
        Func<long, Google.Protobuf.ByteString?> replay, CancellationToken ct = default)
    {
        var items = new List<PublishItem>();
        long replayed = 0, skipped = 0;
        long? runFrom = null, runTo = null;
        void Flush()
        {
            if (runFrom is null) return;
            skipped += runTo!.Value - runFrom.Value + 1;
            items.Add(new PublishItem { Group = group, Source = source, Epoch = epoch, Seq = runFrom.Value, SkipTo = runTo.Value });
            runFrom = runTo = null;
        }
        for (long q = fromSeq; q <= toSeq; q++)
        {
            if (_inFlight.ContainsKey((source, epoch, q))) { Flush(); continue; }
            var body = replay(q);
            if (body is not null)
            {
                Flush();
                replayed++;
                items.Add(new PublishItem { Group = group, Source = source, Epoch = epoch, Seq = q, Body = body });
            }
            else { runFrom ??= q; runTo = q; }
        }
        Flush();
        if (items.Count > 0) await PublishAsync(items, ct);
        return (replayed, skipped);
    }

    private async Task<IReadOnlyList<PublishAck>> PublishOnceAsync(IReadOnlyList<PublishItem> items, CancellationToken ct)
    {
        if (items.Count == 0) return [];
        var req = new PublishRequest { ReplyTo = conn.Inbox };
        req.Items.AddRange(items);
        // one topic for all of this connection's publishes, so they coalesce (dispatchers accept any group in any
        // request; the group level of the topic is informational)
        string topic = conn.Topics.Publish("_");
        var backoff = TimeSpan.FromMilliseconds(50);
        long started = Environment.TickCount64;
        bool alerted = false;
        try
        {
        for (int attempt = 1; MaxAttempts == 0 || attempt <= MaxAttempts; attempt++)
        {
            var waited = TimeSpan.FromMilliseconds(Environment.TickCount64 - started);
            if (!alerted && waited >= AlertAfter)
            {
                alerted = true;
                Interlocked.Increment(ref _stuck);
                try { Stuck?.Invoke(items, waited); } catch { /* the alert hook must not stop the retries */ }
            }
            req.RequestId = conn.NextRequestId();
            var tcs = new TaskCompletionSource<PublishReply>(TaskCreationOptions.RunContinuationsAsynchronously);
            conn.PendingPublishes[req.RequestId] = tcs;
            try
            {
                conn.SendPublish(topic, req.Clone());
                var reply = await tcs.Task.WaitAsync(ReplyTimeout, ct);
                if (reply.Error.Length == 0)
                    return reply.Status.Select((st, i) => new PublishAck(st, i < reply.MissingBelow.Count ? reply.MissingBelow[i] : 0)).ToList();
                if (reply.Error.StartsWith("invalid", StringComparison.Ordinal)) throw new ArgumentException(reply.Error);
            }
            catch (TimeoutException) { }
            finally { conn.PendingPublishes.TryRemove(req.RequestId, out _); }
            Interlocked.Increment(ref Retries);
            await Task.Delay(backoff, ct);
            backoff = TimeSpan.FromMilliseconds(Math.Min(2000, backoff.TotalMilliseconds * 2));
        }
        throw new TimeoutException($"publish not acknowledged after {MaxAttempts} attempts");
        }
        finally
        {
            if (alerted) Interlocked.Decrement(ref _stuck);
        }
    }
}
