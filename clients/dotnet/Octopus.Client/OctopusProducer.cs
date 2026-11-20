using Octopus.V1;

namespace Octopus.Client;

/// <summary>
/// Publishes events. A call returns only after the events are committed to the database; until then it is retried.
/// Retries are safe: publishing is idempotent per (source, seq). Concurrent calls on one connection travel together
/// (one PublishBatch per send), so callers can publish each event the moment it exists.
/// </summary>
public sealed class OctopusProducer(OctopusConnection conn)
{
    public TimeSpan ReplyTimeout { get; set; } = TimeSpan.FromSeconds(5);
    public int MaxAttempts { get; set; } = 20;
    public long Retries;

    /// <summary>Publishes and returns one status per item, in order, once committed.</summary>
    public async Task<IReadOnlyList<PublishStatus>> PublishAsync(IReadOnlyList<PublishItem> items, CancellationToken ct = default)
    {
        if (items.Count == 0) return [];
        var req = new PublishRequest { ReplyTo = conn.Inbox };
        req.Items.AddRange(items);
        // one topic for all of this connection's publishes, so they coalesce (dispatchers accept any group in any
        // request; the group level of the topic is informational)
        string topic = conn.Topics.Publish("_");
        var backoff = TimeSpan.FromMilliseconds(50);
        for (int attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            req.RequestId = conn.NextRequestId();
            var tcs = new TaskCompletionSource<PublishReply>(TaskCreationOptions.RunContinuationsAsynchronously);
            conn.PendingPublishes[req.RequestId] = tcs;
            try
            {
                conn.SendPublish(topic, req.Clone());
                var reply = await tcs.Task.WaitAsync(ReplyTimeout, ct);
                if (reply.Error.Length == 0) return reply.Status;
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
}
