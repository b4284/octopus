using System.Collections.Concurrent;
using Google.Protobuf;
using Octopus.V1;

namespace Octopus.Client;

/// <summary>
/// Publishes events. A call returns only after the events are committed to the database; until then it is retried.
/// Retries are safe: publishing is idempotent per (source, seq).
/// </summary>
public sealed class OctopusProducer
{
    private readonly OctopusConnection _conn;
    private readonly string _inbox;
    private readonly ConcurrentDictionary<ulong, TaskCompletionSource<PublishReply>> _pending = new();
    private long _nextId;

    public TimeSpan ReplyTimeout { get; set; } = TimeSpan.FromSeconds(5);
    public int MaxAttempts { get; set; } = 20;
    public long Retries;

    private OctopusProducer(OctopusConnection conn, string id)
    {
        _conn = conn;
        _inbox = conn.Topics.Inbox(id);
    }

    /// <param name="id">Unique among producers (names the reply inbox).</param>
    public static async Task<OctopusProducer> CreateAsync(OctopusConnection conn, string id, CancellationToken ct = default)
    {
        var p = new OctopusProducer(conn, id);
        await conn.RegisterInboxAsync(p._inbox, payload =>
        {
            Envelope env;
            try { env = Envelope.Parser.ParseFrom(payload); } catch (InvalidProtocolBufferException) { return; }
            if (env.PublishReply is { } r && p._pending.TryRemove(r.RequestId, out var tcs)) tcs.TrySetResult(r);
        }, ct);
        return p;
    }

    /// <summary>Publishes and returns one status per item, in order, once committed.</summary>
    public async Task<IReadOnlyList<PublishStatus>> PublishAsync(IReadOnlyList<PublishItem> items, CancellationToken ct = default)
    {
        if (items.Count == 0) return [];
        var req = new PublishRequest { ReplyTo = _inbox };
        req.Items.AddRange(items);
        string topic = _conn.Topics.Publish(items[0].Group);
        var backoff = TimeSpan.FromMilliseconds(50);
        for (int attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            req.RequestId = (ulong)Interlocked.Increment(ref _nextId);
            var tcs = new TaskCompletionSource<PublishReply>(TaskCreationOptions.RunContinuationsAsynchronously);
            _pending[req.RequestId] = tcs;
            try
            {
                await _conn.PublishAsync(topic, req, ct);
                var reply = await tcs.Task.WaitAsync(ReplyTimeout, ct);
                if (reply.Error.Length == 0) return reply.Status;
                if (reply.Error.StartsWith("invalid", StringComparison.Ordinal)) throw new ArgumentException(reply.Error);
            }
            catch (TimeoutException) { }
            catch (MQTTnet.Exceptions.MqttCommunicationException) { }
            finally { _pending.TryRemove(req.RequestId, out _); }
            Interlocked.Increment(ref Retries);
            await Task.Delay(backoff, ct);
            backoff = TimeSpan.FromMilliseconds(Math.Min(2000, backoff.TotalMilliseconds * 2));
        }
        throw new TimeoutException($"publish not acknowledged after {MaxAttempts} attempts");
    }
}
