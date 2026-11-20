using System.Buffers;
using System.Collections.Concurrent;
using Google.Protobuf;
using MQTTnet;
using MQTTnet.Protocol;
using Octopus.V1;

namespace Octopus.Client;

public sealed class OctopusOptions
{
    public string Host { get; set; } = "127.0.0.1";
    public int Port { get; set; } = 1883;
    /// <summary>Unique per connection (MQTT client id; also names the connection's inbox).</summary>
    public string ClientId { get; set; } = $"octopus-{Guid.NewGuid():N}";
    public string Namespace { get; set; } = "octopus";
    public string? Username { get; set; }
    public string? Password { get; set; }
    /// <summary>0 ("direct", default) or 1. The fabric is never the durability point: replies come after the database
    /// commit and every message is retried, so QoS 1 only adds broker acknowledgements and an in-flight window.</summary>
    public int QoS { get; set; } = 0;
    /// <summary>Upper bound for one coalesced message (bytes, approximate).</summary>
    public int MaxBatchBytes { get; set; } = 4 << 20;
    /// <summary>
    /// A destination sent to less than this long ago waits out the rest of it, so everything queued meanwhile goes
    /// as one message (like Kafka's linger.ms). An idle destination is sent to at once. Bounds the message rate per
    /// destination to 1/Linger at an added latency of at most Linger.
    /// </summary>
    public TimeSpan Linger { get; set; } = TimeSpan.FromMilliseconds(2);
}

/// <summary>Topic layout (see octopus.proto).</summary>
public sealed class Topics(string ns)
{
    public string Ns { get; } = ns;
    public string Publish(string group) => $"{Ns}/pub/{group}";
    public string Any => $"{Ns}/any";
    public string Inbox(string id) => $"{Ns}/inbox/{id}";
    public string MembersFilter => $"{Ns}/member/+";
    public string OwnersFilter => $"{Ns}/owner/+";
}

/// <summary>
/// One MQTT 5 connection shared by any number of producers and workers, with one inbox for all of them.
/// Reconnects and resubscribes on its own; tracks the live dispatchers and the owner of every group from their
/// retained announcements; and coalesces outgoing messages per destination without timers (whatever queued while
/// the previous send to that topic was in progress goes out as one message).
/// </summary>
public sealed class OctopusConnection : IAsyncDisposable
{
    private readonly OctopusOptions _o;
    private readonly IMqttClient _client;
    private readonly MqttClientOptions _options;
    private readonly ConcurrentDictionary<string, byte> _filters = new();
    private readonly ConcurrentDictionary<string, Outbox> _outboxes = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly MqttQualityOfServiceLevel _qos;
    private int _reconnecting;
    private long _nextRequest;

    internal readonly ConcurrentDictionary<ulong, TaskCompletionSource<PublishReply>> PendingPublishes = new();
    internal readonly ConcurrentDictionary<ulong, TaskCompletionSource<ResultStatus>> PendingResults = new();
    internal readonly ConcurrentDictionary<string, OctopusWorker> Workers = new();

    public Topics Topics { get; }
    /// <summary>This connection's inbox: replies and deliveries for every producer and worker on it.</summary>
    public string Inbox { get; }
    /// <summary>member id -> inbox, for live dispatchers.</summary>
    public ConcurrentDictionary<string, string> Members { get; } = new();
    /// <summary>group -> owning member id.</summary>
    public ConcurrentDictionary<string, string> Owners { get; } = new();
    /// <summary>Raised (on the receive loop: keep it short) when members or owners change.</summary>
    public event Action? TopologyChanged;
    public long Received, Sent, ItemsSent;

    private OctopusConnection(OctopusOptions o)
    {
        _o = o;
        Topics = new Topics(o.Namespace);
        Inbox = Topics.Inbox(o.ClientId);
        _qos = o.QoS == 0 ? MqttQualityOfServiceLevel.AtMostOnce : MqttQualityOfServiceLevel.AtLeastOnce;
        _client = new MqttClientFactory().CreateMqttClient();
        var b = new MqttClientOptionsBuilder()
            .WithTcpServer(o.Host, o.Port)
            .WithClientId(o.ClientId)
            .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
            .WithCleanStart(true)
            .WithKeepAlivePeriod(TimeSpan.FromSeconds(5))
            .WithTimeout(TimeSpan.FromSeconds(10));
        if (o.Username != null) b = b.WithCredentials(o.Username, o.Password);
        _options = b.Build();
        if (_options.ChannelOptions is MqttClientTcpOptions tcp)
        {
            tcp.NoDelay = true;
            // MQTTnet's default 8 KB socket buffer makes large sends superlinear (5 MB: 2.8 s); 1 MB: ~4 ms.
            tcp.BufferSize = 1 << 20;
        }
        _client.ApplicationMessageReceivedAsync += e =>
        {
            Interlocked.Increment(ref Received);
            OnMessage(e.ApplicationMessage.Topic, e.ApplicationMessage.Payload);
            return Task.CompletedTask;
        };
        _client.DisconnectedAsync += e =>
        {
            if (!_cts.IsCancellationRequested) _ = ReconnectAsync();
            return Task.CompletedTask;
        };
    }

    public static async Task<OctopusConnection> ConnectAsync(OctopusOptions o, CancellationToken ct = default)
    {
        var c = new OctopusConnection(o);
        await c._client.ConnectAsync(c._options, ct);
        await c.SubscribeAsync(c.Inbox, ct);
        await c.SubscribeAsync(c.Topics.MembersFilter, ct);
        await c.SubscribeAsync(c.Topics.OwnersFilter, ct);
        return c;
    }

    internal ulong NextRequestId() => (ulong)Interlocked.Increment(ref _nextRequest);

    private async Task ReconnectAsync()
    {
        if (Interlocked.Exchange(ref _reconnecting, 1) == 1) return;
        try
        {
            while (!_cts.IsCancellationRequested && !_client.IsConnected)
            {
                try
                {
                    await _client.ConnectAsync(_options, _cts.Token);
                    foreach (var f in _filters.Keys)
                        await _client.SubscribeAsync(new MqttClientSubscribeOptionsBuilder().WithTopicFilter(f, _qos).Build(), _cts.Token);
                }
                catch (Exception) when (!_cts.IsCancellationRequested)
                {
                    await Task.Delay(250);
                }
            }
        }
        catch (OperationCanceledException) { }
        finally { Volatile.Write(ref _reconnecting, 0); }
    }

    public async Task SubscribeAsync(string filter, CancellationToken ct = default)
    {
        _filters[filter] = 0;
        await _client.SubscribeAsync(new MqttClientSubscribeOptionsBuilder().WithTopicFilter(filter, _qos).Build(), ct);
    }

    // ---- outgoing: coalesced per topic ----

    /// <summary>Queue an envelope for <paramref name="topic"/>; it goes out with whatever else is queued for it.</summary>
    public void Send(string topic, Envelope envelope) => GetOutbox(topic, isPublish: false).Enqueue(envelope);

    /// <summary>Queue a publish request; requests for the same topic travel as one PublishBatch.</summary>
    internal void SendPublish(string topic, PublishRequest request) => GetOutbox(topic, isPublish: true).Enqueue(request);

    private Outbox GetOutbox(string topic, bool isPublish) => _outboxes.GetOrAdd(topic, t => new Outbox(this, t, isPublish));

    private async Task PublishRawAsync(string topic, byte[] payload)
    {
        var msg = new MqttApplicationMessageBuilder().WithTopic(topic).WithPayload(payload).WithQualityOfServiceLevel(_qos).Build();
        Interlocked.Increment(ref Sent);
        await _client.PublishAsync(msg, _cts.Token);
    }

    /// <summary>One destination: a queue and at most one send in progress.</summary>
    private sealed class Outbox(OctopusConnection conn, string topic, bool isPublish)
    {
        private readonly ConcurrentQueue<IMessage> _queue = new();
        private int _sending;
        private long _lastFlush;

        public void Enqueue(IMessage m)
        {
            _queue.Enqueue(m);
            if (Interlocked.CompareExchange(ref _sending, 1, 0) == 0) _ = Task.Run(DrainAsync);
        }

        private async Task DrainAsync()
        {
            while (true)
            {
                while (!_queue.IsEmpty)
                {
                    long wait = _lastFlush + (long)conn._o.Linger.TotalMilliseconds - Environment.TickCount64;
                    if (wait > 0) await Task.Delay((int)wait);
                    _lastFlush = Environment.TickCount64;
                    var items = new List<IMessage>();
                    int bytes = 0;
                    while (bytes < conn._o.MaxBatchBytes && _queue.TryDequeue(out var m))
                    {
                        items.Add(m);
                        bytes += m.CalculateSize();
                    }
                    byte[] payload;
                    if (isPublish)
                    {
                        var b = new PublishBatch();
                        foreach (var m in items) b.Requests.Add((PublishRequest)m);
                        payload = b.ToByteArray();
                    }
                    else if (items.Count == 1)
                    {
                        payload = items[0].ToByteArray();
                    }
                    else
                    {
                        var b = new EnvelopeBatch();
                        foreach (var m in items) b.Items.Add((Envelope)m);
                        payload = new Envelope { Batch = b }.ToByteArray();
                    }
                    Interlocked.Add(ref conn.ItemsSent, items.Count);
                    try { await conn.PublishRawAsync(topic, payload); }
                    catch (Exception) { /* lost: every sender retries (publish replies, delivery timeouts, credit refresh) */ }
                }
                Volatile.Write(ref _sending, 0);
                // something may have been queued after the last check
                if (_queue.IsEmpty || Interlocked.CompareExchange(ref _sending, 1, 0) != 0) return;
            }
        }
    }

    // ---- incoming ----

    private void OnMessage(string topic, ReadOnlySequence<byte> payload)
    {
        if (topic == Inbox)
        {
            Envelope env;
            try { env = Envelope.Parser.ParseFrom(payload); } catch (InvalidProtocolBufferException) { return; }
            Dispatch(env);
            return;
        }
        string prefix = Topics.Ns + "/";
        if (!topic.StartsWith(prefix, StringComparison.Ordinal)) return;
        var rest = topic.AsSpan(prefix.Length);
        if (rest.StartsWith("member/"))
        {
            string member = rest[7..].ToString();
            MemberInfo? info = null;
            try { if (payload.Length > 0) info = MemberInfo.Parser.ParseFrom(payload); } catch (InvalidProtocolBufferException) { }
            if (info is { Inbox.Length: > 0 }) Members[member] = info.Inbox;
            else Members.TryRemove(member, out _);
            TopologyChanged?.Invoke();
        }
        else if (rest.StartsWith("owner/"))
        {
            string group = rest[6..].ToString();
            OwnerInfo? info = null;
            try { if (payload.Length > 0) info = OwnerInfo.Parser.ParseFrom(payload); } catch (InvalidProtocolBufferException) { }
            if (info is { MemberId.Length: > 0 }) Owners[group] = info.MemberId;
            else Owners.TryRemove(group, out _);
            TopologyChanged?.Invoke();
        }
    }

    private void Dispatch(Envelope env)
    {
        switch (env.BodyCase)
        {
            case Envelope.BodyOneofCase.Batch:
                foreach (var e in env.Batch.Items) Dispatch(e);
                break;
            case Envelope.BodyOneofCase.PublishReply:
                if (PendingPublishes.TryRemove(env.PublishReply.RequestId, out var p)) p.TrySetResult(env.PublishReply);
                break;
            case Envelope.BodyOneofCase.ResultReply:
                foreach (var a in env.ResultReply.Items)
                    if (PendingResults.TryRemove(a.RequestId, out var r)) r.TrySetResult(a.Status);
                break;
            case Envelope.BodyOneofCase.Deliver:
                var d = env.Deliver;
                foreach (var item in d.Items)
                    if (Workers.TryGetValue(item.WorkerId, out var w)) w.OnDelivery(d.MemberId, d.Inbox, item);
                break;
        }
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        try { await _client.DisconnectAsync(); } catch { /* closing */ }
        _client.Dispose();
    }
}
