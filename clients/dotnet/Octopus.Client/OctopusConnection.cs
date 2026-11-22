using System.Buffers;
using System.Collections.Concurrent;
using Google.Protobuf;
using MQTTnet;
using MQTTnet.Protocol;
using NATS.Client.Core;
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
    /// <summary>"mqtt" (default), "nats" or "zmq". With NATS, Host/Port name the NATS server (default port 4222). With
    /// ZeroMQ there is no broker: <see cref="ZmqPeers"/> lists the dispatchers.</summary>
    public string Transport { get; set; } = "mqtt";
    /// <summary>ZeroMQ: every dispatcher as <c>id@host:port</c>, comma-separated (ROUTER on port, PUB on port + 1).</summary>
    public string? ZmqPeers { get; set; }
}

/// <summary>
/// MQTT-style topics as NATS subjects, as in the Rust fabric: '/' -> '.', '+' -> '*', '#' -> '>', a shared
/// subscription ($share/name/...) -> a queue group, and the characters NATS reserves inside a segment percent-encoded.
/// </summary>
internal static class NatsSubjects
{
    public static (string Subject, string? Queue) Of(string topic)
    {
        string? queue = null;
        var rest = topic;
        if (topic.StartsWith("$share/", StringComparison.Ordinal))
        {
            int slash = topic.IndexOf('/', 7);
            if (slash > 0) { queue = topic[7..slash]; rest = topic[(slash + 1)..]; }
        }
        var parts = rest.Split('/').Select(seg => seg switch { "+" => "*", "#" => ">", _ => Encode(seg) });
        return (string.Join('.', parts), queue);
    }

    private static string Encode(string seg)
    {
        if (seg.IndexOfAny(['%', '.', '*', '>', ' ', '\t']) < 0) return seg;
        var b = new System.Text.StringBuilder(seg.Length + 8);
        foreach (var c in seg)
            b.Append(c switch { '%' => "%25", '.' => "%2E", '*' => "%2A", '>' => "%3E", ' ' => "%20", '\t' => "%09", _ => c.ToString() });
        return b.ToString();
    }

    public static string Topic(string subject) =>
        string.Join('/', subject.Split('.').Select(t => t.Contains('%') ? Uri.UnescapeDataString(t) : t));
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
    private readonly IMqttClient? _client;
    private readonly MqttClientOptions? _options;
    private readonly NatsConnection? _nats;
    private readonly ZmqTransport? _zmq;
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
        if (o.Transport.Equals("zmq", StringComparison.OrdinalIgnoreCase))
        {
            var peers = ZmqTransport.ParsePeers(o.ZmqPeers ?? throw new ArgumentException("ZmqPeers is required with the zmq transport"));
            _zmq = new ZmqTransport(o.ClientId, o.Namespace, peers, (topic, payload) =>
            {
                Interlocked.Increment(ref Received);
                OnMessage(topic, payload);
            });
            return;
        }
        if (o.Transport.Equals("nats", StringComparison.OrdinalIgnoreCase))
        {
            int port = o.Port == 1883 ? 4222 : o.Port;
            var nopts = NatsOpts.Default with
            {
                Url = $"nats://{o.Host}:{port}",
                Name = o.ClientId,
                AuthOpts = o.Username != null ? NatsAuthOpts.Default with { Username = o.Username, Password = o.Password } : NatsAuthOpts.Default,
                SubPendingChannelCapacity = 65536,
                SubPendingChannelFullMode = System.Threading.Channels.BoundedChannelFullMode.Wait,
                MaxReconnectRetry = -1,
            };
            _nats = new NatsConnection(nopts);
            return;
        }
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
        _client!.ApplicationMessageReceivedAsync += e =>
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
        if (c._nats != null) await c._nats.ConnectAsync();
        else if (c._client != null) await c._client.ConnectAsync(c._options!, ct);
        // (ZeroMQ connects in the background and reconnects on its own)
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
            while (!_cts.IsCancellationRequested && _client != null && !_client.IsConnected)
            {
                try
                {
                    await _client.ConnectAsync(_options!, _cts.Token);
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
        if (_zmq != null)
        {
            if (_filters.TryAdd(filter, 0)) _zmq.Subscribe(filter);
            return;
        }
        if (!_filters.TryAdd(filter, 0) && _nats != null) return; // NATS keeps subscriptions across reconnects
        if (_nats != null)
        {
            var (subject, queue) = NatsSubjects.Of(filter);
            var sub = await _nats.SubscribeCoreAsync<byte[]>(subject, queueGroup: queue, cancellationToken: _cts.Token);
            _ = Task.Run(async () =>
            {
                try
                {
                    await foreach (var m in sub.Msgs.ReadAllAsync(_cts.Token))
                    {
                        Interlocked.Increment(ref Received);
                        OnMessage(NatsSubjects.Topic(m.Subject), new ReadOnlySequence<byte>(m.Data ?? []));
                    }
                }
                catch (OperationCanceledException) { }
            });
            return;
        }
        await _client!.SubscribeAsync(new MqttClientSubscribeOptionsBuilder().WithTopicFilter(filter, _qos).Build(), ct);
    }

    // ---- outgoing: coalesced per topic ----

    /// <summary>
    /// Queue an envelope for <paramref name="topic"/>; it goes out with whatever else is queued for it.
    /// <paramref name="urgent"/>: latency-critical (results): flush without waiting out the linger.
    /// </summary>
    public void Send(string topic, Envelope envelope, bool urgent = false) => GetOutbox(topic, isPublish: false).Enqueue(envelope, urgent);

    /// <summary>Queue a publish request; requests for the same topic travel as one PublishBatch.</summary>
    internal void SendPublish(string topic, PublishRequest request) => GetOutbox(topic, isPublish: true).Enqueue(request);

    private Outbox GetOutbox(string topic, bool isPublish) => _outboxes.GetOrAdd(topic, t => new Outbox(this, t, isPublish));

    private async Task PublishRawAsync(string topic, byte[] payload)
    {
        Interlocked.Increment(ref Sent);
        if (_zmq != null)
        {
            _zmq.Publish(topic, payload);
            return;
        }
        if (_nats != null)
        {
            await _nats.PublishAsync(NatsSubjects.Of(topic).Subject, payload, cancellationToken: _cts.Token);
            return;
        }
        var msg = new MqttApplicationMessageBuilder().WithTopic(topic).WithPayload(payload).WithQualityOfServiceLevel(_qos).Build();
        await _client!.PublishAsync(msg, _cts.Token);
    }

    /// <summary>One destination: a queue and at most one send in progress.</summary>
    private sealed class Outbox(OctopusConnection conn, string topic, bool isPublish)
    {
        private readonly ConcurrentQueue<IMessage> _queue = new();
        private int _sending;
        private int _urgent;
        private long _lastFlush;

        public void Enqueue(IMessage m, bool urgent = false)
        {
            _queue.Enqueue(m);
            if (urgent) Volatile.Write(ref _urgent, 1);
            if (Interlocked.CompareExchange(ref _sending, 1, 0) == 0) _ = Task.Run(DrainAsync);
        }

        private async Task DrainAsync()
        {
            while (true)
            {
                while (!_queue.IsEmpty)
                {
                    long wait = _lastFlush + (long)conn._o.Linger.TotalMilliseconds - Environment.TickCount64;
                    if (wait > 0 && Interlocked.Exchange(ref _urgent, 0) == 0) await Task.Delay((int)wait);
                    Volatile.Write(ref _urgent, 0);
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
            case Envelope.BodyOneofCase.Demand:
                foreach (var id in env.Demand.WorkerIds)
                    if (Workers.TryGetValue(id, out var dw)) dw.OnDemand(env.Demand);
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
        if (_zmq != null)
        {
            _zmq.Dispose();
            return;
        }
        if (_nats != null)
        {
            try { await _nats.DisposeAsync(); } catch { /* closing */ }
            return;
        }
        try { await _client!.DisconnectAsync(); } catch { /* closing */ }
        _client!.Dispose();
    }
}
