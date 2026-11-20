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
    /// <summary>Unique per connection (MQTT client id).</summary>
    public string ClientId { get; set; } = $"octopus-{Guid.NewGuid():N}";
    public string Namespace { get; set; } = "octopus";
    public string? Username { get; set; }
    public string? Password { get; set; }
    /// <summary>0 ("direct", default) or 1. The fabric is never the durability point: replies come after the database
    /// commit and every message is retried, so QoS 1 only adds broker acknowledgements and an in-flight window.</summary>
    public int QoS { get; set; } = 0;
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
/// One MQTT 5 connection shared by any number of producers and workers. Reconnects and resubscribes on its own,
/// and tracks the live dispatchers and the owner of every group from their retained announcements.
/// </summary>
public sealed class OctopusConnection : IAsyncDisposable
{
    private readonly OctopusOptions _o;
    private readonly IMqttClient _client;
    private readonly MqttClientOptions _options;
    private readonly ConcurrentDictionary<string, Action<ReadOnlySequence<byte>>> _inboxes = new();
    private readonly ConcurrentDictionary<string, byte> _filters = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly MqttQualityOfServiceLevel _qos;
    private int _reconnecting;

    public Topics Topics { get; }
    /// <summary>member id -> inbox, for live dispatchers.</summary>
    public ConcurrentDictionary<string, string> Members { get; } = new();
    /// <summary>group -> owning member id.</summary>
    public ConcurrentDictionary<string, string> Owners { get; } = new();
    /// <summary>Raised (on the receive loop: keep it short) when members or owners change.</summary>
    public event Action? TopologyChanged;
    public long Received, Sent;

    private OctopusConnection(OctopusOptions o)
    {
        _o = o;
        Topics = new Topics(o.Namespace);
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
        await c.SubscribeAsync(c.Topics.MembersFilter, ct);
        await c.SubscribeAsync(c.Topics.OwnersFilter, ct);
        return c;
    }

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

    /// <summary>Route messages arriving on <paramref name="inbox"/> to <paramref name="handler"/> (called on the receive loop).</summary>
    public Task RegisterInboxAsync(string inbox, Action<ReadOnlySequence<byte>> handler, CancellationToken ct = default)
    {
        _inboxes[inbox] = handler;
        return SubscribeAsync(inbox, ct);
    }

    public async Task PublishAsync(string topic, IMessage message, CancellationToken ct = default)
    {
        var msg = new MqttApplicationMessageBuilder().WithTopic(topic).WithPayload(message.ToByteArray()).WithQualityOfServiceLevel(_qos).Build();
        Interlocked.Increment(ref Sent);
        await _client.PublishAsync(msg, ct);
    }

    public Task SendAsync(string topic, Envelope envelope, CancellationToken ct = default) => PublishAsync(topic, envelope, ct);

    private void OnMessage(string topic, ReadOnlySequence<byte> payload)
    {
        if (_inboxes.TryGetValue(topic, out var h))
        {
            h(payload);
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

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        try { await _client.DisconnectAsync(); } catch { /* closing */ }
        _client.Dispose();
    }
}
