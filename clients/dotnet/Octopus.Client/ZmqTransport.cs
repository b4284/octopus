using System.Buffers;
using System.Diagnostics;
using System.Text;
using NetMQ;
using NetMQ.Monitoring;
using NetMQ.Sockets;

namespace Octopus.Client;

/// <summary>
/// The brokerless transport (as the Rust fabric's ZeroMQ binding): a DEALER to every dispatcher's ROUTER, identity =
/// this connection's id, and one SUB connected to every dispatcher's PUB (port + 1). Messages are [topic, payload].
/// Routing: <c>&lt;ns&gt;/inbox/&lt;dispatcher&gt;</c> to that dispatcher; any other topic (publishes, the shared
/// subscriptions of MQTT) to one dispatcher, round robin over those connected and heard from in the last 15 s (each
/// announces itself every 5 s). Replies come back on the DEALERs. NetMQ sockets belong to one thread: a poller owns
/// them all and sends are queued to it.
/// </summary>
internal sealed class ZmqTransport : IDisposable
{
    public sealed record Peer(string Id, string Host, int Port);

    /// <summary><c>id@host:port,...</c></summary>
    public static List<Peer> ParsePeers(string s) =>
        s.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Select(p =>
        {
            int at = p.IndexOf('@'), colon = p.LastIndexOf(':');
            if (at <= 0 || colon <= at + 1 || !int.TryParse(p[(colon + 1)..], out var port))
                throw new ArgumentException($"ZeroMQ peer \"{p}\": expected id@host:port");
            return new Peer(p[..at], p[(at + 1)..colon], port);
        }).ToList();

    private sealed class Link
    {
        public required string Id;
        public required DealerSocket Dealer;
        public required NetMQMonitor Monitor;
        public volatile bool Connected;
        public long SeenMs;
    }

    private static readonly long SilentMs = 15_000;
    private readonly NetMQPoller _poller = new();
    private readonly NetMQQueue<(int Link, string Topic, byte[] Payload)> _out = new();
    private readonly NetMQQueue<string> _subscribe = new();
    private readonly List<Link> _links = new();
    private readonly Dictionary<string, int> _byId = new();
    private readonly SubscriberSocket _sub = new();
    private readonly Action<string, ReadOnlySequence<byte>> _onMessage;
    private readonly string _ns;
    private readonly Stopwatch _clock = Stopwatch.StartNew();
    private int _rr;

    public ZmqTransport(string id, string ns, List<Peer> peers, Action<string, ReadOnlySequence<byte>> onMessage)
    {
        _onMessage = onMessage;
        _ns = ns;
        foreach (var p in peers)
        {
            var d = new DealerSocket();
            d.Options.Identity = Encoding.UTF8.GetBytes(id);
            d.Options.SendHighWatermark = 1_000_000;
            d.Options.ReceiveHighWatermark = 1_000_000;
            d.Options.Linger = TimeSpan.Zero;
            var mon = new NetMQMonitor(d, $"inproc://octopus-mon-{Guid.NewGuid():N}", SocketEvents.Connected | SocketEvents.Disconnected);
            var link = new Link { Id = p.Id, Dealer = d, Monitor = mon };
            mon.Connected += (_, _) => { Interlocked.Exchange(ref link.SeenMs, _clock.ElapsedMilliseconds); link.Connected = true; };
            mon.Disconnected += (_, _) => link.Connected = false;
            mon.AttachToPoller(_poller);
            d.ReceiveReady += (_, e) => Drain(e.Socket);
            _byId[p.Id] = _links.Count;
            _links.Add(link);
            _poller.Add(d);
            d.Connect($"tcp://{p.Host}:{p.Port}");
            _sub.Connect($"tcp://{p.Host}:{p.Port + 1}");
        }
        _sub.Options.ReceiveHighWatermark = 1_000_000;
        _sub.ReceiveReady += (_, e) => Drain(e.Socket);
        _poller.Add(_sub);
        _out.ReceiveReady += (_, e) =>
        {
            while (e.Queue.TryDequeue(out var m, TimeSpan.Zero))
            {
                var s = _links[m.Link].Dealer;
                // a full queue (the peer is gone or slow): dropped, the caller retries
                if (s.TrySendFrame(TimeSpan.Zero, m.Topic, more: true)) s.TrySendFrame(TimeSpan.Zero, m.Payload);
            }
        };
        _poller.Add(_out);
        _subscribe.ReceiveReady += (_, e) => { while (e.Queue.TryDequeue(out var p, TimeSpan.Zero)) _sub.Subscribe(p); };
        _poller.Add(_subscribe);
        _poller.RunAsync("octopus-zmq");
    }

    private void Drain(NetMQSocket s)
    {
        var msg = new NetMQMessage();
        while (s.TryReceiveMultipartMessage(TimeSpan.Zero, ref msg, 2))
        {
            if (msg.FrameCount != 2) continue;
            var topic = msg[0].ConvertToString(Encoding.UTF8);
            var payload = msg[1].ToByteArray();
            // a dispatcher's own announcement says it is alive
            var prefix = _ns + "/member/";
            if (topic.StartsWith(prefix, StringComparison.Ordinal) && _byId.TryGetValue(topic[prefix.Length..], out var i))
                Interlocked.Exchange(ref _links[i].SeenMs, _clock.ElapsedMilliseconds);
            _onMessage(topic, new ReadOnlySequence<byte>(payload));
        }
    }

    /// <summary>Filters for the SUB: inbox and shared ones need none (those messages come to the DEALERs).</summary>
    public void Subscribe(string filter)
    {
        if (filter.StartsWith("$share/", StringComparison.Ordinal) || filter.Contains("/inbox/")) return;
        int cut = filter.IndexOfAny(['+', '#']);
        _subscribe.Enqueue(cut < 0 ? filter : filter[..cut]);
    }

    public void Publish(string topic, byte[] payload)
    {
        var inbox = _ns + "/inbox/";
        if (topic.StartsWith(inbox, StringComparison.Ordinal))
        {
            if (!_byId.TryGetValue(topic[inbox.Length..], out var i)) throw new InvalidOperationException($"ZeroMQ: no route to {topic}");
            _out.Enqueue((i, topic, payload));
            return;
        }
        long now = _clock.ElapsedMilliseconds;
        int n = _links.Count, first = Interlocked.Increment(ref _rr);
        for (int k = 0; k < n; k++)
        {
            int i = (int)((uint)(first + k) % (uint)n);
            var l = _links[i];
            if (l.Connected && now - Interlocked.Read(ref l.SeenMs) < SilentMs) { _out.Enqueue((i, topic, payload)); return; }
        }
        throw new InvalidOperationException("ZeroMQ: no dispatcher reachable");
    }

    public IReadOnlyList<string> ConnectedPeers => _links.Where(l => l.Connected).Select(l => l.Id).ToList();

    public void Dispose()
    {
        try { _poller.Stop(); } catch { /* stopping */ }
        foreach (var l in _links) { l.Monitor.DetachFromPoller(); l.Monitor.Dispose(); l.Dealer.Dispose(); }
        _sub.Dispose();
        _out.Dispose();
        _subscribe.Dispose();
        _poller.Dispose();
    }
}
