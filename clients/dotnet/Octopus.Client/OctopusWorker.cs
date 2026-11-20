using System.Collections.Concurrent;
using System.Threading.Channels;
using Google.Protobuf;
using Octopus.V1;

namespace Octopus.Client;

/// <summary>A delivery as handed to the application. (Source, Seq) is the idempotency key for external calls.</summary>
public sealed class OctopusDelivery
{
    public required Delivery Item { get; init; }
    public required string Member { get; init; }
    public required string MemberInbox { get; init; }
    public long ReceivedAt { get; init; } = Environment.TickCount64;
    internal int Done;
}

/// <summary>
/// Receives work for some groups. The application takes deliveries from <see cref="Deliveries"/>, reports each one
/// with <see cref="CompleteAsync"/> or <see cref="Fail"/>, and frees its slot with <see cref="Done"/>.
/// Credits are granted to the owning dispatchers from free slots only, so no event waits inside a busy worker.
/// </summary>
public sealed class OctopusWorker : IAsyncDisposable
{
    private readonly OctopusConnection _conn;
    private readonly string _id;
    private readonly string _inbox;
    private readonly string[] _groups;
    private readonly int _slots;
    private readonly Channel<OctopusDelivery> _deliveries = Channel.CreateUnbounded<OctopusDelivery>();
    private readonly ConcurrentDictionary<ulong, TaskCompletionSource<ResultStatus>> _pending = new();
    private readonly ConcurrentDictionary<string, OctopusDelivery> _running = new();
    private readonly object _gate = new();
    private readonly Dictionary<string, int> _granted = new();   // member -> credits granted, not yet used
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _refresh;
    private long _nextReq;
    private int _rr;

    public ChannelReader<OctopusDelivery> Deliveries => _deliveries.Reader;
    public TimeSpan ResultTimeout { get; set; } = TimeSpan.FromSeconds(10);
    public TimeSpan ProgressAfter { get; set; } = TimeSpan.FromSeconds(5);
    public long Received, ResultsSent;

    private OctopusWorker(OctopusConnection conn, string id, string[] groups, int slots)
    {
        _conn = conn;
        _id = id;
        _inbox = conn.Topics.Inbox(id);
        _groups = groups;
        _slots = slots;
        _refresh = Task.Run(RefreshLoopAsync);
    }

    /// <param name="id">Unique among workers (names its inbox).</param>
    public static async Task<OctopusWorker> CreateAsync(OctopusConnection conn, string id, IEnumerable<string> groups, int slots, CancellationToken ct = default)
    {
        var w = new OctopusWorker(conn, id, groups.ToArray(), slots);
        await conn.RegisterInboxAsync(w._inbox, w.OnInbox, ct);
        conn.TopologyChanged += w.OnTopologyChanged;
        w.OnTopologyChanged();
        return w;
    }

    private void OnInbox(System.Buffers.ReadOnlySequence<byte> payload)
    {
        Envelope env;
        try { env = Envelope.Parser.ParseFrom(payload); } catch (InvalidProtocolBufferException) { return; }
        if (env.Deliver is { } d)
        {
            lock (_gate)
            {
                _granted[d.MemberId] = Math.Max(0, _granted.GetValueOrDefault(d.MemberId) - d.Items.Count);
            }
            foreach (var item in d.Items)
            {
                var od = new OctopusDelivery { Item = item, Member = d.MemberId, MemberInbox = d.Inbox };
                _running[item.Source] = od;
                Interlocked.Increment(ref Received);
                _deliveries.Writer.TryWrite(od);
            }
        }
        else if (env.ResultReply is { } r)
        {
            foreach (var a in r.Items)
                if (_pending.TryRemove(a.RequestId, out var tcs)) tcs.TrySetResult(a.Status);
        }
    }

    /// <summary>Commit the event with this effect (passed to the apply hook). Returns what the database decided.</summary>
    public async Task<ResultStatus> CompleteAsync(OctopusDelivery d, ByteString effect, CancellationToken ct = default)
    {
        var r = new WorkResult { RequestId = (ulong)Interlocked.Increment(ref _nextReq), Group = d.Item.Group, Source = d.Item.Source, Seq = d.Item.Seq, Ok = true, Effect = effect };
        var tcs = new TaskCompletionSource<ResultStatus>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pending[r.RequestId] = tcs;
        try
        {
            await SendResultAsync(d, r, ct);
            return await tcs.Task.WaitAsync(ResultTimeout, ct);
        }
        catch (TimeoutException) { return ResultStatus.ResultError; }
        finally { _pending.TryRemove(r.RequestId, out _); }
    }

    /// <summary>The event failed: offer it again after <paramref name="retryDelay"/>.</summary>
    public Task Fail(OctopusDelivery d, TimeSpan retryDelay, CancellationToken ct = default)
    {
        var r = new WorkResult
        {
            RequestId = (ulong)Interlocked.Increment(ref _nextReq), Group = d.Item.Group, Source = d.Item.Source, Seq = d.Item.Seq,
            Ok = false, RetryDelayMs = (uint)Math.Clamp(retryDelay.TotalMilliseconds, 1, uint.MaxValue),
        };
        return SendResultAsync(d, r, ct);
    }

    private async Task SendResultAsync(OctopusDelivery d, WorkResult r, CancellationToken ct)
    {
        var batch = new ResultBatch { WorkerId = _id, ReplyTo = _inbox };
        batch.Items.Add(r);
        // the delivering dispatcher if it is alive, otherwise any dispatcher
        string topic = _conn.Members.ContainsKey(d.Member) ? d.MemberInbox : _conn.Topics.Any;
        Interlocked.Increment(ref ResultsSent);
        await _conn.SendAsync(topic, new Envelope { Results = batch }, ct);
    }

    /// <summary>The application is done with the delivery: its slot is free again.</summary>
    public void Done(OctopusDelivery d)
    {
        if (Interlocked.Exchange(ref d.Done, 1) == 1) return;
        _running.TryRemove(new KeyValuePair<string, OctopusDelivery>(d.Item.Source, d));
        _ = RebalanceAsync();
    }

    private void OnTopologyChanged()
    {
        lock (_gate)
        {
            // credits held by members that are gone or no longer own any of our groups are void
            var owners = OwnerMembers();
            foreach (var m in _granted.Keys.ToList())
                if (!owners.Contains(m)) _granted.Remove(m);
        }
        _ = RebalanceAsync();
    }

    private HashSet<string> OwnerMembers()
    {
        var s = new HashSet<string>();
        foreach (var g in _groups)
            if (_conn.Owners.TryGetValue(g, out var m) && _conn.Members.ContainsKey(m)) s.Add(m);
        return s;
    }

    /// <summary>Hand free slots out as credits, round robin over the live owners of our groups.</summary>
    private async Task RebalanceAsync()
    {
        var grants = new Dictionary<string, int>();
        lock (_gate)
        {
            var owners = OwnerMembers().Order(StringComparer.Ordinal).ToArray();
            if (owners.Length == 0) return;
            int free = _slots - _running.Count - _granted.Values.Sum();
            while (free-- > 0)
            {
                string m = owners[_rr++ % owners.Length];
                _granted[m] = _granted.GetValueOrDefault(m) + 1;
                grants[m] = grants.GetValueOrDefault(m) + 1;
            }
        }
        foreach (var (m, n) in grants) await SendCreditAsync(m, n, reset: false);
    }

    private async Task SendCreditAsync(string member, int credit, bool reset)
    {
        if (!_conn.Members.TryGetValue(member, out var inbox)) return;
        var c = new WorkerCredit { WorkerId = _id, Inbox = _inbox, Credit = (uint)credit, Reset = reset };
        c.Groups.AddRange(_groups);
        try { await _conn.SendAsync(inbox, new Envelope { Credit = c }); } catch (Exception) { /* resent by the refresh */ }
    }

    /// <summary>Every second: resend each owner the exact outstanding credit (resync), report long-running events.</summary>
    private async Task RefreshLoopAsync()
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        try
        {
            while (await timer.WaitForNextTickAsync(_cts.Token))
            {
                Dictionary<string, int> resync;
                lock (_gate)
                {
                    resync = OwnerMembers().ToDictionary(m => m, m => _granted.GetValueOrDefault(m));
                }
                foreach (var (m, n) in resync) await SendCreditAsync(m, n, reset: true);
                var progress = _running.Values
                    .Where(d => Environment.TickCount64 - d.ReceivedAt >= ProgressAfter.TotalMilliseconds && _conn.Members.ContainsKey(d.Member))
                    .GroupBy(d => d.MemberInbox);
                foreach (var g in progress)
                {
                    var p = new Progress { WorkerId = _id };
                    p.Sources.AddRange(g.Select(d => d.Item.Source));
                    try { await _conn.SendAsync(g.Key, new Envelope { Progress = p }); } catch (Exception) { }
                }
                await RebalanceAsync();
            }
        }
        catch (OperationCanceledException) { }
    }

    public async ValueTask DisposeAsync()
    {
        _conn.TopologyChanged -= OnTopologyChanged;
        _cts.Cancel();
        try { await _refresh; } catch { }
        foreach (var m in OwnerMembers()) await SendCreditAsync(m, 0, reset: true);
        _deliveries.Writer.TryComplete();
    }
}
