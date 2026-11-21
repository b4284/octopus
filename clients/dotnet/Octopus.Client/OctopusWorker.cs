using System.Collections.Concurrent;
using System.Threading.Channels;
using Google.Protobuf;
using Octopus.V1;

namespace Octopus.Client;

/// <summary>
/// A delivery as handed to the application. (Source, Epoch, Seq) is the idempotency key for external calls.
/// <c>Item.Skipped</c> lists positions of the source given up since its previous delivery (lost; declared by the
/// source or skipped after the gap timeout): the application may resynchronise or flag the record.
/// </summary>
public sealed class OctopusDelivery
{
    public required Delivery Item { get; init; }
    public required string Member { get; init; }
    public required string MemberInbox { get; init; }
    public long ReceivedAt { get; init; } = Environment.TickCount64;
    internal int Done;
    internal int Reported;
}

/// <summary>
/// Receives work for some groups. The application takes deliveries from <see cref="Deliveries"/>, does its work and
/// commits its own writes, then reports each one with <see cref="ReportDone"/> (or <see cref="Fail"/>), and frees its
/// slot with <see cref="Release"/>. A delivery with <c>Item.PossiblyRedelivered</c> may already have been committed by
/// an earlier attempt: check the idempotency key (Item.Source, Item.Seq) in your own data.
/// Credits are granted to the owning dispatchers from free slots only, so no event waits inside a busy worker.
/// A result returns its slot's credit to the dispatcher that receives it, so a busy worker sends no separate
/// credit messages. Many workers can share one <see cref="OctopusConnection"/> (and its inbox).
/// </summary>
public sealed class OctopusWorker : IAsyncDisposable
{
    private readonly OctopusConnection _conn;
    private readonly string _id;
    private readonly string[] _groups;
    private readonly int _slots;
    private readonly Channel<OctopusDelivery> _deliveries = Channel.CreateUnbounded<OctopusDelivery>();
    // every delivery received and not yet given up (by identity: two deliveries of one source are two entries)
    private readonly ConcurrentDictionary<OctopusDelivery, byte> _running = new(ReferenceEqualityComparer.Instance);
    private readonly object _gate = new();
    private readonly Dictionary<string, int> _granted = new();   // member -> credits granted, not yet used
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _refresh;
    private int _rr;
    private volatile bool _crashed;
    // deliveries whose result is sent but whose slot is not released yet: their credit is already back with the
    // dispatcher, so they no longer count against the slots
    private int _reportedHeld;

    public ChannelReader<OctopusDelivery> Deliveries => _deliveries.Reader;
    public TimeSpan ResultTimeout { get; set; } = TimeSpan.FromSeconds(10);
    public TimeSpan ProgressAfter { get; set; } = TimeSpan.FromSeconds(5);
    public string Id => _id;
    public long Received, ResultsSent;

    private OctopusWorker(OctopusConnection conn, string id, string[] groups, int slots)
    {
        _conn = conn;
        _id = id;
        _groups = groups;
        _slots = slots;
        _refresh = Task.Run(RefreshLoopAsync);
    }

    /// <param name="id">Unique among workers (deliveries are routed by it).</param>
    public static OctopusWorker Create(OctopusConnection conn, string id, IEnumerable<string> groups, int slots)
    {
        var w = new OctopusWorker(conn, id, groups.ToArray(), slots);
        if (!conn.Workers.TryAdd(id, w)) throw new ArgumentException($"worker id {id} already used on this connection");
        conn.TopologyChanged += w.OnTopologyChanged;
        w.OnTopologyChanged();
        return w;
    }

    internal void OnDelivery(string member, string memberInbox, Delivery item)
    {
        lock (_gate) _granted[member] = Math.Max(0, _granted.GetValueOrDefault(member) - 1);
        var od = new OctopusDelivery { Item = item, Member = member, MemberInbox = memberInbox };
        _running[od] = 0;
        Interlocked.Increment(ref Received);
        _deliveries.Writer.TryWrite(od);
    }

    /// <summary>Commit the event with this effect (passed to the apply hook). Returns what the database decided.</summary>
    public async Task<ResultStatus> CompleteAsync(OctopusDelivery d, ByteString effect, CancellationToken ct = default)
    {
        var r = new WorkResult { RequestId = _conn.NextRequestId(), Group = d.Item.Group, Source = d.Item.Source, Epoch = d.Item.Epoch, Seq = d.Item.Seq, Ok = true, Effect = effect, WorkerId = _id };
        var tcs = new TaskCompletionSource<ResultStatus>(TaskCreationOptions.RunContinuationsAsynchronously);
        _conn.PendingResults[r.RequestId] = tcs;
        try
        {
            SendResult(d, r);
            return await tcs.Task.WaitAsync(ResultTimeout, ct);
        }
        catch (TimeoutException) { return ResultStatus.ResultError; }
        finally { _conn.PendingResults.TryRemove(r.RequestId, out _); }
    }

    /// <summary>
    /// The worker has committed its own writes for this event: Octopus removes it so it is never handed out again.
    /// Fire and forget (a lost report only means a repeat, flagged PossiblyRedelivered, after the delivery timeout).
    /// </summary>
    public void ReportDone(OctopusDelivery d)
    {
        var r = new WorkResult { RequestId = _conn.NextRequestId(), Group = d.Item.Group, Source = d.Item.Source, Epoch = d.Item.Epoch, Seq = d.Item.Seq, Ok = true, Done = true, WorkerId = _id };
        SendResult(d, r);
    }

    /// <summary>
    /// The event failed: offer it again after <paramref name="retryDelay"/>. This gives the delivery up, so it also
    /// frees its slot (as <see cref="Release"/> does): no progress is reported for it any more.
    /// </summary>
    public Task Fail(OctopusDelivery d, TimeSpan retryDelay, CancellationToken ct = default)
    {
        var r = new WorkResult
        {
            RequestId = _conn.NextRequestId(), Group = d.Item.Group, Source = d.Item.Source, Epoch = d.Item.Epoch, Seq = d.Item.Seq,
            Ok = false, RetryDelayMs = (uint)Math.Clamp(retryDelay.TotalMilliseconds, 1, uint.MaxValue), WorkerId = _id,
        };
        SendResult(d, r);
        Release(d);
        return Task.CompletedTask;
    }

    private void SendResult(OctopusDelivery d, WorkResult r)
    {
        if (_crashed) return;
        var batch = new ResultBatch { WorkerId = _id, ReplyTo = _conn.Inbox };
        batch.Items.Add(r);
        // the delivering dispatcher if it is alive, otherwise any dispatcher
        bool alive = _conn.Members.ContainsKey(d.Member);
        if (alive)
        {
            // the dispatcher counts the result as one credit back: account for it as granted
            lock (_gate)
                if (OwnerMembers().Contains(d.Member)) _granted[d.Member] = _granted.GetValueOrDefault(d.Member) + 1;
        }
        Interlocked.Increment(ref ResultsSent);
        if (_running.ContainsKey(d) && Interlocked.Exchange(ref d.Reported, 1) == 0) Interlocked.Increment(ref _reportedHeld);
        _conn.Send(alive ? d.MemberInbox : _conn.Topics.Any, new Envelope { Results = batch }, urgent: true);
        // the implicit return assumes the delivery came out of a credit; if this worker holds more than its slots
        // (deliveries crossed a credit change), correct the owners now instead of letting the excess persist
        foreach (var (m, n) in Clamp()) SendCredit(m, n, reset: true);
    }

    /// <summary>
    /// The invariant: deliveries held plus credits granted never exceed the slots. Takes back any excess (largest
    /// grants first) and returns the owners whose credit changed, with their new amount.
    /// </summary>
    private List<(string Member, int Credit)> Clamp()
    {
        var changed = new List<(string, int)>();
        lock (_gate)
        {
            int held = _running.Count - Volatile.Read(ref _reportedHeld);
            int excess = held + _granted.Values.Sum() - _slots;
            while (excess > 0)
            {
                var top = _granted.Where(kv => kv.Value > 0).OrderByDescending(kv => kv.Value).Select(kv => kv.Key).FirstOrDefault();
                if (top is null) break;
                int take = Math.Min(excess, _granted[top]);
                _granted[top] -= take;
                excess -= take;
                changed.RemoveAll(c => c.Item1 == top);
                changed.Add((top, _granted[top]));
            }
        }
        return changed;
    }

    /// <summary>The application is done with the delivery: its slot is free again.</summary>
    public void Release(OctopusDelivery d)
    {
        if (Interlocked.Exchange(ref d.Done, 1) == 1) return;
        if (_running.TryRemove(d, out _) && Volatile.Read(ref d.Reported) == 1) Interlocked.Decrement(ref _reportedHeld);
        Rebalance();
    }

    /// <summary>
    /// A dispatcher has ready events for one of our groups but none of our credit: give it our free slots, or move
    /// credit from owners that hold it idle (they are told the reduced amount).
    /// </summary>
    internal void OnDemand(Demand d)
    {
        if (!_groups.Contains(d.Group) || !_conn.Members.ContainsKey(d.MemberId)) return;
        var sends = new List<(string Member, int Credit, bool Reset)>();
        lock (_gate)
        {
            int want = (int)Math.Max(1, d.Ready);
            int give = Math.Min(want, Math.Max(0, _slots - _running.Count - _granted.Values.Sum()));
            want -= give;
            foreach (var o in _granted.Keys.Where(m => m != d.MemberId && _granted[m] > 0).ToList())
            {
                if (want == 0) break;
                int take = Math.Min(_granted[o], want);
                _granted[o] -= take;
                want -= take;
                give += take;
                sends.Add((o, _granted[o], true));
            }
            if (give > 0)
            {
                _granted[d.MemberId] = _granted.GetValueOrDefault(d.MemberId) + give;
                sends.Add((d.MemberId, give, false));
            }
        }
        foreach (var (m, n, reset) in sends) SendCredit(m, n, reset);
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
        Rebalance();
    }

    private HashSet<string> OwnerMembers()
    {
        var s = new HashSet<string>();
        foreach (var g in _groups)
            if (_conn.Owners.TryGetValue(g, out var m) && _conn.Members.ContainsKey(m)) s.Add(m);
        return s;
    }

    /// <summary>Hand free slots out as credits, round robin over the live owners of our groups.</summary>
    private void Rebalance()
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
        foreach (var (m, n) in grants) SendCredit(m, n, reset: false);
    }

    private void SendCredit(string member, int credit, bool reset)
    {
        if (_crashed) return;
        if (!_conn.Members.TryGetValue(member, out var inbox)) return;
        var c = new WorkerCredit { WorkerId = _id, Inbox = _conn.Inbox, Credit = (uint)credit, Reset = reset, Slots = (uint)_slots };
        c.Groups.AddRange(_groups);
        _conn.Send(inbox, new Envelope { Credit = c });
    }

    /// <summary>Every second: resend each owner the exact outstanding credit (resync), report long-running events.</summary>
    private async Task RefreshLoopAsync()
    {
        // spread the refreshes of many workers over the second
        try { await Task.Delay(Random.Shared.Next(1000), _cts.Token); } catch (OperationCanceledException) { return; }
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        try
        {
            while (await timer.WaitForNextTickAsync(_cts.Token))
            {
                Clamp();
                Dictionary<string, int> resync;
                lock (_gate) resync = OwnerMembers().ToDictionary(m => m, m => _granted.GetValueOrDefault(m));
                foreach (var (m, n) in resync) SendCredit(m, n, reset: true);
                foreach (var g in _running.Keys
                             .Where(d => Environment.TickCount64 - d.ReceivedAt >= ProgressAfter.TotalMilliseconds && _conn.Members.ContainsKey(d.Member))
                             .GroupBy(d => d.MemberInbox))
                {
                    var p = new Progress { WorkerId = _id };
                    p.Sources.AddRange(g.Select(d => d.Item.Source));
                    _conn.Send(g.Key, new Envelope { Progress = p });
                }
                Rebalance();
            }
        }
        catch (OperationCanceledException) { }
    }

    /// <summary>Diagnostics: slots, deliveries held, credits granted per owner, deliveries not yet taken.</summary>
    public string Describe()
    {
        lock (_gate)
            return $"{_id}: slots {_slots}, running {_running.Count}, queued {_deliveries.Reader.Count}, granted [{string.Join(", ", _granted.Select(kv => $"{kv.Key}={kv.Value}"))}], received {Received}, results {ResultsSent}";
    }

    /// <summary>
    /// Testing: behave as if this worker's process died now. It sends nothing more (no results, credits or progress)
    /// and receives nothing more; its deliveries are left to the dispatchers' timeouts.
    /// </summary>
    public void Crash()
    {
        _crashed = true;
        _cts.Cancel();
        _conn.TopologyChanged -= OnTopologyChanged;
        _conn.Workers.TryRemove(_id, out _);
        _deliveries.Writer.TryComplete();
    }

    public async ValueTask DisposeAsync()
    {
        if (_crashed) return;
        _conn.TopologyChanged -= OnTopologyChanged;
        _conn.Workers.TryRemove(_id, out _);
        _cts.Cancel();
        try { await _refresh; } catch { }
        foreach (var m in OwnerMembers()) SendCredit(m, 0, reset: true);
        _deliveries.Writer.TryComplete();
    }
}
