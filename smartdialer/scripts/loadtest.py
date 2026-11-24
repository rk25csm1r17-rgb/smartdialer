"""Basic load test.

  python scripts/loadtest.py                # default: 1,2,4,8 worker processes, 20s each
  python scripts/loadtest.py --seconds 10 --procs 1 4

Part 1  Full call write path under contention. N OS processes share one SQLite
        file (WAL). Each loops: allocate (Safety Controller + claims) -> DIAL ->
        ANSWERED (+30% duplicate deliveries) -> COMPLETED -> agent ready again.
        Afterwards we audit that no agent ever had two overlapping calls and no
        borrower was dialled twice concurrently.
Part 2  Cost of one Safety Controller decision as the agent pool grows, to show
        where the CPU goes at 100 / 1,000 / 10,000 agents.
"""
from __future__ import annotations

import argparse
import multiprocessing as mp
import os
import random
import statistics
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from smartdialer import invariants                                   # noqa: E402
from smartdialer.clock import RealClock                               # noqa: E402
from smartdialer.config import DialerConfig                           # noqa: E402
from smartdialer.safety import OverflowRisk                           # noqa: E402
from smartdialer.store import Store                                   # noqa: E402
from smartdialer.testing import FakeProvider, FixedPacer              # noqa: E402
from smartdialer.worker import DialerWorker                           # noqa: E402


def setup(path, agents, borrowers):
    s = Store(path)
    now = time.time()
    with s.tx() as c:
        c.executemany("INSERT INTO agents(id, state, state_since, heartbeat_at) VALUES (?, 'AVAILABLE', ?, 9e18)",
                      [(f"a{i}", now) for i in range(agents)])
        c.executemany("INSERT INTO borrowers(id, phone, state) VALUES (?, '+1', 'READY')",
                      [(f"b{i}",) for i in range(borrowers)])


def worker_proc(path, idx, seconds, q):
    store = Store(path)
    prov = FakeProvider()
    cfg = DialerConfig(wrap_up_s=0)
    w = DialerWorker(f"p{idx}", store, RealClock(), FixedPacer(10, "progressive"), [prov], cfg)
    rng = random.Random(idx)
    lat, done, conflicts = [], 0, 0
    t_end = time.time() + seconds
    while time.time() < t_end:
        t0 = time.perf_counter()
        d = w.allocator.allocate(w.pacer.propose(None))
        lat.append(time.perf_counter() - t0)
        if d.approved == 0:
            conflicts += 1
        w.allocator.dispatch()
        mine = store.read().execute("SELECT id, agent_id FROM calls WHERE state='INITIATED' AND decision_id IN "
                                    "(SELECT id FROM safety_decisions WHERE worker=?)", (w.id,)).fetchall()
        for cid, agent in mine:
            for _ in range(2 if rng.random() < 0.3 else 1):       # 30% duplicate webhook deliveries
                w.ingestor.ingest(prov.ev(cid, "ANSWERED", at=time.time()))
            w.allocator.dispatch()
            w.ingestor.ingest(prov.ev(cid, "COMPLETED", answered=True))
            w.agents.go_available(agent)
            done += 1
    q.put((done, lat, conflicts))


def audit(path):
    c = Store(path).read()
    errs = invariants.check(c)
    rows = c.execute("SELECT agent_id, created_at, ended_at FROM calls WHERE agent_id IS NOT NULL ORDER BY agent_id, created_at").fetchall()
    last = {}
    for a, s, e in rows:
        if a in last and last[a] is not None and s < last[a]:
            errs.append(f"agent {a} overlapping calls")
        last[a] = e
    brow = c.execute("SELECT borrower_id, created_at, ended_at FROM calls ORDER BY borrower_id, created_at").fetchall()
    lastb = {}
    for b, s, e in brow:
        if b in lastb and lastb[b] is not None and s < lastb[b]:
            errs.append(f"borrower {b} dialled twice concurrently")
        lastb[b] = e
    return errs, len(rows)


def _mp_ctx():
    """fork on Linux/macOS; spawn on Windows (which has no fork)."""
    for method in ("fork", "spawn"):
        if method in mp.get_all_start_methods():
            return mp.get_context(method)
    return mp.get_context()


def part1(procs_list, seconds, agents):
    print(f"\nPart 1: full write path, {agents} agents, {seconds}s per run, SQLite WAL on local disk")
    print("| worker processes | calls completed/s | allocate p50 ms | allocate p99 ms | empty allocations | audit |")
    print("|---|---|---|---|---|---|")
    for n in procs_list:
        d = tempfile.mkdtemp()
        path = os.path.join(d, "load.db")
        setup(path, agents, 200000)
        ctx = _mp_ctx()
        q = ctx.Queue()
        ps = [ctx.Process(target=worker_proc, args=(path, i, seconds, q)) for i in range(n)]
        [p.start() for p in ps]
        res = [q.get() for _ in ps]
        [p.join() for p in ps]
        done = sum(r[0] for r in res)
        lat = sorted(x for r in res for x in r[1])
        empty = sum(r[2] for r in res)
        errs, ncalls = audit(path)
        p99 = lat[int(len(lat) * 0.99) - 1] if lat else 0
        print(f"| {n} | {done / seconds:,.0f} | {statistics.median(lat) * 1000:.2f} | {p99 * 1000:.1f} | "
              f"{empty} | {'OK' if not errs else errs[:3]} ({ncalls} calls) |")


def part2():
    print("\nPart 2: Safety Controller risk computation per decision")
    print("| capacity (free agents) | unbound calls in flight | exact Poisson-binomial ms | normal approx ms | P(overflow) exact vs approx |")
    print("|---|---|---|---|---|")
    for cap in (100, 1000, 10000):
        inflight = int((cap - 2.0 * (cap * 0.7) ** 0.5) / 0.3)    # sized so P(overflow) is ~2%
        rng = random.Random(cap)
        ps = [rng.uniform(0.1, 0.5) for _ in range(inflight)]
        t0 = time.perf_counter()
        OverflowRisk.EXACT_MAX = 10 ** 9
        ex = OverflowRisk(cap)
        for p in ps:
            ex.add(p)
        t_ex = (time.perf_counter() - t0) * 1000
        OverflowRisk.EXACT_MAX = -1
        t0 = time.perf_counter()
        ap = OverflowRisk(cap)
        for p in ps:
            ap.add(p)
        t_ap = (time.perf_counter() - t0) * 1000
        print(f"| {cap:,} | {inflight:,} | {t_ex:,.1f} | {t_ap:,.2f} | {ex.over:.4f} vs {ap.over:.4f} |")
    OverflowRisk.EXACT_MAX = 400


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--procs", type=int, nargs="*", default=[1, 2, 4, 8])
    ap.add_argument("--seconds", type=float, default=20)
    ap.add_argument("--agents", type=int, default=1000)
    a = ap.parse_args()
    part1(a.procs, a.seconds, a.agents)
    part2()
