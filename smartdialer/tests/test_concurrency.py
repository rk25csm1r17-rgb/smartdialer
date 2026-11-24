"""Two (or many) workers racing for the same agent / borrowers / headroom."""
import multiprocessing as mp
import sqlite3
import threading

from smartdialer.calls import claim_available_agent
from smartdialer import invariants
from smartdialer.states import AgentState as A
from smartdialer.store import Store
from smartdialer.testing import FixedPacer, World, proc_worker, seed_history


def _setup(path, agents=1, borrowers=50):
    s = Store(path)
    with s.tx() as c:
        c.executemany("INSERT INTO agents(id, state, state_since, heartbeat_at) VALUES (?, 'AVAILABLE', 0, 9e18)",
                      [(f"a{i}",) for i in range(agents)])
        c.executemany("INSERT INTO borrowers(id, phone, state) VALUES (?, '+1', 'READY')",
                      [(f"b{i}",) for i in range(borrowers)])
    return s


def test_cas_without_lock_two_readers_one_winner(tmp_path):
    """Even with no write lock held between read and write, the conditional UPDATE
    lets exactly one of two workers that both *saw* the agent AVAILABLE reserve it."""
    path = str(tmp_path / "d.db")
    _setup(path)
    c1 = sqlite3.connect(path, isolation_level=None)
    c2 = sqlite3.connect(path, isolation_level=None)
    assert c1.execute("SELECT state FROM agents WHERE id='a0'").fetchone()[0] == "AVAILABLE"
    assert c2.execute("SELECT state FROM agents WHERE id='a0'").fetchone()[0] == "AVAILABLE"
    sql = "UPDATE agents SET state='RESERVED', call_id=? WHERE id='a0' AND state='AVAILABLE'"
    assert c1.execute(sql, ("call-1",)).rowcount == 1
    assert c2.execute(sql, ("call-2",)).rowcount == 0
    assert c2.execute("SELECT call_id FROM agents WHERE id='a0'").fetchone()[0] == "call-1"


def test_threads_race_for_one_agent(tmp_path):
    store = _setup(str(tmp_path / "d.db"), agents=1)
    winners, barrier = [], threading.Barrier(16)

    def go(i):
        barrier.wait()
        with store.tx() as c:
            if claim_available_agent(c, f"call-{i}", A.RESERVED, 1.0):
                winners.append(i)

    ts = [threading.Thread(target=go, args=(i,)) for i in range(16)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert len(winners) == 1


def test_unique_index_rejects_second_active_call_for_agent(tmp_path):
    store = _setup(str(tmp_path / "d.db"))
    with store.tx() as c:
        c.execute("INSERT INTO calls(id, mode, borrower_id, agent_id, state, created_at, updated_at) VALUES ('c1','progressive','b1','a0','RINGING',0,0)")
    try:
        with store.tx() as c:
            c.execute("INSERT INTO calls(id, mode, borrower_id, agent_id, state, created_at, updated_at) VALUES ('c2','progressive','b2','a0','RESERVED',0,0)")
        raise AssertionError("second active call for the same agent was accepted")
    except sqlite3.IntegrityError:
        pass


def _mp_ctx():
    """fork on Linux/macOS; spawn on Windows (which has no fork)."""
    for method in ("fork", "spawn"):
        if method in mp.get_all_start_methods():
            return mp.get_context(method)
    return mp.get_context()


def test_processes_never_overbook_agents(tmp_path):
    """8 OS processes hammer one campaign asking for 25 calls each, 20 times, with 20 agents."""
    path = str(tmp_path / "d.db")
    _setup(path, agents=20, borrowers=5000)
    ctx = _mp_ctx()
    q = ctx.Queue()
    ps = [ctx.Process(target=proc_worker, args=(path, i, 20, q)) for i in range(8)]
    [p.start() for p in ps]
    total = sum(q.get() for _ in ps)   # drain before join (safe on spawn/Windows)
    [p.join() for p in ps]
    c = Store(path).read()
    assert total == 20
    assert c.execute("SELECT COUNT(*) FROM calls").fetchone()[0] == 20
    assert c.execute("SELECT COUNT(DISTINCT agent_id) FROM calls").fetchone()[0] == 20
    assert invariants.check(c) == []


def test_two_workers_cannot_double_spend_predictive_headroom(tmp_path):
    """Both workers see the same free capacity, both ask for 500 predictive calls.
    The decision and the inserts share one serialised transaction, so the second
    worker's Safety Controller sees the first worker's calls."""
    w = World(agents=10, borrowers=2000, path=str(tmp_path / "d.db"), pacer=FixedPacer(500), workers=2)
    seed_history(w, n=300, answer_rate=0.3)
    results = []
    barrier = threading.Barrier(2)

    def go(worker):
        barrier.wait()
        results.append(worker.allocator.allocate(worker.pacer.propose(None)))

    ts = [threading.Thread(target=go, args=(wk,)) for wk in w.workers]
    [t.start() for t in ts]
    [t.join() for t in ts]
    first, second = sorted(results, key=lambda d: d.id)
    assert first.approved > 0 and first.mode == "predictive"
    # The second worker's controller saw the first worker's calls already in flight
    # and had (almost) no headroom left: together they stay inside one decision's bound.
    assert second.detail["unbound_inflight"] == first.approved
    assert first.approved + second.approved <= first.detail["allowed_new"]
