"""Small deterministic harness used by the tests and the failure demos.
FakeProvider never emits events by itself: the test decides exactly which
events arrive, how many times, and in what order."""
from __future__ import annotations

from typing import Optional

from .clock import RealClock, SimClock
from .config import DialerConfig
from .pacing import PacingProposal, ProgressivePacer
from .providers.base import ProviderError, ProviderEvent, ProviderTimeout, TelecomProvider
from .safety import SafetyConfig
from .store import Store
from .worker import DialerWorker


def proc_worker(path, idx, rounds, q):
    """Multiprocess test/demo worker. Lives in the package (not a test file) so it
    re-imports cleanly in children under the 'spawn' start method on Windows."""
    store = Store(path)
    w = DialerWorker(f"p{idx}", store, RealClock(), FixedPacer(25, "progressive"), [FakeProvider()])
    placed = 0
    for _ in range(rounds):
        placed += w.allocator.allocate(w.pacer.propose(None)).approved
    q.put(placed)


class FakeProvider(TelecomProvider):
    def __init__(self, name: str = "fake"):
        self.name = name
        self.calls: dict[str, str] = {}      # call_id -> status
        self.dial_requests: list[str] = []
        self.hangups: list[str] = []
        self.bridges: list[tuple] = []
        self.fail_next: list[Exception] = []  # exceptions to raise on the next dial()s
        self.create_on_timeout = False
        self.down = False

    def set_event_sink(self, sink):
        self.sink = sink

    def dial(self, call_id, phone):
        self.dial_requests.append(call_id)
        if self.down:
            raise ProviderTimeout("down")
        if self.fail_next:
            e = self.fail_next.pop(0)
            if isinstance(e, ProviderTimeout) and self.create_on_timeout:
                self.calls.setdefault(call_id, "dialing")
            raise e
        self.calls.setdefault(call_id, "dialing")
        return f"p-{call_id}"

    def bridge(self, call_id, agent_id):
        if self.down:
            raise ProviderTimeout("down")
        self.bridges.append((call_id, agent_id))

    def hangup(self, call_id):
        if self.down:
            raise ProviderTimeout("down")
        self.hangups.append(call_id)
        if call_id in self.calls:
            self.calls[call_id] = "ended"

    def lookup(self, call_id) -> Optional[str]:
        if self.down:
            raise ProviderTimeout("down")
        return self.calls.get(call_id)

    def ev(self, call_id: str, etype: str, at: float = 0.0, event_id: str | None = None, **kw) -> ProviderEvent:
        return ProviderEvent(provider=self.name, event_id=event_id or f"{call_id}:{etype}", call_id=call_id,
                             provider_call_id=f"p-{call_id}", type=etype, at=at, **kw)


class FixedPacer:
    """Asks for exactly n calls in the given mode. Used to prove the Safety
    Controller caps whatever the pacer asks for."""
    def __init__(self, n: int, mode: str = "predictive"):
        self.n, self.mode = n, mode

    def propose(self, snap):
        return PacingProposal(self.mode, self.n, {"fixed": self.n})


class World:
    def __init__(self, agents: int = 2, borrowers: int = 10, path: str = ":memory:", pacer=None,
                 cfg: DialerConfig = DialerConfig(), safety: SafetyConfig = SafetyConfig(), workers: int = 1):
        self.clock = SimClock(1000.0)
        self.store = Store(path)
        self.provider = FakeProvider()
        self.cfg = cfg
        self.workers = [DialerWorker(f"w{i}", self.store, self.clock, pacer or ProgressivePacer(),
                                     [self.provider], cfg, safety) for i in range(workers)]
        self.w = self.workers[0]
        with self.store.tx() as c:
            c.executemany("INSERT INTO borrowers(id, phone, state) VALUES (?,?, 'READY')",
                          [(f"b{i}", f"+91{i:08d}") for i in range(borrowers)])
        for i in range(agents):
            self.w.agents.add(f"a{i}")
            self.w.agents.go_available(f"a{i}")

    def advance(self, dt: float):
        self.clock.advance_to(self.clock.now() + dt)

    def q(self, sql, *a):
        return self.store.read().execute(sql, a).fetchall()

    def call(self, call_id):
        return self.q("SELECT * FROM calls WHERE id=?", call_id)[0]

    def agent(self, agent_id):
        return self.q("SELECT * FROM agents WHERE id=?", agent_id)[0]

    def calls(self):
        return self.q("SELECT * FROM calls WHERE borrower_id LIKE 'b%' ORDER BY created_at, id")  # skip seeded history

    def ingest(self, ev):
        return self.w.ingestor.ingest(ev)


def seed_history(world: "World", n: int = 200, answer_rate: float = 0.3, tta: float = 8.0, talk: float = 60.0):
    """Insert finished calls so the statistics windows have data (predictive tests)."""
    now = world.clock.now()
    with world.store.tx() as c:
        for i in range(n):
            t = now - 300 + i * (290 / n)
            answered = (i % 100) < answer_rate * 100
            c.execute("INSERT INTO borrowers(id, phone, state) VALUES (?,?, 'DONE')", (f"h{i}", "+0"))
            c.execute("""INSERT INTO calls(id, mode, borrower_id, state, created_at, updated_at, answered_at,
                         connected_at, ended_at, end_reason, was_answered)
                         VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                      (f"h{i}", "predictive", f"h{i}", "COMPLETED" if answered else "FAILED", t - tta, t,
                       t if answered else None, t if answered else None, t + (talk if answered else 0) if answered else t,
                       "normal" if answered else "no_answer", 1 if answered else 0))
