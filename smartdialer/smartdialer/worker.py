"""A dialer worker. N of these can run against the same database/campaign."""
from __future__ import annotations

from .agents import AgentService
from .allocator import CallAllocator
from .config import DialerConfig
from .events import EventIngestor
from .providers.router import ProviderRouter
from .reconciler import Reconciler
from .safety import SafetyConfig, SafetyController
from .snapshot import take_snapshot


class DialerWorker:
    def __init__(self, worker_id: str, store, clock, pacer, providers, cfg: DialerConfig = DialerConfig(),
                 safety_cfg: SafetyConfig = SafetyConfig(), seed: int = 0):
        self.id = worker_id
        self.store, self.clock, self.cfg = store, clock, cfg
        self.router = ProviderRouter(providers, clock)
        # The pacer is deliberately given nothing but snapshots.
        self.pacer = pacer
        self.safety = SafetyController(safety_cfg, self.router.health)
        self.allocator = CallAllocator(store, clock, self.safety, self.router, cfg, worker_id, seed)
        self.agents = AgentService(store, clock, cfg)
        self.reconciler = Reconciler(store, clock, cfg, self.agents, self.router)
        self.ingestor = EventIngestor(store, clock, cfg)
        self.last_decision = None
        self.last_proposal = None

    def tick(self):
        self.reconciler.run()
        snap = take_snapshot(self.store.read(), self.clock.now())
        self.last_proposal = self.pacer.propose(snap)
        self.last_decision = self.allocator.allocate(self.last_proposal)
        self.allocator.dispatch()
        return self.last_decision
