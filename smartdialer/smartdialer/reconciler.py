"""Reconciler: the timers. Any worker may run it at any time; every action is a
CAS, so running it on N workers concurrently is safe (the losers match 0 rows).

It is what makes the system converge after crashes, lost webhooks and outages:
  * wrap-up timers expire           -> agent AVAILABLE
  * agent heartbeat lost            -> treat as logout (rebind/cancel their call)
  * answered borrower waited > grace -> ABANDONED + hangup (bounded silence)
  * INITIATED with no events        -> ask the provider; else FAILED(setup_timeout)
  * RINGING too long                -> FAILED(ring_timeout) + hangup
  * CONNECTED far too long          -> ask the provider whether it's really still up
Expired command leases need no work here: dispatch() picks them up by lease_until.
"""
from __future__ import annotations

from .agents import AgentService
from .calls import finalize, get_call, make_available
from .config import DialerConfig
from .providers.base import ProviderError
from .providers.router import ProviderRouter
from .states import AgentState as A, CallState as C


class Reconciler:
    def __init__(self, store, clock, cfg: DialerConfig, agents: AgentService, router: ProviderRouter):
        self.store, self.clock, self.cfg, self.agents, self.router = store, clock, cfg, agents, router
        self.actions: dict[str, int] = {}

    def _count(self, k):
        self.actions[k] = self.actions.get(k, 0) + 1

    def run(self) -> None:
        now, cfg = self.clock.now(), self.cfg
        with self.store.tx() as c:
            for r in c.execute("SELECT id FROM agents WHERE state='WRAP_UP' AND wrap_until <= ?", (now,)).fetchall():
                if make_available(c, r["id"], now, cfg, from_states={A.WRAP_UP}):
                    self._count("wrap_up_expired")
            for r in c.execute("""SELECT * FROM calls WHERE state='ANSWERED' AND agent_id IS NULL
                                  AND answered_at <= ?""", (now - cfg.abandon_grace_s,)).fetchall():
                if finalize(c, r, C.ABANDONED, "no_agent_within_grace", now, cfg, hangup=True, answered=True):
                    self._count("abandoned_after_grace")
            for r in c.execute("""SELECT * FROM calls WHERE state='RINGING' AND updated_at <= ?""",
                               (now - cfg.ring_timeout_s,)).fetchall():
                if finalize(c, r, C.FAILED, "ring_timeout", now, cfg, hangup=True):
                    self._count("ring_timeout")
            stale_agents = [r["id"] for r in c.execute(
                """SELECT id FROM agents WHERE state NOT IN ('OFFLINE') AND heartbeat_at <= ?""",
                (now - cfg.agent_heartbeat_timeout_s,)).fetchall()]
            setup_stuck = c.execute("""SELECT * FROM calls WHERE state='INITIATED' AND updated_at <= ?""",
                                    (now - cfg.setup_timeout_s,)).fetchall()
            long_calls = c.execute("""SELECT * FROM calls WHERE state='CONNECTED' AND connected_at <= ?""",
                                   (now - cfg.max_call_s,)).fetchall()
        for a in stale_agents:
            self.agents.go_unavailable(a, A.OFFLINE, reason="heartbeat_lost")
            self._count("agent_heartbeat_lost")
        # Provider lookups happen outside the transaction.
        for r in setup_stuck:
            status = self._lookup(r)
            with self.store.tx() as c:
                call = get_call(c, r["id"])
                if call["state"] != C.INITIATED.value:
                    continue
                if status in ("ringing", "dialing"):
                    c.execute("UPDATE calls SET updated_at=? WHERE id=?", (now, r["id"]))  # alive, events late
                    self._count("setup_alive")
                elif status in ("answered", "bridged"):
                    pass  # ANSWERED webhook will arrive (or redelivery); leave it
                else:
                    finalize(c, call, C.FAILED, "setup_timeout", now, cfg, hangup=True)
                    self._count("setup_timeout")
        for r in long_calls:
            status = self._lookup(r)
            if status in (None, "ended"):
                with self.store.tx() as c:
                    call = get_call(c, r["id"])
                    if finalize(c, call, C.COMPLETED, "ended_per_lookup", now, cfg):
                        self._count("connected_closed_by_lookup")

    def _lookup(self, call):
        if not call["provider"]:
            return None
        try:
            return self.router.get(call["provider"]).lookup(call["id"])
        except ProviderError:
            return "unknown"
