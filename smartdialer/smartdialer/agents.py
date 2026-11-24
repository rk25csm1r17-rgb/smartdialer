"""Agent presence: login, ready, pause, logout, heartbeat.

The interesting case is an agent disappearing while a call is being set up for
them (progressive). We first try to hand the call to another AVAILABLE agent;
if there is none we cancel it before the borrower can answer into silence.
"""
from __future__ import annotations

from .calls import get_call, make_available, rebind_or_cancel
from .config import DialerConfig
from .states import AgentState as A


class AgentService:
    def __init__(self, store, clock, cfg: DialerConfig):
        self.store, self.clock, self.cfg = store, clock, cfg

    def add(self, agent_id: str, state: A = A.OFFLINE) -> None:
        now = self.clock.now()
        with self.store.tx() as c:
            c.execute("""INSERT OR IGNORE INTO agents(id, state, state_since, heartbeat_at)
                         VALUES (?,?,?,?)""", (agent_id, state.value, now, now))

    def heartbeat(self, agent_id: str) -> None:
        with self.store.tx() as c:
            c.execute("UPDATE agents SET heartbeat_at=? WHERE id=?", (self.clock.now(), agent_id))

    def go_available(self, agent_id: str) -> bool:
        now = self.clock.now()
        with self.store.tx() as c:
            c.execute("UPDATE agents SET heartbeat_at=?, pending_state=NULL WHERE id=?", (now, agent_id))
            return make_available(c, agent_id, now, self.cfg, from_states={A.OFFLINE, A.PAUSED, A.WRAP_UP})

    def go_unavailable(self, agent_id: str, target: A = A.OFFLINE, reason: str = "logout") -> str:
        """Logout / pause / lost heartbeat. Returns what happened, for logs and tests."""
        assert target in (A.OFFLINE, A.PAUSED)
        now = self.clock.now()
        with self.store.tx() as c:
            ag = c.execute("SELECT * FROM agents WHERE id=?", (agent_id,)).fetchone()
            if ag is None:
                return "unknown"
            st = A(ag["state"])
            if st in (A.AVAILABLE, A.WRAP_UP, A.PAUSED, A.OFFLINE):
                c.execute("""UPDATE agents SET state=?, call_id=NULL, wrap_until=NULL, version=version+1,
                             state_since=? WHERE id=? AND version=?""", (target.value, now, agent_id, ag["version"]))
                return f"{st.value}->{target.value}"
            if st == A.CONNECTED:
                # Don't drop a live conversation: apply after the call ends.
                c.execute("UPDATE agents SET pending_state=? WHERE id=?", (target.value, agent_id))
                return "deferred_until_call_ends"
            # RESERVED / DIALING: a call is being set up for this agent.
            call_id = ag["call_id"]
            c.execute("""UPDATE agents SET state=?, call_id=NULL, version=version+1, state_since=?
                         WHERE id=? AND version=?""", (target.value, now, agent_id, ag["version"]))
            call = get_call(c, call_id) if call_id else None
            if call is None or call["agent_id"] != agent_id:
                return f"{st.value}->{target.value}"
            return f"{st.value}->{target.value}; call {rebind_or_cancel(c, call, now, self.cfg)}"
