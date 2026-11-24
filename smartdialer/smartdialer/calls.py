"""Core state transitions shared by the allocator, event ingestion, the agent
service and the reconciler. Every function here must be called inside
`Store.tx()` and every write is a compare-and-set on the expected state.
"""
from __future__ import annotations

import sqlite3
from typing import Optional

from .config import DialerConfig
from .states import (AGENT_BUSY, AgentState as A, BorrowerState as B, CallState as C,
                     TERMINAL_CALL_STATES, call_transition_allowed)

SYSTEM_REASONS = {"provider_error", "provider_timeout", "setup_timeout", "permit_expired",
                  "agent_left", "shed", "no_provider", "dial_dead"}


def get_call(c: sqlite3.Connection, call_id: str) -> Optional[sqlite3.Row]:
    return c.execute("SELECT * FROM calls WHERE id=?", (call_id,)).fetchone()


def enqueue(c, call_id: str, kind: str, now: float, rearm: bool = False) -> None:
    """Outbox write. UNIQUE(call_id, kind) makes it idempotent. `rearm` re-opens a
    command that already ran (e.g. a HANGUP that executed before a late dial landed)."""
    if rearm:
        c.execute("""INSERT INTO commands(call_id, kind, created_at) VALUES (?,?,?)
                     ON CONFLICT(call_id, kind) DO UPDATE SET status='PENDING', attempts=0,
                     lease_until=0, owner=NULL""", (call_id, kind, now))
    else:
        c.execute("INSERT OR IGNORE INTO commands(call_id, kind, created_at) VALUES (?,?,?)",
                  (call_id, kind, now))


def advance(c, call: sqlite3.Row, new: C, now: float, **fields) -> bool:
    """Forward-only CAS transition. Returns False (and changes nothing) if the
    transition is not allowed from the *current* state or someone else moved
    the call first."""
    cur = C(call["state"])
    if not call_transition_allowed(cur, new):
        return False
    sets = ", ".join([f"{k}=?" for k in fields] + ["state=?", "updated_at=?"])
    n = c.execute(f"UPDATE calls SET {sets} WHERE id=? AND state=?",
                  (*fields.values(), new.value, now, call["id"], cur.value)).rowcount
    return n == 1


# --------------------------------------------------------------------------- agents

def claim_available_agent(c, call_id: str, to_state: A, now: float) -> Optional[str]:
    """Atomically take the longest-idle AVAILABLE agent. The `AND state='AVAILABLE'`
    in the outer WHERE is the compare-and-set: two workers can both *see* the agent,
    only one UPDATE can match it."""
    row = c.execute(
        """UPDATE agents SET state=?, call_id=?, version=version+1, state_since=?
           WHERE id=(SELECT id FROM agents WHERE state='AVAILABLE' ORDER BY state_since LIMIT 1)
             AND state='AVAILABLE'
           RETURNING id""", (to_state.value, call_id, now)).fetchone()
    return row["id"] if row else None


def make_available(c, agent_id: str, now: float, cfg: DialerConfig, from_states: set[A],
                   call_id: Optional[str] = None) -> bool:
    """Move an agent to AVAILABLE (or to the PAUSED/OFFLINE they asked for while busy),
    then immediately offer them to any answered borrower who is waiting."""
    ag = c.execute("SELECT * FROM agents WHERE id=?", (agent_id,)).fetchone()
    if ag is None or A(ag["state"]) not in from_states:
        return False
    if call_id is not None and ag["call_id"] != call_id:
        return False  # stale: the agent has moved on to another call
    target = A(ag["pending_state"]) if ag["pending_state"] else A.AVAILABLE
    n = c.execute(
        """UPDATE agents SET state=?, call_id=NULL, wrap_until=NULL, pending_state=NULL,
                  version=version+1, state_since=?
           WHERE id=? AND version=?""", (target.value, now, agent_id, ag["version"])).rowcount
    if n != 1:
        return False
    if target == A.AVAILABLE:
        serve_waiting_answered(c, now, cfg)
    return True


def serve_waiting_answered(c, now: float, cfg: DialerConfig) -> None:
    """Answered borrowers who are waiting take priority over any new dial."""
    while True:
        w = c.execute("""SELECT * FROM calls WHERE state='ANSWERED' AND agent_id IS NULL
                         ORDER BY answered_at LIMIT 1""").fetchone()
        if w is None or not connect_answered(c, w, now, cfg):
            return


def connect_answered(c, call: sqlite3.Row, now: float, cfg: DialerConfig) -> bool:
    """Bind an AVAILABLE agent to an answered, unbound call. Same transaction:
    agent -> CONNECTED, call -> CONNECTED, BRIDGE command in the outbox."""
    agent = claim_available_agent(c, call["id"], A.CONNECTED, now)
    if agent is None:
        return False
    if not advance(c, call, C.CONNECTED, now, agent_id=agent, connected_at=now):
        raise RuntimeError("call moved under a write lock; should be impossible")
    enqueue(c, call["id"], "BRIDGE", now)
    return True


def _release_agent(c, call: sqlite3.Row, was_connected: bool, now: float, cfg: DialerConfig) -> None:
    agent_id = call["agent_id"]
    if not agent_id:
        return
    if was_connected:
        c.execute("""UPDATE agents SET state='WRAP_UP', call_id=NULL, wrap_until=?,
                     version=version+1, state_since=?
                     WHERE id=? AND call_id=? AND state='CONNECTED'""",
                  (now + cfg.wrap_up_s, now, agent_id, call["id"]))
    else:
        make_available(c, agent_id, now, cfg, from_states={A.RESERVED, A.DIALING}, call_id=call["id"])


# --------------------------------------------------------------------------- borrowers

def _settle_borrower(c, call: sqlite3.Row, terminal: C, reason: str, now: float, cfg: DialerConfig) -> None:
    b = c.execute("SELECT * FROM borrowers WHERE id=?", (call["borrower_id"],)).fetchone()
    if b is None or b["call_id"] != call["id"]:
        return
    if terminal == C.COMPLETED:
        state, attempts, sysf, nxt, prio = B.DONE, b["attempts"] + 1, b["system_failures"], now, b["priority"]
    elif terminal == C.ABANDONED:
        # They picked up and nobody was there: call back soon, at higher priority.
        state, attempts, sysf = B.READY, b["attempts"] + 1, b["system_failures"]
        nxt, prio = now + cfg.abandon_callback_delay_s, b["priority"] + 10
    elif terminal == C.CANCELLED or reason in SYSTEM_REASONS:
        # Our fault, not the borrower's: don't burn an attempt; back off on repeats.
        sysf = b["system_failures"] + (0 if terminal == C.CANCELLED else 1)
        state = B.READY if sysf < cfg.max_system_failures else B.EXHAUSTED
        attempts, prio = b["attempts"], b["priority"]
        nxt = now if terminal == C.CANCELLED else now + cfg.system_retry_base_s * (2 ** (sysf - 1))
    else:  # no_answer / busy / ring_timeout
        attempts, sysf, prio = b["attempts"] + 1, b["system_failures"], b["priority"]
        state = B.READY if attempts < cfg.max_attempts else B.EXHAUSTED
        nxt = now + cfg.retry_backoff_s
    c.execute("""UPDATE borrowers SET state=?, attempts=?, system_failures=?, next_attempt_at=?,
                 priority=?, call_id=NULL WHERE id=? AND call_id=?""",
              (state.value, attempts, sysf, nxt, prio, b["id"], call["id"]))


# --------------------------------------------------------------------------- end of call

def finalize(c, call: sqlite3.Row, terminal: C, reason: str, now: float, cfg: DialerConfig,
             hangup: bool = False, answered: Optional[bool] = None) -> bool:
    """Move a call to a terminal state exactly once, releasing agent and borrower
    in the same transaction. Returns False if the call was already terminal."""
    assert terminal in TERMINAL_CALL_STATES
    was_connected = call["state"] == C.CONNECTED.value
    fields = dict(ended_at=now, end_reason=reason)
    if answered:
        fields["was_answered"] = 1
    if not advance(c, call, terminal, now, **fields):
        return False
    _release_agent(c, call, was_connected, now, cfg)
    _settle_borrower(c, call, terminal, reason, now, cfg)
    if hangup and call["provider"]:  # provider is pinned just before the first dial attempt
        enqueue(c, call["id"], "HANGUP", now)
    return True


def rebind_or_cancel(c, call: sqlite3.Row, now: float, cfg: DialerConfig) -> str:
    """The agent bound to a not-yet-answered progressive call disappeared.
    Prefer handing the call to another AVAILABLE agent; otherwise cancel it before
    the borrower can answer into silence."""
    target = A.RESERVED if call["state"] == C.RESERVED.value else A.DIALING
    new_agent = claim_available_agent(c, call["id"], target, now)
    if new_agent:
        c.execute("UPDATE calls SET agent_id=?, updated_at=? WHERE id=? AND state=?",
                  (new_agent, now, call["id"], call["state"]))
        return f"rebound:{new_agent}"
    # Detach the agent first so finalize doesn't try to release them.
    c.execute("UPDATE calls SET agent_id=NULL WHERE id=?", (call["id"],))
    call = get_call(c, call["id"])
    finalize(c, call, C.CANCELLED, "agent_left", now, cfg, hangup=True)
    return "cancelled"
