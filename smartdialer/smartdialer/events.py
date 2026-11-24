"""Provider webhook ingestion.

One event = one transaction:
    1. INSERT into provider_events (PK provider+event_id). Already there -> it's a
       redelivery; return without touching anything.
    2. Apply the event to the call with forward-only CAS transitions, including
       agent/borrower side effects and outbox commands.
Both commit together or not at all. If the worker dies before COMMIT, nothing
happened and the provider redelivers (webhooks are acked only after commit).
Semantic duplicates with new ids and out-of-order events are absorbed by the
monotonic call state machine (see states.CALL_RANK).
"""
from __future__ import annotations

import sqlite3

from .calls import connect_answered, enqueue, finalize, get_call, advance
from .config import DialerConfig
from .providers.base import ProviderEvent
from .states import AgentState as A, CallState as C, TERMINAL_CALL_STATES


class EventIngestor:
    def __init__(self, store, clock, cfg: DialerConfig):
        self.store, self.clock, self.cfg = store, clock, cfg
        self.effects: dict[str, int] = {}
        self.crash_hook = lambda point, **kw: None

    def ingest(self, ev: ProviderEvent) -> str:
        now = self.clock.now()
        with self.store.tx() as c:
            try:
                c.execute("""INSERT INTO provider_events(provider, event_id, call_id, type, received_at, effect)
                             VALUES (?,?,?,?,?, 'pending')""",
                          (ev.provider, ev.event_id, ev.call_id, ev.type, now))
            except sqlite3.IntegrityError:
                effect = "duplicate_delivery"
            else:
                effect = self._apply(c, ev, now)
                c.execute("UPDATE provider_events SET effect=? WHERE provider=? AND event_id=?",
                          (effect, ev.provider, ev.event_id))
                self.crash_hook("before_commit", ev=ev)      # raising here rolls everything back
        self.crash_hook("after_commit", ev=ev)               # committed, but never acked to the provider
        self.effects[effect] = self.effects.get(effect, 0) + 1
        return effect

    def _apply(self, c, ev: ProviderEvent, now: float) -> str:
        call = get_call(c, ev.call_id)
        if call is None:
            return "unknown_call"
        if call["provider"] and call["provider"] != ev.provider:
            return "wrong_provider"
        if not call["provider"]:
            # Only possible if the pinning write was lost; the event proves where the call lives.
            c.execute("UPDATE calls SET provider=? WHERE id=? AND provider IS NULL", (ev.provider, call["id"]))
            call = get_call(c, ev.call_id)
        state = C(call["state"])
        cfg = self.cfg

        if ev.type == "RINGING":
            if state == C.RESERVED:   # dial response was lost (timeout / crash) but the call exists
                advance(c, call, C.INITIATED, now, provider_call_id=ev.provider_call_id, initiated_at=ev.at)
                call = get_call(c, call["id"])
                self._agent_dialing(c, call, now)
            return "applied" if advance(c, call, C.RINGING, now) else "ignored_stale"

        if ev.type == "ANSWERED":
            if state in TERMINAL_CALL_STATES:
                # We already cancelled/failed it but the borrower picked up. Make sure
                # the line is torn down and count it honestly as an abandon.
                if not call["was_answered"]:
                    c.execute("""UPDATE calls SET was_answered=1, answered_at=?,
                                 end_reason=CASE WHEN state IN ('CANCELLED','FAILED') THEN 'late_answer'
                                                 ELSE end_reason END WHERE id=?""", (ev.at, call["id"]))
                if state != C.COMPLETED:
                    enqueue(c, call["id"], "HANGUP", now, rearm=True)
                return "late_answer_on_terminal"
            if not advance(c, call, C.ANSWERED, now, answered_at=min(ev.at, now), was_answered=1):
                return "ignored_stale"            # e.g. duplicate ANSWERED after CONNECTED
            call = get_call(c, call["id"])
            if call["agent_id"]:
                # Progressive: the agent is already waiting on this call.
                n = c.execute("""UPDATE agents SET state='CONNECTED', version=version+1, state_since=?
                                 WHERE id=? AND call_id=? AND state IN ('RESERVED','DIALING')""",
                              (now, call["agent_id"], call["id"])).rowcount
                if n == 1:
                    advance(c, call, C.CONNECTED, now, connected_at=now)
                    enqueue(c, call["id"], "BRIDGE", now)
                    return "connected_bound_agent"
                c.execute("UPDATE calls SET agent_id=NULL WHERE id=?", (call["id"],))
                call = get_call(c, call["id"])
            if connect_answered(c, call, now, cfg):
                return "connected_pool_agent"
            return "answered_waiting"         # reconciler abandons it after the grace period

        if ev.type == "COMPLETED":
            if state in TERMINAL_CALL_STATES:
                return "ignored_terminal"
            if state == C.CONNECTED:
                finalize(c, call, C.COMPLETED, "normal", now, cfg)
                return "completed"
            if ev.answered or state == C.ANSWERED:
                # Borrower picked up but was never bridged to an agent.
                finalize(c, call, C.ABANDONED, "answered_not_connected", now, cfg, answered=True)
                return "abandoned"
            finalize(c, call, C.FAILED, ev.reason or "no_answer", now, cfg)
            return "failed"

        if ev.type == "FAILED":
            if state in TERMINAL_CALL_STATES:
                return "ignored_terminal"
            if state in (C.ANSWERED, C.CONNECTED):
                # A FAILED after an answer contradicts what we know; trust the answer and
                # let COMPLETED / the reconciler close it.
                return "ignored_contradictory"
            finalize(c, call, C.FAILED, ev.reason or "provider_error", now, cfg)
            return "failed"

        return "unknown_type"

    @staticmethod
    def _agent_dialing(c, call, now):
        if call["agent_id"]:
            c.execute("""UPDATE agents SET state='DIALING', version=version+1, state_since=?
                         WHERE id=? AND call_id=? AND state='RESERVED'""", (now, call["agent_id"], call["id"]))
