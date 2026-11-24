"""Call Allocator + outbox dispatcher.

allocate():  one write transaction that (1) sheds excess in-flight calls if
             capacity collapsed, (2) asks the Safety Controller for a Decision,
             (3) claims exactly `decision.approved` borrowers (and agents in
             progressive mode) and inserts the calls + DIAL commands.
dispatch():  executes outbox commands (DIAL / BRIDGE / HANGUP) under a lease,
             always *outside* any DB transaction, because provider calls are slow
             and can hang.
"""
from __future__ import annotations

import uuid

from .calls import (claim_available_agent, enqueue, finalize, get_call, advance)
from .config import DialerConfig
from .pacing import PacingProposal
from .providers.base import ProviderError, ProviderTimeout
from .providers.router import ProviderRouter
from .safety import Decision, SafetyController
from .states import AgentState as A, CallState as C


class WorkerCrash(BaseException):
    """Raised by test/demo crash hooks to simulate the process dying at a precise point."""


class CallAllocator:
    def __init__(self, store, clock, safety: SafetyController, router: ProviderRouter,
                 cfg: DialerConfig, worker_id: str, seed: int = 0):
        self.store, self.clock, self.safety, self.router, self.cfg = store, clock, safety, router, cfg
        self.worker_id = worker_id
        self.stats = {"shed": 0}
        self.crash_hook = lambda point, **kw: None   # demos/tests replace this to kill the worker mid-flight

    def _new_id(self) -> str:
        # Truly random (uuid4), never a per-worker seeded PRNG: two worker incarnations
        # with the same id/seed (a crashed worker that restarts, or same-named workers in
        # a test) must never generate colliding call ids. Call ids are opaque, so this
        # does not affect the reproducibility of simulation *behaviour* (that comes from
        # borrower ordering and the providers' own RNGs).
        return uuid.uuid4().hex

    # ------------------------------------------------------------------ allocate
    def allocate(self, proposal: PacingProposal) -> Decision:
        with self.store.tx() as c:
            now = self.clock.now()          # read the clock AFTER taking the write lock
            self._shed(c, now)
            d = self.safety.authorize(c, proposal, now, self.worker_id)
            placed = 0
            for _ in range(d.approved):
                cid = self._new_id()
                b = c.execute(
                    """UPDATE borrowers SET state='IN_CALL', call_id=?
                       WHERE id=(SELECT id FROM borrowers WHERE state='READY' AND next_attempt_at<=?
                                 ORDER BY priority DESC, next_attempt_at LIMIT 1)
                         AND state='READY' RETURNING id""", (cid, now)).fetchone()
                if b is None:
                    break                                  # campaign list exhausted for now
                agent = None
                if d.mode == "progressive":
                    agent = claim_available_agent(c, cid, A.RESERVED, now)
                    if agent is None:                      # can't happen under the write lock, but be strict
                        c.execute("UPDATE borrowers SET state='READY', call_id=NULL WHERE id=?", (b["id"],))
                        break
                c.execute("""INSERT INTO calls(id, mode, borrower_id, agent_id, state, decision_id, created_at, updated_at)
                             VALUES (?,?,?,?,?,?,?,?)""",
                          (cid, d.mode, b["id"], agent, C.RESERVED.value, d.id, now, now))
                enqueue(c, cid, "DIAL", now)
                placed += 1
            if placed != d.approved:
                c.execute("UPDATE safety_decisions SET detail=json_set(detail, '$.placed', ?) WHERE id=?",
                          (placed, d.id))
        return d

    def _shed(self, c, now: float) -> None:
        excess, info = self.safety.shed_target(c, now)
        if excess <= 0:
            return
        states = ("'RESERVED','INITIATED','RINGING'" if self.safety.cfg.shed_ringing
                  else "'RESERVED','INITIATED'")
        rows = c.execute(f"""SELECT * FROM calls WHERE agent_id IS NULL AND state IN ({states})
                             ORDER BY created_at DESC LIMIT ?""", (excess,)).fetchall()
        for row in rows:
            if finalize(c, row, C.CANCELLED, "shed", now, self.cfg, hangup=True):
                self.stats["shed"] += 1

    # ------------------------------------------------------------------ outbox
    def dispatch(self, limit: int = 1000) -> int:
        done = 0
        while done < limit:
            now = self.clock.now()
            with self.store.tx() as c:
                cmd = c.execute(
                    """UPDATE commands SET owner=?, lease_until=?, attempts=attempts+1
                       WHERE id=(SELECT id FROM commands WHERE status='PENDING' AND lease_until<=?
                                 ORDER BY id LIMIT 1)
                       RETURNING *""", (self.worker_id, now + self.cfg.command_lease_s, now)).fetchone()
            if cmd is None:
                break
            self.crash_hook("after_lease", cmd=cmd)
            getattr(self, f"_exec_{cmd['kind'].lower()}")(cmd)
            done += 1
        return done

    def _finish_cmd(self, c, cmd, status: str = "DONE", err: str | None = None):
        c.execute("UPDATE commands SET status=?, last_error=? WHERE id=? AND owner=?",
                  (status, err, cmd["id"], self.worker_id))

    def _retry_cmd(self, c, cmd, err: str):
        if cmd["attempts"] >= self.cfg.command_max_attempts:
            self._finish_cmd(c, cmd, "DEAD", err)
            return False
        backoff = 0.5 * (2 ** cmd["attempts"])
        c.execute("UPDATE commands SET lease_until=?, owner=NULL, last_error=? WHERE id=? AND owner=?",
                  (self.clock.now() + backoff, err, cmd["id"], self.worker_id))
        return True

    def _exec_dial(self, cmd):
        cfg, call_id = self.cfg, cmd["call_id"]
        with self.store.tx() as c:
            call = get_call(c, call_id)
            if call is None or call["state"] != C.RESERVED.value:
                self._finish_cmd(c, cmd)                       # already moved on (event arrived / cancelled)
                return
            expired = self.clock.now() - call["created_at"] > cfg.permit_ttl_s
            if expired and not call["provider"]:
                # Approval is stale and we never contacted a provider: drop it.
                finalize(c, call, C.CANCELLED, "permit_expired", self.clock.now(), cfg)
                self._finish_cmd(c, cmd)
                return
            pname = call["provider"]
            if pname is None:
                pname = self.router.choose()
                if pname is None:
                    finalize(c, call, C.FAILED, "no_provider", self.clock.now(), cfg)
                    self._finish_cmd(c, cmd)
                    return
                # Pin the provider BEFORE dialling: every retry goes to the same one.
                c.execute("UPDATE calls SET provider=? WHERE id=?", (pname, call_id))
            phone = c.execute("SELECT phone FROM borrowers WHERE id=?", (call["borrower_id"],)).fetchone()[0]
        prov = self.router.get(pname)

        if expired:
            # A previous attempt (maybe by a crashed worker) may have reached the provider.
            # Never re-dial on a stale approval: adopt the call if it exists, else cancel.
            try:
                status = prov.lookup(call_id)
            except ProviderError:
                status = "unknown"
            with self.store.tx() as c:
                call = get_call(c, call_id)
                if status not in (None, "unknown", "ended") and call["state"] == C.RESERVED.value:
                    self._mark_initiated(c, call, None)
                elif call["state"] == C.RESERVED.value:
                    finalize(c, call, C.CANCELLED, "permit_expired", self.clock.now(), cfg, hangup=True)
                self._finish_cmd(c, cmd)
            return

        try:
            pcid = prov.dial(call_id, phone)
        except ProviderTimeout as e:
            self.router.record(pname, False)
            with self.store.tx() as c:
                if not self._retry_cmd(c, cmd, str(e)):
                    call = get_call(c, call_id)
                    if call["state"] == C.RESERVED.value:
                        finalize(c, call, C.FAILED, "provider_timeout", self.clock.now(), cfg, hangup=True)
            return
        except ProviderError as e:
            self.router.record(pname, False)
            with self.store.tx() as c:
                call = get_call(c, call_id)
                finalize(c, call, C.FAILED, "provider_error", self.clock.now(), cfg)
                self._finish_cmd(c, cmd, "DONE", str(e))
            return
        self.router.record(pname, True)
        self.crash_hook("after_dial", cmd=cmd)
        with self.store.tx() as c:
            call = get_call(c, call_id)
            if call["state"] == C.RESERVED.value:
                self._mark_initiated(c, call, pcid)
            elif C(call["state"]) in (C.CANCELLED, C.FAILED, C.ABANDONED):
                # We gave up on it while the dial was in flight: make sure it's torn down.
                enqueue(c, call_id, "HANGUP", self.clock.now(), rearm=True)
            elif call["provider_call_id"] is None:
                c.execute("UPDATE calls SET provider_call_id=? WHERE id=?", (pcid, call_id))
            self._finish_cmd(c, cmd)

    def _mark_initiated(self, c, call, pcid):
        now = self.clock.now()
        advance(c, call, C.INITIATED, now, provider_call_id=pcid, initiated_at=now)
        if call["agent_id"]:
            c.execute("""UPDATE agents SET state='DIALING', version=version+1, state_since=?
                         WHERE id=? AND call_id=? AND state='RESERVED'""", (now, call["agent_id"], call["id"]))

    def _exec_bridge(self, cmd):
        with self.store.tx() as c:
            call = get_call(c, cmd["call_id"])
            if call is None or call["state"] != C.CONNECTED.value:
                self._finish_cmd(c, cmd)
                return
        try:
            self.router.get(call["provider"]).bridge(call["id"], call["agent_id"])
        except ProviderTimeout as e:
            with self.store.tx() as c:
                self._retry_cmd(c, cmd, str(e))
            return
        except ProviderError as e:
            with self.store.tx() as c:   # call already gone at the provider; COMPLETED will follow
                self._finish_cmd(c, cmd, "DONE", str(e))
            return
        with self.store.tx() as c:
            self._finish_cmd(c, cmd)

    def _exec_hangup(self, cmd):
        with self.store.tx() as c:
            call = get_call(c, cmd["call_id"])
        if call is None or not call["provider"]:
            with self.store.tx() as c:
                self._finish_cmd(c, cmd)
            return
        try:
            self.router.get(call["provider"]).hangup(call["id"])
        except ProviderError as e:
            with self.store.tx() as c:
                self._retry_cmd(c, cmd, str(e))
            return
        with self.store.tx() as c:
            self._finish_cmd(c, cmd)
