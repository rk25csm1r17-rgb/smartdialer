"""SQLite-backed source of truth.

Why SQLite: the assignment is about correctness of allocation under concurrency,
not about infrastructure. SQLite in WAL mode gives us real ACID transactions,
a single serialised writer (BEGIN IMMEDIATE), partial unique indexes and
multi-process access with zero setup. Every guarantee we rely on (conditional
UPDATE, unique index, transaction atomicity) exists identically in PostgreSQL,
which is the production target (see docs/ADR.md).

Rules followed everywhere:
  * The database is the only source of truth for agent / borrower / call state.
  * Every state change is a compare-and-set: `UPDATE ... WHERE id=? AND state=?`
    and the caller checks rowcount.
  * Partial unique indexes make "one active call per agent" and
    "one active call per borrower" impossible to violate even if application
    code has a bug.
"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from typing import Iterator

from .states import ACTIVE_CALL_STATES

_ACTIVE = ",".join(f"'{s.value}'" for s in ACTIVE_CALL_STATES)

SCHEMA = f"""
CREATE TABLE IF NOT EXISTS agents (
    id              TEXT PRIMARY KEY,
    state           TEXT NOT NULL,
    call_id         TEXT,
    version         INTEGER NOT NULL DEFAULT 0,
    state_since     REAL NOT NULL,
    wrap_until      REAL,
    heartbeat_at    REAL NOT NULL,
    pending_state   TEXT            -- logout/pause requested while on a call
);
CREATE INDEX IF NOT EXISTS agents_state ON agents(state, state_since);

CREATE TABLE IF NOT EXISTS borrowers (
    id              TEXT PRIMARY KEY,
    phone           TEXT NOT NULL,
    state           TEXT NOT NULL,
    priority        INTEGER NOT NULL DEFAULT 0,
    attempts        INTEGER NOT NULL DEFAULT 0,
    system_failures INTEGER NOT NULL DEFAULT 0,
    next_attempt_at REAL NOT NULL DEFAULT 0,
    call_id         TEXT
);
CREATE INDEX IF NOT EXISTS borrowers_ready ON borrowers(state, priority DESC, next_attempt_at);

CREATE TABLE IF NOT EXISTS calls (
    id               TEXT PRIMARY KEY,   -- also the provider idempotency key
    mode             TEXT NOT NULL,      -- progressive | predictive
    borrower_id      TEXT NOT NULL,
    agent_id         TEXT,               -- NULL for predictive until answered
    state            TEXT NOT NULL,
    provider         TEXT,
    provider_call_id TEXT,
    decision_id      INTEGER,            -- safety decision that authorised it
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL,
    initiated_at     REAL,
    answered_at      REAL,
    connected_at     REAL,
    ended_at         REAL,
    end_reason       TEXT,
    was_answered     INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS one_active_call_per_agent
    ON calls(agent_id) WHERE agent_id IS NOT NULL AND state IN ({_ACTIVE});
CREATE UNIQUE INDEX IF NOT EXISTS one_active_call_per_borrower
    ON calls(borrower_id) WHERE state IN ({_ACTIVE});
CREATE INDEX IF NOT EXISTS calls_state ON calls(state, updated_at);
CREATE INDEX IF NOT EXISTS calls_ended ON calls(ended_at);
CREATE INDEX IF NOT EXISTS calls_created ON calls(created_at);

-- Provider event inbox: (provider, event_id) dedupes redelivered webhooks.
CREATE TABLE IF NOT EXISTS provider_events (
    provider     TEXT NOT NULL,
    event_id     TEXT NOT NULL,
    call_id      TEXT,
    type         TEXT NOT NULL,
    received_at  REAL NOT NULL,
    effect       TEXT NOT NULL,
    PRIMARY KEY (provider, event_id)
);

-- Transactional outbox for side effects on the provider (DIAL / BRIDGE / HANGUP).
-- Written in the same transaction as the state change; executed by any worker
-- holding a lease; idempotent at the provider via the call id.
CREATE TABLE IF NOT EXISTS commands (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    call_id     TEXT NOT NULL,
    kind        TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING | DONE | DEAD
    owner       TEXT,
    lease_until REAL NOT NULL DEFAULT 0,
    attempts    INTEGER NOT NULL DEFAULT 0,
    created_at  REAL NOT NULL,
    last_error  TEXT,
    UNIQUE (call_id, kind)
);
CREATE INDEX IF NOT EXISTS commands_pending ON commands(status, lease_until);

CREATE TABLE IF NOT EXISTS safety_decisions (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts         REAL NOT NULL,
    worker     TEXT NOT NULL,
    mode       TEXT NOT NULL,
    requested  INTEGER NOT NULL,
    approved   INTEGER NOT NULL,
    action     TEXT NOT NULL,
    detail     TEXT NOT NULL
);
"""


class Store:
    """Thread-local SQLite connections onto one database file.

    Use `path=":memory:"` only from a single thread (the simulator)."""

    def __init__(self, path: str = ":memory:"):
        self.path = path
        self._local = threading.local()
        self.conn().executescript(SCHEMA)

    def conn(self) -> sqlite3.Connection:
        c = getattr(self._local, "conn", None)
        if c is None:
            c = sqlite3.connect(self.path, timeout=30, isolation_level=None, check_same_thread=False)
            c.row_factory = sqlite3.Row
            if self.path != ":memory:":
                c.execute("PRAGMA journal_mode=WAL")
                c.execute("PRAGMA synchronous=NORMAL")
            c.execute("PRAGMA busy_timeout=30000")
            self._local.conn = c
        return c

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        """A write transaction. BEGIN IMMEDIATE takes the write lock up front, so
        read-then-write logic inside the block cannot interleave with another
        writer (no lost updates, no write-skew between workers)."""
        c = self.conn()
        if c.in_transaction:  # nested use: join the outer transaction
            yield c
            return
        c.execute("BEGIN IMMEDIATE")
        try:
            yield c
        except BaseException:
            c.execute("ROLLBACK")
            raise
        else:
            c.execute("COMMIT")

    def read(self) -> sqlite3.Connection:
        return self.conn()
