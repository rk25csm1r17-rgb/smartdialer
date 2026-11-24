"""Agent and call state machines.

Both machines are explicit tables. Every state change in the system goes through
`Store` compare-and-set updates whose WHERE clause includes the expected current
state, so an illegal or stale transition simply matches zero rows.
"""
from __future__ import annotations

from enum import Enum


class AgentState(str, Enum):
    OFFLINE = "OFFLINE"
    AVAILABLE = "AVAILABLE"
    RESERVED = "RESERVED"      # held for one specific call (progressive) before dialing
    DIALING = "DIALING"        # its bound call is at the provider, borrower not answered yet
    CONNECTED = "CONNECTED"    # talking to a borrower
    WRAP_UP = "WRAP_UP"        # after-call work; system-timed, so its end is known
    PAUSED = "PAUSED"


A = AgentState
AGENT_TRANSITIONS: dict[AgentState, set[AgentState]] = {
    A.OFFLINE: {A.AVAILABLE, A.PAUSED},
    A.AVAILABLE: {A.RESERVED, A.CONNECTED, A.PAUSED, A.OFFLINE},
    # RESERVED -> AVAILABLE: call failed / cancelled before dialing
    A.RESERVED: {A.DIALING, A.AVAILABLE, A.CONNECTED, A.OFFLINE, A.PAUSED},
    # DIALING -> AVAILABLE: no answer / busy / failed
    A.DIALING: {A.CONNECTED, A.AVAILABLE, A.OFFLINE, A.PAUSED},
    A.CONNECTED: {A.WRAP_UP},
    A.WRAP_UP: {A.AVAILABLE, A.PAUSED, A.OFFLINE},
    A.PAUSED: {A.AVAILABLE, A.OFFLINE},
}

# States in which the agent is "taken" by a call (the call_id column is set).
AGENT_BUSY = {A.RESERVED, A.DIALING, A.CONNECTED}


class CallState(str, Enum):
    RESERVED = "RESERVED"      # slot authorised by the Safety Controller, borrower (+agent) claimed
    INITIATED = "INITIATED"    # provider accepted the dial request
    RINGING = "RINGING"
    ANSWERED = "ANSWERED"      # borrower picked up, no agent bridged yet (predictive)
    CONNECTED = "CONNECTED"    # borrower bridged to an agent
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"          # no-answer, busy, provider error, timeout
    CANCELLED = "CANCELLED"    # we stopped it before the borrower answered
    ABANDONED = "ABANDONED"    # borrower answered and no agent reached them in time


C = CallState
TERMINAL_CALL_STATES = {C.COMPLETED, C.FAILED, C.CANCELLED, C.ABANDONED}
ACTIVE_CALL_STATES = [s for s in CallState if s not in TERMINAL_CALL_STATES]

# Monotonic rank. Non-terminal states only ever move forward (skips allowed),
# terminal states are absorbing. This one rule is what makes duplicate and
# out-of-order provider events harmless: ANSWERED after CONNECTED, RINGING after
# ANSWERED, anything after COMPLETED -> no-op.
CALL_RANK = {
    C.RESERVED: 1,
    C.INITIATED: 2,
    C.RINGING: 3,
    C.ANSWERED: 4,
    C.CONNECTED: 5,
    C.COMPLETED: 9,
    C.FAILED: 9,
    C.CANCELLED: 9,
    C.ABANDONED: 9,
}


def call_transition_allowed(current: CallState, new: CallState) -> bool:
    if current in TERMINAL_CALL_STATES:
        return False
    return CALL_RANK[new] > CALL_RANK[current]


def agent_transition_allowed(current: AgentState, new: AgentState) -> bool:
    return new in AGENT_TRANSITIONS[current]


class BorrowerState(str, Enum):
    READY = "READY"            # can be dialled once next_attempt_at has passed
    IN_CALL = "IN_CALL"        # claimed by exactly one active call
    DONE = "DONE"              # reached
    EXHAUSTED = "EXHAUSTED"    # max attempts used
