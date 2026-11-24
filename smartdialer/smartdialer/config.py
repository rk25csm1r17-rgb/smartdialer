from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DialerConfig:
    wrap_up_s: float = 10.0              # system-timed wrap-up: its end time is known in advance
    abandon_grace_s: float = 2.0         # answered borrower must reach an agent within this
    max_attempts: int = 3                # real (no-answer / busy) attempts per borrower
    retry_backoff_s: float = 600.0       # wait before re-dialling after no-answer
    system_retry_base_s: float = 5.0     # our/provider fault: retry sooner, exponential
    max_system_failures: int = 6
    abandon_callback_delay_s: float = 120.0
    permit_ttl_s: float = 5.0            # an authorised call not dialled within this is cancelled
    setup_timeout_s: float = 20.0        # INITIATED and no provider event at all
    ring_timeout_s: float = 45.0         # RINGING and no outcome
    max_call_s: float = 3600.0
    command_lease_s: float = 2.0             # short: a crashed worker's BRIDGE must be retried fast
    command_max_attempts: int = 3
    agent_heartbeat_timeout_s: float = 15.0
