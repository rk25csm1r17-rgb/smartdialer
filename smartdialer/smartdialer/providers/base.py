"""Telecom provider interface. The dialer only ever sees this.

Contract every adapter must meet:
  * `dial(call_id, phone)` is idempotent on `call_id` (we pass our call id as the
    provider idempotency key / custom param). Calling it twice must not ring the
    borrower twice; it returns the existing provider call id.
  * `bridge` and `hangup` are idempotent.
  * Events are delivered at-least-once, in any order, via the event sink, and
    always carry our `call_id` back.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Optional


class ProviderError(Exception):
    """Definitive rejection: the call was NOT created."""


class ProviderTimeout(ProviderError):
    """Unknown outcome: the call may or may not have been created."""


@dataclass(frozen=True)
class ProviderEvent:
    provider: str
    event_id: str            # provider's id for this delivery's logical event
    call_id: str             # our call id, echoed back
    provider_call_id: str
    type: str                # RINGING | ANSWERED | COMPLETED | FAILED
    at: float                # provider-side timestamp of the event
    answered: bool = False   # COMPLETED: did the borrower ever pick up?
    reason: str = ""         # FAILED: no_answer | busy | provider_error | hangup


EventSink = Callable[[ProviderEvent], None]


class TelecomProvider(ABC):
    name: str

    @abstractmethod
    def dial(self, call_id: str, phone: str) -> str: ...

    @abstractmethod
    def bridge(self, call_id: str, agent_id: str) -> None: ...

    @abstractmethod
    def hangup(self, call_id: str) -> None: ...

    @abstractmethod
    def lookup(self, call_id: str) -> Optional[str]:
        """Return the provider's view of the call status, or None if unknown."""

    @abstractmethod
    def set_event_sink(self, sink: EventSink) -> None: ...
