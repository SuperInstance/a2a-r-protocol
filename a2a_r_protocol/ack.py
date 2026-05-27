"""Acknowledgment manager with timeout-based retransmission."""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from .message import MessageStatus, QoS, ReliableMessage


@dataclass
class AckRecord:
    """Tracks acknowledgment state for a single message."""
    message_id: str
    sequence: int
    sent_at: float
    timeout_seconds: float
    max_retries: int
    retries_so_far: int = 0
    acked_at: Optional[float] = None
    last_retry_at: Optional[float] = None
    nack_reason: str = ""

    @property
    def is_acked(self) -> bool:
        return self.acked_at is not None

    @property
    def is_timed_out(self) -> bool:
        deadline = self.last_retry_at or self.sent_at
        return not self.is_acked and (time.time() - deadline) > self.timeout_seconds

    @property
    def retries_remaining(self) -> int:
        return max(0, self.max_retries - self.retries_so_far)

    @property
    def can_retry(self) -> bool:
        return not self.is_acked and self.retries_remaining > 0


class AckManager:
    """Manages message acknowledgments and retransmission scheduling.

    Args:
        default_timeout: Default ACK timeout in seconds.
        max_retries: Default maximum retries per message.
        backoff_factor: Multiplier for exponential backoff on retries.
    """

    def __init__(
        self,
        default_timeout: float = 2.0,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        self.default_timeout = default_timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

        # message_id -> AckRecord
        self._pending: Dict[str, AckRecord] = {}
        # Acked message IDs (kept for is_acked queries)
        self._acked_set: Set[str] = set()
        # Callbacks
        self._on_timeout: Optional[Callable[[str, int], None]] = None
        self._on_retry: Optional[Callable[[str, int], None]] = None

    # ── configuration ────────────────────────────────────────────

    def on_timeout(self, callback: Callable[[str, int], None]) -> None:
        """Register callback(message_id, sequence) when a message fully times out."""
        self._on_timeout = callback

    def on_retry(self, callback: Callable[[str, int], None]) -> None:
        """Register callback(message_id, sequence) when a retry is needed."""
        self._on_retry = callback

    # ── core operations ──────────────────────────────────────────

    def track(self, message: ReliableMessage) -> None:
        """Start tracking a message for acknowledgment."""
        if not message.requires_ack:
            return
        timeout = self.default_timeout
        if message.qos == QoS.RELIABLE or message.qos == QoS.ORDERED:
            timeout = self.default_timeout  # uses default; callers can adjust
        self._pending[message.message_id] = AckRecord(
            message_id=message.message_id,
            sequence=message.sequence,
            sent_at=message.timestamp,
            timeout_seconds=timeout,
            max_retries=min(message.max_retries, self.max_retries),
        )

    def acknowledge(self, message_id: str) -> Optional[AckRecord]:
        """Mark a message as acknowledged.

        Returns the AckRecord if found, None otherwise.
        """
        record = self._pending.pop(message_id, None)
        if record is not None:
            record.acked_at = time.time()
            self._acked_set.add(message_id)
        return record

    def negative_ack(self, message_id: str, reason: str = "") -> Optional[AckRecord]:
        """Process a NACK. The message stays pending for retry.

        Returns the AckRecord if found, None otherwise.
        """
        record = self._pending.get(message_id)
        if record is not None:
            record.nack_reason = reason
        return record

    # ── timeout / retry checking ─────────────────────────────────

    def check_timeouts(self) -> List[Tuple[str, int]]:
        """Check all pending messages for timeouts.

        Returns list of (message_id, sequence) that need retry or have expired.
        Triggers retry callbacks for retriable messages, timeout callbacks for
        messages that have exhausted retries.
        """
        now = time.time()
        timed_out: List[Tuple[str, int]] = []

        for msg_id, record in list(self._pending.items()):
            if record.is_acked:
                continue

            effective_sent = record.last_retry_at or record.sent_at
            elapsed = now - effective_sent

            if elapsed <= record.timeout_seconds:
                continue

            # Timeout detected
            if record.can_retry:
                record.retries_so_far += 1
                record.last_retry_at = now
                record.timeout_seconds *= self.backoff_factor
                if self._on_retry:
                    self._on_retry(msg_id, record.sequence)
            else:
                # Fully timed out
                timed_out.append((msg_id, record.sequence))
                del self._pending[msg_id]
                if self._on_timeout:
                    self._on_timeout(msg_id, record.sequence)

        return timed_out

    # ── query ────────────────────────────────────────────────────

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    def is_acked(self, message_id: str) -> bool:
        """Check whether a message has already been acknowledged."""
        return message_id not in self._pending and message_id in self._acked_set

    def get_pending_message_ids(self) -> Set[str]:
        return set(self._pending.keys())

    def get_record(self, message_id: str) -> Optional[AckRecord]:
        return self._pending.get(message_id)

    def clear(self) -> None:
        self._pending.clear()
