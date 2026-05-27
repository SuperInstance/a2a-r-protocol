"""Reliable message with acknowledgment tracking and sequence numbers."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, IntEnum
from typing import Any, Dict, Optional


class QoS(IntEnum):
    """Quality-of-service levels for message delivery."""
    BEST_EFFORT = 0       # Fire and forget
    ACKNOWLEDGED = 1      # Requires ACK, single retry
    RELIABLE = 2          # Requires ACK, multiple retries with backoff
    ORDERED = 3           # RELIABLE + in-order delivery guarantee


class MessageStatus(Enum):
    """Lifecycle states of a reliable message."""
    PENDING = "pending"
    SENT = "sent"
    ACKED = "acked"
    NACKED = "nacked"
    TIMED_OUT = "timed_out"
    RETRYING = "retrying"
    DELIVERED = "delivered"  # ACKed + ordering window satisfied
    EXPIRED = "expired"


@dataclass
class ReliableMessage:
    """A message with reliability metadata.

    Attributes:
        payload: Application-level data.
        source_id: Sender agent identifier.
        target_id: Recipient agent identifier (or "broadcast").
        qos: Delivery guarantee level.
        message_id: Unique message identifier (auto-generated if empty).
        sequence: Monotonically-increasing sequence number within a session.
        correlation_id: Links request/response pairs.
        timestamp: Creation time (epoch seconds, auto-set).
        ttl_seconds: Time-to-live; message expires after this many seconds.
        max_retries: Maximum number of retransmission attempts.
        retry_count: Current number of retries performed.
        status: Current delivery status.
        metadata: Arbitrary key-value metadata.
    """

    payload: Dict[str, Any] = field(default_factory=dict)
    source_id: str = ""
    target_id: str = ""
    qos: QoS = QoS.ACKNOWLEDGED
    message_id: str = ""
    sequence: int = 0
    correlation_id: str = ""
    timestamp: float = 0.0
    ttl_seconds: float = 30.0
    max_retries: int = 3
    retry_count: int = 0
    status: MessageStatus = MessageStatus.PENDING
    metadata: Dict[str, Any] = field(default_factory=dict)

    # Internal: last send time for timeout calculation
    _last_sent_at: float = 0.0

    def __post_init__(self):
        if not self.message_id:
            self.message_id = uuid.uuid4().hex[:16]
        if not self.timestamp:
            self.timestamp = time.time()
        if not self.correlation_id:
            self.correlation_id = self.message_id
        self._last_sent_at = self.timestamp

    # ── helpers ──────────────────────────────────────────────────

    @property
    def requires_ack(self) -> bool:
        return self.qos >= QoS.ACKNOWLEDGED

    @property
    def requires_ordering(self) -> bool:
        return self.qos >= QoS.ORDERED

    @property
    def age_seconds(self) -> float:
        return time.time() - self.timestamp

    @property
    def is_expired(self) -> bool:
        return self.age_seconds > self.ttl_seconds

    @property
    def retries_remaining(self) -> int:
        return max(0, self.max_retries - self.retry_count)

    @property
    def elapsed_since_send(self) -> float:
        if self._last_sent_at == 0:
            return 0.0
        return time.time() - self._last_sent_at

    def mark_sent(self) -> None:
        self._last_sent_at = time.time()
        if self.status == MessageStatus.PENDING or self.status == MessageStatus.RETRYING:
            self.status = MessageStatus.SENT

    def mark_retry(self) -> None:
        self.retry_count += 1
        self._last_sent_at = time.time()
        self.status = MessageStatus.RETRYING

    def mark_acked(self) -> None:
        self.status = MessageStatus.ACKED

    def mark_nacked(self) -> None:
        self.status = MessageStatus.NACKED

    def mark_delivered(self) -> None:
        self.status = MessageStatus.DELIVERED

    def mark_expired(self) -> None:
        self.status = MessageStatus.EXPIRED

    def mark_timed_out(self) -> None:
        self.status = MessageStatus.TIMED_OUT

    # ── serialization ────────────────────────────────────────────

    def to_dict(self) -> Dict[str, Any]:
        return {
            "message_id": self.message_id,
            "source_id": self.source_id,
            "target_id": self.target_id,
            "qos": int(self.qos),
            "sequence": self.sequence,
            "correlation_id": self.correlation_id,
            "timestamp": self.timestamp,
            "ttl_seconds": self.ttl_seconds,
            "max_retries": self.max_retries,
            "retry_count": self.retry_count,
            "status": self.status.value,
            "payload": self.payload,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ReliableMessage:
        return cls(
            message_id=data.get("message_id", ""),
            source_id=data.get("source_id", ""),
            target_id=data.get("target_id", ""),
            qos=QoS(data.get("qos", 1)),
            sequence=data.get("sequence", 0),
            correlation_id=data.get("correlation_id", ""),
            timestamp=data.get("timestamp", 0.0),
            ttl_seconds=data.get("ttl_seconds", 30.0),
            max_retries=data.get("max_retries", 3),
            retry_count=data.get("retry_count", 0),
            status=MessageStatus(data.get("status", "pending")),
            payload=data.get("payload", {}),
            metadata=data.get("metadata", {}),
        )

    def clone_for_retry(self) -> ReliableMessage:
        """Create a copy suitable for retransmission."""
        import copy
        dupe = copy.copy(self)
        dupe.message_id = uuid.uuid4().hex[:16]
        dupe.timestamp = time.time()
        dupe._last_sent_at = dupe.timestamp
        return dupe

    def __repr__(self) -> str:
        return (
            f"ReliableMessage(id={self.message_id!r}, seq={self.sequence}, "
            f"qos={self.qos.name}, status={self.status.value})"
        )
