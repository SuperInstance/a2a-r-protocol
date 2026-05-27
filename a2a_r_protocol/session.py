"""Reliable session with connection state and flow control."""

from __future__ import annotations

import time
from enum import Enum
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set

from .message import MessageStatus, QoS, ReliableMessage
from .ack import AckManager
from .order import OrderManager


class SessionState(Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    SUSPENDED = "suspended"
    CLOSING = "closing"
    CLOSED = "closed"


@dataclass
class FlowControl:
    """Simple sliding-window flow control.

    Attributes:
        window_size: Maximum in-flight (un-ACKed) messages.
        send_rate_limit: Max messages per second (0 = unlimited).
    """
    window_size: int = 64
    send_rate_limit: float = 0.0  # unlimited

    _sent_timestamps: List[float] = field(default_factory=list)

    @property
    def window_available(self) -> int:
        return self.window_size

    def check_rate(self) -> bool:
        """Return True if we're within rate limits."""
        if self.send_rate_limit <= 0:
            return True
        now = time.time()
        # Keep only timestamps within the last second
        self._sent_timestamps = [t for t in self._sent_timestamps if now - t < 1.0]
        return len(self._sent_timestamps) < self.send_rate_limit

    def record_send(self) -> None:
        self._sent_timestamps.append(time.time())


class ReliableSession:
    """A reliable communication session between two agents.

    Manages connection lifecycle, acknowledgment tracking, ordering,
    flow control, and message send/receive with reliability guarantees.

    Args:
        agent_id: Local agent identifier.
        default_qos: Default QoS level for messages without explicit QoS.
        ack_timeout: ACK timeout in seconds.
        max_retries: Default max retries per message.
    """

    def __init__(
        self,
        agent_id: str,
        default_qos: QoS = QoS.ACKNOWLEDGED,
        ack_timeout: float = 2.0,
        max_retries: int = 3,
    ):
        self.agent_id = agent_id
        self.default_qos = default_qos

        self._state = SessionState.DISCONNECTED
        self._remote_id: Optional[str] = None
        self._connected_at: Optional[float] = None

        self._ack_manager = AckManager(
            default_timeout=ack_timeout,
            max_retries=max_retries,
        )
        self._order_manager = OrderManager()
        self._flow_control = FlowControl()

        # Outbound messages awaiting ACK: message_id -> ReliableMessage
        self._inflight: Dict[str, ReliableMessage] = {}
        # Received message log for dedup
        self._received_ids: Set[str] = set()
        # Delivered messages (passed to application)
        self._delivered: List[ReliableMessage] = []

        # Callbacks
        self._on_message: Optional[Callable[[ReliableMessage], None]] = None
        self._on_connect: Optional[Callable[[str], None]] = None
        self._on_disconnect: Optional[Callable[[str], None]] = None
        self._on_error: Optional[Callable[[str, str], None]] = None

        # Stats
        self._stats_sent = 0
        self._stats_received = 0
        self._stats_acked = 0
        self._stats_retries = 0
        self._stats_timeouts = 0

        # Wire up ack manager callbacks
        self._ack_manager.on_retry(self._handle_retry)
        self._ack_manager.on_timeout(self._handle_timeout)

    # ── configuration ────────────────────────────────────────────

    def on_message(self, callback: Callable[[ReliableMessage], None]) -> None:
        self._on_message = callback

    def on_connect(self, callback: Callable[[str], None]) -> None:
        self._on_connect = callback

    def on_disconnect(self, callback: Callable[[str], None]) -> None:
        self._on_disconnect = callback

    def on_error(self, callback: Callable[[str, str], None]) -> None:
        self._on_error = callback

    # ── connection lifecycle ─────────────────────────────────────

    @property
    def state(self) -> SessionState:
        return self._state

    @property
    def remote_id(self) -> Optional[str]:
        return self._remote_id

    @property
    def is_connected(self) -> bool:
        return self._state == SessionState.CONNECTED

    def connect(self, remote_id: str) -> None:
        """Establish a session with a remote agent."""
        if self._state not in (SessionState.DISCONNECTED, SessionState.CLOSED):
            raise RuntimeError(f"Cannot connect from state {self._state.value}")
        self._state = SessionState.CONNECTING
        self._remote_id = remote_id
        # Simulate handshake completion
        self._state = SessionState.CONNECTED
        self._connected_at = time.time()
        if self._on_connect:
            self._on_connect(remote_id)

    def disconnect(self) -> None:
        """Close the session."""
        if self._state == SessionState.DISCONNECTED:
            return
        old_remote = self._remote_id
        self._state = SessionState.CLOSED
        self._remote_id = None
        self._connected_at = None
        if self._on_disconnect and old_remote:
            self._on_disconnect(old_remote)

    def suspend(self) -> None:
        """Temporarily suspend the session."""
        if self._state == SessionState.CONNECTED:
            self._state = SessionState.SUSPENDED

    def resume(self) -> None:
        """Resume a suspended session."""
        if self._state == SessionState.SUSPENDED:
            self._state = SessionState.CONNECTED

    # ── send / receive ───────────────────────────────────────────

    def send(self, message: ReliableMessage) -> ReliableMessage:
        """Send a message with reliability guarantees.

        Assigns sequence number, tracks for ACK, applies flow control.
        Returns the (possibly modified) message.
        """
        if not self.is_connected:
            raise RuntimeError(f"Cannot send in state {self._state.value}")

        if not self._flow_control.check_rate():
            raise RuntimeError("Rate limit exceeded")

        # Set source/target
        message.source_id = self.agent_id
        message.target_id = self._remote_id or message.target_id

        # Assign QoS
        if message.qos == QoS.BEST_EFFORT and self.default_qos > QoS.BEST_EFFORT:
            message.qos = self.default_qos

        # Assign sequence number for ordered messages
        if message.requires_ordering and message.sequence == 0:
            message.sequence = self._order_manager.next_sequence(self.agent_id)

        message.mark_sent()
        self._flow_control.record_send()

        if message.requires_ack:
            self._ack_manager.track(message)
            self._inflight[message.message_id] = message

        self._stats_sent += 1
        return message

    def receive(self, message: ReliableMessage) -> List[ReliableMessage]:
        """Process an incoming message.

        Handles dedup, ordering, and delivers messages to the application
        in-order when applicable. Returns delivered messages.
        """
        # Dedup
        if message.message_id in self._received_ids:
            return []
        self._received_ids.add(message.message_id)
        self._stats_received += 1

        # Process through order manager
        delivered = self._order_manager.receive(message)
        for msg in delivered:
            self._delivered.append(msg)
            if self._on_message:
                self._on_message(msg)

        return delivered

    def handle_ack(self, message_id: str) -> bool:
        """Process an acknowledgment for a tracked message."""
        record = self._ack_manager.acknowledge(message_id)
        if record is None:
            return False

        msg = self._inflight.pop(message_id, None)
        if msg:
            msg.mark_acked()
            if not msg.requires_ordering:
                msg.mark_delivered()

        self._stats_acked += 1
        return True

    def handle_nack(self, message_id: str, reason: str = "") -> None:
        """Process a negative acknowledgment."""
        self._ack_manager.negative_ack(message_id, reason)
        msg = self._inflight.get(message_id)
        if msg:
            msg.mark_nacked()

    # ── tick / maintenance ───────────────────────────────────────

    def tick(self) -> None:
        """Drive the session forward: check timeouts, deliver ordered messages.

        Should be called periodically by the application.
        """
        if not self.is_connected:
            return
        self._ack_manager.check_timeouts()

    # ── internal handlers ────────────────────────────────────────

    def _handle_retry(self, message_id: str, sequence: int) -> None:
        msg = self._inflight.get(message_id)
        if msg:
            msg.mark_retry()
            self._stats_retries += 1

    def _handle_timeout(self, message_id: str, sequence: int) -> None:
        msg = self._inflight.pop(message_id, None)
        if msg:
            msg.mark_timed_out()
            self._stats_timeouts += 1
            if self._on_error:
                self._on_error(message_id, "timeout")

    # ── query ────────────────────────────────────────────────────

    @property
    def inflight_count(self) -> int:
        return len(self._inflight)

    def get_stats(self) -> Dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "remote_id": self._remote_id,
            "state": self._state.value,
            "sent": self._stats_sent,
            "received": self._stats_received,
            "acked": self._stats_acked,
            "retries": self._stats_retries,
            "timeouts": self._stats_timeouts,
            "inflight": len(self._inflight),
            "pending_acks": self._ack_manager.pending_count,
            "buffered_ordered": self._order_manager.buffered_count(),
        }
