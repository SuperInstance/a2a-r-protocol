"""High-level A2A-R protocol coordinator.

Ties together sessions, message routing, and reliability layers
into a unified protocol handler for multi-agent communication.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set

from .message import MessageStatus, QoS, ReliableMessage
from .ack import AckManager
from .order import OrderManager
from .session import ReliableSession, SessionState


class A2RProtocol:
    """Top-level A2A-R protocol handler.

    Manages multiple sessions, routes messages between agents,
    and coordinates reliability across the local agent's connections.

    Args:
        agent_id: Local agent identifier.
    """

    def __init__(self, agent_id: str):
        self.agent_id = agent_id
        self._sessions: Dict[str, ReliableSession] = {}
        self._handlers: Dict[str, List[Callable[[ReliableMessage], None]]] = {}
        self._default_qos = QoS.ACKNOWLEDGED

    # ── session management ───────────────────────────────────────

    def create_session(
        self,
        remote_id: str,
        qos: QoS | None = None,
        ack_timeout: float = 2.0,
        max_retries: int = 3,
    ) -> ReliableSession:
        """Create and connect a session to a remote agent."""
        if remote_id in self._sessions:
            raise ValueError(f"Session with {remote_id} already exists")

        session = ReliableSession(
            agent_id=self.agent_id,
            default_qos=qos or self._default_qos,
            ack_timeout=ack_timeout,
            max_retries=max_retries,
        )
        session.on_message(self._route_incoming)
        session.connect(remote_id)
        self._sessions[remote_id] = session
        return session

    def get_session(self, remote_id: str) -> Optional[ReliableSession]:
        return self._sessions.get(remote_id)

    def close_session(self, remote_id: str) -> None:
        session = self._sessions.pop(remote_id, None)
        if session:
            session.disconnect()

    def close_all(self) -> None:
        for remote_id in list(self._sessions):
            self.close_session(remote_id)

    # ── message routing ──────────────────────────────────────────

    def on(self, msg_type: str, handler: Callable[[ReliableMessage], None]) -> None:
        """Register a handler for a message type."""
        if msg_type not in self._handlers:
            self._handlers[msg_type] = []
        self._handlers[msg_type].append(handler)

    def send(
        self,
        remote_id: str,
        payload: Dict[str, Any],
        qos: QoS | None = None,
        msg_type: str = "task",
        ttl_seconds: float = 30.0,
    ) -> Optional[ReliableMessage]:
        """Send a reliable message to a remote agent."""
        session = self._sessions.get(remote_id)
        if not session or not session.is_connected:
            return None

        message = ReliableMessage(
            payload={"type": msg_type, **payload},
            target_id=remote_id,
            qos=qos or self._default_qos,
            ttl_seconds=ttl_seconds,
        )
        return session.send(message)

    def _route_incoming(self, message: ReliableMessage) -> None:
        """Route a received message to registered handlers."""
        msg_type = message.payload.get("type", "task")
        for handler in self._handlers.get(msg_type, []):
            try:
                handler(message)
            except Exception:
                pass

    # ── maintenance ──────────────────────────────────────────────

    def tick(self) -> None:
        """Drive all sessions forward."""
        for session in self._sessions.values():
            session.tick()

    # ── query ────────────────────────────────────────────────────

    @property
    def active_sessions(self) -> List[str]:
        return [rid for rid, s in self._sessions.items() if s.is_connected]

    def get_stats(self) -> Dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "sessions": len(self._sessions),
            "active": len(self.active_sessions),
            "per_session": {
                rid: s.get_stats() for rid, s in self._sessions.items()
            },
        }
