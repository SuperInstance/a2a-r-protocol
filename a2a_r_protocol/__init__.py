"""A2A-R Protocol — Reliable Agent-to-Agent messaging.

Extends Google's A2A with acknowledgments, retries, ordering guarantees,
and flow control for reliable agent communication.

Example::

    from a2a_r_protocol import ReliableSession, ReliableMessage, QoS

    session = ReliableSession("agent-1")
    session.connect("agent-2")

    msg = ReliableMessage(payload={"action": "hello"}, qos=QoS.RELIABLE)
    session.send(msg)
"""

from .message import ReliableMessage, QoS, MessageStatus
from .ack import AckManager
from .order import OrderManager
from .session import ReliableSession
from .protocol import A2RProtocol

__all__ = [
    "ReliableMessage",
    "QoS",
    "MessageStatus",
    "AckManager",
    "OrderManager",
    "ReliableSession",
    "A2RProtocol",
]
__version__ = "0.1.0"
