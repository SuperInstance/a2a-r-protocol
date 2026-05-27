# a2a-r-protocol

A2A-R: **Reliable** Agent-to-Agent protocol — extends Google's A2A with acknowledgments, retries, ordering guarantees, and flow control.

## What A2A-R Adds

| Feature | A2A Base | A2A-R |
|---------|----------|-------|
| Delivery | Fire-and-forget | ACK/NACK with retry tracking |
| Retries | None | Configurable retries with exponential backoff |
| Ordering | None | Sequence numbers with gap detection and in-order delivery |
| Flow Control | None | Sliding window + rate limiting |
| QoS Levels | None | Best-effort, Acknowledged, Reliable, Ordered |

## Installation

```bash
pip install a2a-r-protocol
```

## Quick Start

### Send a reliable message

```python
from a2a_r_protocol import ReliableSession, ReliableMessage, QoS

# Create a session
session = ReliableSession("agent-1")
session.connect("agent-2")

# Send with acknowledgment guarantees
msg = ReliableMessage(payload={"action": "navigate", "lat": 61.2, "lon": -149.9})
sent = session.send(msg)

# Handle ACK (on the receiver side)
session.handle_ack(sent.message_id)
```

### Multi-agent protocol

```python
from a2a_r_protocol import A2RProtocol, QoS

protocol = A2RProtocol("vessel-01")

# Create sessions
protocol.create_session("vessel-02", qos=QoS.ORDERED)

# Register message handlers
protocol.on("navigate", lambda msg: print(f"Navigate: {msg.payload}"))

# Send
protocol.send("vessel-02", {"type": "navigate", "heading": 270}, qos=QoS.ORDERED)

# Drive the protocol
protocol.tick()
```

### In-order delivery

```python
from a2a_r_protocol import ReliableMessage, QoS

# Messages arrive out of order but are delivered in sequence
session = ReliableSession("agent-1", default_qos=QoS.ORDERED)
session.connect("agent-2")

# Receiver side: messages are buffered until gaps are filled
msg3 = ReliableMessage(source_id="agent-2", sequence=2, qos=QoS.ORDERED)
msg1 = ReliableMessage(source_id="agent-2", sequence=0, qos=QoS.ORDERED)
msg2 = ReliableMessage(source_id="agent-2", sequence=1, qos=QoS.ORDERED)

session.receive(msg3)  # buffered (waiting for 0)
session.receive(msg1)  # delivers 0
result = session.receive(msg2)  # delivers 1 and drains buffered 2
assert len(result) == 2
```

## Architecture

```
a2a_r_protocol/
├── __init__.py       # Public API
├── message.py        # ReliableMessage, QoS, MessageStatus
├── ack.py            # AckManager with timeout + retransmission
├── order.py          # OrderManager with gap detection
├── session.py        # ReliableSession with flow control
└── protocol.py       # A2RProtocol multi-agent coordinator
```

## Development

```bash
pip install -e ".[dev]"
pytest tests/ -q
```

## License

MIT
