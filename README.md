# a2a-r-protocol

**A2A-R: Reliable Agent-to-Agent protocol** — extends Google's A2A with acknowledgments, retries, ordering guarantees, and flow control.

## What This Gives You

| Feature | A2A Base | A2A-R |
|---------|----------|-------|
| Delivery | Fire-and-forget | ACK/NACK with retry tracking |
| Retries | None | Configurable with exponential backoff |
| Ordering | None | Sequence numbers with gap detection |
| Flow Control | None | Sliding window + rate limiting |
| QoS Levels | None | Best-effort, Acknowledged, Reliable, Ordered |

## Installation

```bash
pip install a2a-r-protocol
```

## Quick Start

```python
from a2a_r_protocol import ReliableSession, ReliableMessage, QoS

session = ReliableSession("agent-1")
session.connect("agent-2")

msg = ReliableMessage(payload={"action": "navigate", "lat": 61.2, "lon": -149.9})
sent = session.send(msg, qos=QoS.RELIABLE)

# On receiver
session.handle_ack(sent.message_id)
```

## QoS Levels

- **Best-effort** — fire and forget
- **Acknowledged** — ACK required, no retry
- **Reliable** — ACK with configurable retries and backoff
- **Ordered** — reliable + sequence-based in-order delivery

## Testing

```bash
pip install -e .
pytest
```

## How It Fits

Reliability layer on top of `a2a-protocol`. Used when agent messages require delivery guarantees (navigation commands, financial transactions, critical coordination).

## License

MIT
