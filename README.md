# a2a-r-protocol

A2A-R: Agent-to-Agent protocol extensions for real-time robotics operations. Extends Google's A2A with QoS levels, sensor streaming, safety-critical coordination, and latency guarantees.

## What A2A-R Adds to A2A

| Feature | A2A Base | A2A-R Extension |
|---------|----------|-----------------|
| QoS Levels | None | Safety-critical, Realtime, Interactive, Background |
| Transport | HTTP/2 | + WebRTC DataChannels, UDP multicast, shared memory |
| Streaming | None | Sensor, control, video, audio, telemetry, lidar |
| Coordination | None | Multi-agent planning, formation, leader election |
| Safety | None | Safety state broadcast, veto system, constraint enforcement |
| Latency | N/A | Per-message TTL, jitter monitoring, deadline tracking |

## Usage

```python
from protocol import A2ARRouter, AgentCard, QoS, SafetyLevel, CoordinationState

# Create router for local agent
router = A2ARRouter("vessel-01")

# Handle incoming messages
router.on("sensor", lambda msg: process_sensor(msg))
router.on("safety", lambda msg: handle_safety(msg))

# Broadcast fleet state
msg = router.broadcast("telemetry", {"speed": 5.2, "heading": 270}, qos=QoS.REALTIME)

# Safety veto
router.set_safety(SafetyLevel.WARNING, conditions=["low_battery"], veto=True)

# Multi-agent coordination
coord = router.start_coordination("task-42", "leader-01", ["vessel-01", "vessel-02"])

# Agent capability card
card = AgentCard("vessel-01", "Fishing Vessel Alpha",
    vessel_type="marine",
    capabilities=["navigation", "fishing", "autonomy"],
    sensors=[{"type": "gps"}, {"type": "sonar"}],
    actuators=[{"type": "thruster"}, {"type": "rudder"}],
    transport=["http", "webrtc", "dds"],
    qos_supported=[0, 1, 2, 3])
```

Part of the [Lucineer ecosystem](https://github.com/Lucineer/the-fleet).
