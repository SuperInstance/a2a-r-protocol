"""A2A-R Protocol — Agent-to-Agent for Robotics.

Extensions to Google's A2A protocol for real-time robotics operations.
Adds QoS levels, WebRTC sensor streaming, safety-critical coordination,
and latency guarantees.

Base A2A objects: AgentCard, Task, Message, Artifact
A2A-R additions:  Stream, Coordination, Heartbeat, SafetyState
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Callable
from enum import Enum, IntEnum
import time
import json
import hashlib
import uuid


# ─── QoS Levels ─────────────────────────────────────────────────

class QoS(IntEnum):
    SAFETY_CRITICAL = 0   # Must deliver, ordered, <1ms jitter
    REALTIME = 1          # Deadline-aware, <10ms
    INTERACTIVE = 2       # Human-noticeable, <100ms
    BACKGROUND = 3        # Bulk, no deadline


class SafetyLevel(IntEnum):
    NOMINAL = 0
    CAUTION = 1
    WARNING = 2
    CRITICAL = 3
    EMERGENCY = 4


class StreamType(Enum):
    SENSOR = "sensor"
    CONTROL = "control"
    VIDEO = "video"
    AUDIO = "audio"
    TELEMETRY = "telemetry"
    LIDAR = "lidar"


# ─── Core Protocol Objects ──────────────────────────────────────

@dataclass
class AgentCard:
    """A2A Agent Card — extended for robotics."""
    agent_id: str
    name: str
    description: str
    vessel_type: str = "generic"  # marine, aerial, industrial, home, medical
    capabilities: List[str] = field(default_factory=list)
    sensors: List[Dict[str, Any]] = field(default_factory=list)
    actuators: List[Dict[str, Any]] = field(default_factory=list)
    transport: List[str] = field(default_factory=list)  # http, webrtc, dds, mqtt
    qos_supported: List[int] = field(default_factory=lambda: [1, 2, 3])
    safety_level: SafetyLevel = SafetyLevel.NOMINAL
    max_latency_ms: int = 100
    endpoint: str = ""

    def to_dict(self) -> dict:
        return {
            "agent_id": self.agent_id,
            "name": self.name,
            "description": self.description,
            "vessel_type": self.vessel_type,
            "capabilities": self.capabilities,
            "sensors": self.sensors,
            "actuators": self.actuators,
            "transport": self.transport,
            "qos": self.qos_supported,
            "safety": self.safety_level.value,
            "latency_ms": self.max_latency_ms,
            "endpoint": self.endpoint,
        }


@dataclass
class A2RMessage:
    """A2A-R message with QoS and latency tracking."""
    message_id: str = ""
    source_id: str = ""
    target_id: str = ""
    task_id: str = ""
    msg_type: str = "task"  # task, stream, coord, heartbeat, safety
    qos: QoS = QoS.INTERACTIVE
    timestamp: float = 0.0
    content: Dict[str, Any] = field(default_factory=dict)
    ttl_ms: int = 5000  # Time to live
    priority: int = 5  # 0=highest, 9=lowest
    requires_ack: bool = False

    def __post_init__(self):
        if not self.message_id:
            self.message_id = uuid.uuid4().hex[:16]
        if not self.timestamp:
            self.timestamp = time.time()

    def to_json(self) -> str:
        return json.dumps({
            "mid": self.message_id,
            "src": self.source_id,
            "tgt": self.target_id,
            "tid": self.task_id,
            "type": self.msg_type,
            "qos": int(self.qos),
            "ts": self.timestamp,
            "content": self.content,
            "ttl": self.ttl_ms,
            "pri": self.priority,
            "ack": self.requires_ack,
        })

    @classmethod
    def from_json(cls, data: str) -> "A2RMessage":
        d = json.loads(data)
        return cls(
            message_id=d.get("mid", ""),
            source_id=d.get("src", ""),
            target_id=d.get("tgt", ""),
            task_id=d.get("tid", ""),
            msg_type=d.get("type", "task"),
            qos=QoS(d.get("qos", 2)),
            timestamp=d.get("ts", time.time()),
            content=d.get("content", {}),
            ttl_ms=d.get("ttl", 5000),
            priority=d.get("pri", 5),
            requires_ack=d.get("ack", False),
        )


@dataclass
class SensorStream:
    """Real-time sensor data stream."""
    stream_id: str = ""
    agent_id: str = ""
    stream_type: StreamType = StreamType.SENSOR
    sensor_id: str = ""
    frequency_hz: float = 10.0
    resolution: Dict[str, Any] = field(default_factory=dict)
    transport: str = "webrtc"  # webrtc, udp, shared_memory
    qos: QoS = QoS.REALTIME
    active: bool = False

    def __post_init__(self):
        if not self.stream_id:
            self.stream_id = uuid.uuid4().hex[:12]


@dataclass
class CoordinationState:
    """Multi-agent coordination state."""
    coordination_id: str = ""
    task_id: str = ""
    leader_id: str = ""
    participants: List[str] = field(default_factory=list)
    phase: str = "idle"  # idle, planning, executing, monitoring, complete
    formation: Dict[str, Any] = field(default_factory=dict)
    safety_veto: bool = False
    timestamp: float = 0.0

    def __post_init__(self):
        if not self.coordination_id:
            self.coordination_id = uuid.uuid4().hex[:12]
        if not self.timestamp:
            self.timestamp = time.time()


@dataclass
class SafetyState:
    """Fleet-wide safety state broadcast."""
    agent_id: str = ""
    level: SafetyLevel = SafetyLevel.NOMINAL
    conditions: List[str] = field(default_factory=list)
    active_constraints: List[Dict[str, Any]] = field(default_factory=list)
    veto_active: bool = False
    veto_reason: str = ""
    timestamp: float = 0.0

    def to_dict(self) -> dict:
        return {
            "agent_id": self.agent_id,
            "level": self.level.value,
            "level_name": self.level.name,
            "conditions": self.conditions,
            "constraints": self.active_constraints,
            "veto": self.veto_active,
            "veto_reason": self.veto_reason,
            "timestamp": self.timestamp,
        }


# ─── Message Router ─────────────────────────────────────────────

class A2ARRouter:
    """Routes A2A-R messages based on QoS and priority."""

    def __init__(self, local_agent_id: str):
        self.agent_id = local_agent_id
        self._handlers: Dict[str, List[Callable]] = {}
        self._message_log: List[A2RMessage] = []
        self._safety_state = SafetyState(agent_id=local_agent_id)
        self._coordination_states: Dict[str, CoordinationState] = {}

    def on(self, msg_type: str, handler: Callable):
        if msg_type not in self._handlers:
            self._handlers[msg_type] = []
        self._handlers[msg_type].append(handler)

    def route(self, message: A2RMessage) -> bool:
        if message.target_id != self.agent_id and message.target_id != "broadcast":
            return False

        self._message_log.append(message)
        if len(self._message_log) > 1000:
            self._message_log = self._message_log[-500:]

        for handler in self._handlers.get(message.msg_type, []):
            try:
                handler(message)
            except Exception:
                pass
        return True

    def broadcast(self, msg_type: str, content: Dict[str, Any],
                  qos: QoS = QoS.BEST_EFFORT, priority: int = 5):
        msg = A2RMessage(
            source_id=self.agent_id,
            target_id="broadcast",
            msg_type=msg_type,
            content=content,
            qos=qos,
            priority=priority,
        )
        return msg

    def set_safety(self, level: SafetyLevel, conditions: List[str] = None,
                    veto: bool = False, reason: str = ""):
        self._safety_state = SafetyState(
            agent_id=self.agent_id,
            level=level,
            conditions=conditions or [],
            veto_active=veto,
            veto_reason=reason,
            timestamp=time.time(),
        )

    def get_safety_state(self) -> SafetyState:
        return self._safety_state

    def start_coordination(self, task_id: str, leader_id: str,
                           participants: List[str]) -> CoordinationState:
        state = CoordinationState(
            task_id=task_id,
            leader_id=leader_id,
            participants=participants,
            phase="planning",
        )
        self._coordination_states[state.coordination_id] = state
        return state

    def check_message_ttl(self, message: A2RMessage) -> bool:
        age_ms = (time.time() - message.timestamp) * 1000
        return age_ms < message.ttl_ms

    def get_stats(self) -> Dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "messages_received": len(self._message_log),
            "handlers": {k: len(v) for k, v in self._handlers.items()},
            "safety_level": self._safety_state.level.name,
            "active_coordinations": len(self._coordination_states),
        }
