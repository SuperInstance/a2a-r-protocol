"""Comprehensive tests for a2a_r_protocol."""

import time
import pytest

from a2a_r_protocol import (
    ReliableMessage,
    QoS,
    MessageStatus,
    AckManager,
    OrderManager,
    ReliableSession,
    A2RProtocol,
)


# ─── ReliableMessage ────────────────────────────────────────────

class TestReliableMessage:

    def test_auto_id_and_timestamp(self):
        msg = ReliableMessage()
        assert msg.message_id
        assert msg.timestamp > 0
        assert msg.status == MessageStatus.PENDING

    def test_requires_ack_by_qos(self):
        assert not ReliableMessage(qos=QoS.BEST_EFFORT).requires_ack
        assert ReliableMessage(qos=QoS.ACKNOWLEDGED).requires_ack
        assert ReliableMessage(qos=QoS.RELIABLE).requires_ack

    def test_requires_ordering(self):
        assert not ReliableMessage(qos=QoS.RELIABLE).requires_ordering
        assert ReliableMessage(qos=QoS.ORDERED).requires_ordering

    def test_expiry(self):
        msg = ReliableMessage(ttl_seconds=-1)
        assert msg.is_expired

    def test_not_expired(self):
        msg = ReliableMessage(ttl_seconds=60)
        assert not msg.is_expired

    def test_retries_remaining(self):
        msg = ReliableMessage(max_retries=3, retry_count=1)
        assert msg.retries_remaining == 2

    def test_mark_sent(self):
        msg = ReliableMessage()
        msg.mark_sent()
        assert msg.status == MessageStatus.SENT

    def test_mark_retry(self):
        msg = ReliableMessage()
        msg.mark_retry()
        assert msg.retry_count == 1
        assert msg.status == MessageStatus.RETRYING

    def test_mark_acked(self):
        msg = ReliableMessage()
        msg.mark_acked()
        assert msg.status == MessageStatus.ACKED

    def test_mark_delivered(self):
        msg = ReliableMessage()
        msg.mark_delivered()
        assert msg.status == MessageStatus.DELIVERED

    def test_serialization_roundtrip(self):
        msg = ReliableMessage(
            payload={"key": "value"},
            source_id="a",
            target_id="b",
            qos=QoS.RELIABLE,
            sequence=42,
        )
        d = msg.to_dict()
        restored = ReliableMessage.from_dict(d)
        assert restored.message_id == msg.message_id
        assert restored.sequence == 42
        assert restored.qos == QoS.RELIABLE
        assert restored.payload == {"key": "value"}

    def test_clone_for_retry(self):
        msg = ReliableMessage(payload={"x": 1}, source_id="a", qos=QoS.RELIABLE)
        clone = msg.clone_for_retry()
        assert clone.message_id != msg.message_id
        assert clone.payload == msg.payload
        assert clone.source_id == msg.source_id

    def test_repr(self):
        msg = ReliableMessage()
        r = repr(msg)
        assert "ReliableMessage" in r


# ─── AckManager ─────────────────────────────────────────────────

class TestAckManager:

    def test_track_and_acknowledge(self):
        mgr = AckManager()
        msg = ReliableMessage(qos=QoS.ACKNOWLEDGED)
        mgr.track(msg)
        assert mgr.pending_count == 1
        record = mgr.acknowledge(msg.message_id)
        assert record is not None
        assert record.is_acked
        assert mgr.pending_count == 0

    def test_best_effort_not_tracked(self):
        mgr = AckManager()
        msg = ReliableMessage(qos=QoS.BEST_EFFORT)
        mgr.track(msg)
        assert mgr.pending_count == 0

    def test_ack_nonexistent(self):
        mgr = AckManager()
        assert mgr.acknowledge("nonexistent") is None

    def test_is_acked(self):
        mgr = AckManager()
        msg = ReliableMessage(qos=QoS.ACKNOWLEDGED)
        mgr.track(msg)
        assert not mgr.is_acked(msg.message_id)
        mgr.acknowledge(msg.message_id)
        assert mgr.is_acked(msg.message_id)

    def test_timeout_detection(self):
        mgr = AckManager(default_timeout=0.01, max_retries=0)
        msg = ReliableMessage(qos=QoS.ACKNOWLEDGED)
        msg.timestamp = time.time() - 1  # in the past
        mgr.track(msg)
        time.sleep(0.02)
        timed_out = mgr.check_timeouts()
        assert len(timed_out) == 1
        assert timed_out[0][0] == msg.message_id
        assert mgr.pending_count == 0

    def test_retry_with_backoff(self):
        retries = []
        mgr = AckManager(default_timeout=0.01, max_retries=2, backoff_factor=1.0)
        mgr.on_retry(lambda mid, seq: retries.append(mid))

        msg = ReliableMessage(qos=QoS.RELIABLE)
        msg.timestamp = time.time() - 1
        mgr.track(msg)

        time.sleep(0.02)
        mgr.check_timeouts()
        assert len(retries) == 1

        time.sleep(0.02)
        mgr.check_timeouts()
        assert len(retries) == 2

        # Should now be fully timed out
        time.sleep(0.02)
        timed_out = mgr.check_timeouts()
        assert len(timed_out) == 1

    def test_negative_ack(self):
        mgr = AckManager()
        msg = ReliableMessage(qos=QoS.ACKNOWLEDGED)
        mgr.track(msg)
        record = mgr.negative_ack(msg.message_id, "busy")
        assert record is not None
        assert record.nack_reason == "busy"
        # Still pending
        assert mgr.pending_count == 1

    def test_get_pending(self):
        mgr = AckManager()
        msg = ReliableMessage(qos=QoS.ACKNOWLEDGED)
        mgr.track(msg)
        assert msg.message_id in mgr.get_pending_message_ids()

    def test_clear(self):
        mgr = AckManager()
        mgr.track(ReliableMessage(qos=QoS.ACKNOWLEDGED))
        mgr.clear()
        assert mgr.pending_count == 0


# ─── OrderManager ────────────────────────────────────────────────

class TestOrderManager:

    def test_sequential_delivery(self):
        om = OrderManager()
        delivered = []
        om.on_deliver(lambda m: delivered.append(m))

        msgs = [
            ReliableMessage(source_id="a", sequence=i, qos=QoS.ORDERED)
            for i in range(5)
        ]

        for msg in msgs:
            result = om.receive(msg)
            assert len(result) == 1
            assert result[0].sequence == msg.sequence

        assert len(delivered) == 5

    def test_out_of_order_buffering(self):
        om = OrderManager()
        gaps = []
        om.on_gap(lambda g: gaps.append(g))

        # Send seq 2 first
        result = om.receive(
            ReliableMessage(source_id="a", sequence=2, qos=QoS.ORDERED)
        )
        assert len(result) == 0  # buffered
        assert len(gaps) == 1
        assert gaps[0].expected == 0

        # Send seq 0
        result = om.receive(
            ReliableMessage(source_id="a", sequence=0, qos=QoS.ORDERED)
        )
        assert len(result) == 1
        assert result[0].sequence == 0

        # Send seq 1 — should deliver 1 and drain buffered 2
        result = om.receive(
            ReliableMessage(source_id="a", sequence=1, qos=QoS.ORDERED)
        )
        assert len(result) == 2
        assert [m.sequence for m in result] == [1, 2]

    def test_duplicate_detection(self):
        om = OrderManager()
        msg = ReliableMessage(source_id="a", sequence=0, qos=QoS.ORDERED)
        om.receive(msg)
        result = om.receive(msg)
        assert len(result) == 0

    def test_non_ordered_passthrough(self):
        om = OrderManager()
        msg = ReliableMessage(source_id="a", qos=QoS.BEST_EFFORT)
        result = om.receive(msg)
        assert len(result) == 1

    def test_next_sequence(self):
        om = OrderManager()
        assert om.next_sequence("a") == 0
        assert om.next_sequence("a") == 1
        assert om.next_sequence("b") == 0

    def test_gap_detection(self):
        om = OrderManager()
        om.receive(ReliableMessage(source_id="a", sequence=0, qos=QoS.ORDERED))
        om.receive(ReliableMessage(source_id="a", sequence=3, qos=QoS.ORDERED))
        gaps = om.get_gaps("a")
        assert gaps == [1, 2]

    def test_buffered_count(self):
        om = OrderManager()
        om.receive(ReliableMessage(source_id="a", sequence=5, qos=QoS.ORDERED))
        assert om.buffered_count("a") == 1
        assert om.buffered_count() == 1

    def test_reset(self):
        om = OrderManager()
        om.next_sequence("a")
        om.reset("a")
        assert om.current_expected("a") == 0

    def test_reset_all(self):
        om = OrderManager()
        om.next_sequence("a")
        om.next_sequence("b")
        om.reset()
        assert om.current_expected("a") == 0
        assert om.current_expected("b") == 0


# ─── ReliableSession ────────────────────────────────────────────

class TestReliableSession:

    def test_connect_disconnect(self):
        s = ReliableSession("agent-1")
        assert not s.is_connected
        s.connect("agent-2")
        assert s.is_connected
        assert s.remote_id == "agent-2"
        s.disconnect()
        assert not s.is_connected

    def test_cannot_connect_twice(self):
        s = ReliableSession("agent-1")
        s.connect("agent-2")
        with pytest.raises(RuntimeError):
            s.connect("agent-3")

    def test_send_requires_connection(self):
        s = ReliableSession("agent-1")
        with pytest.raises(RuntimeError):
            s.send(ReliableMessage())

    def test_send_assigns_fields(self):
        s = ReliableSession("agent-1")
        s.connect("agent-2")
        msg = ReliableMessage(payload={"hello": True})
        result = s.send(msg)
        assert result.source_id == "agent-1"
        assert result.target_id == "agent-2"
        assert result.status == MessageStatus.SENT

    def test_send_with_ordering(self):
        s = ReliableSession("agent-1", default_qos=QoS.ORDERED)
        s.connect("agent-2")
        msg1 = ReliableMessage(qos=QoS.ORDERED)
        msg2 = ReliableMessage(qos=QoS.ORDERED)
        s.send(msg1)
        s.send(msg2)
        assert msg1.sequence == 0
        assert msg2.sequence == 1

    def test_receive_dedup(self):
        s = ReliableSession("agent-1")
        s.connect("agent-2")
        msg = ReliableMessage(source_id="agent-2", payload={"x": 1})
        r1 = s.receive(msg)
        r2 = s.receive(msg)  # duplicate
        assert len(r1) == 1
        assert len(r2) == 0

    def test_ack_flow(self):
        s = ReliableSession("agent-1")
        s.connect("agent-2")
        msg = ReliableMessage(qos=QoS.ACKNOWLEDGED)
        s.send(msg)
        assert s.inflight_count == 1
        assert s.handle_ack(msg.message_id)
        assert s.inflight_count == 0
        stats = s.get_stats()
        assert stats["acked"] == 1

    def test_nack_flow(self):
        s = ReliableSession("agent-1")
        s.connect("agent-2")
        msg = ReliableMessage(qos=QoS.ACKNOWLEDGED)
        s.send(msg)
        s.handle_nack(msg.message_id, "busy")
        assert msg.status == MessageStatus.NACKED

    def test_suspend_resume(self):
        s = ReliableSession("agent-1")
        s.connect("agent-2")
        s.suspend()
        with pytest.raises(RuntimeError):
            s.send(ReliableMessage())
        s.resume()
        s.send(ReliableMessage())  # should work now

    def test_on_message_callback(self):
        s = ReliableSession("agent-1")
        s.connect("agent-2")
        received = []
        s.on_message(lambda m: received.append(m))
        s.receive(ReliableMessage(source_id="agent-2"))
        assert len(received) == 1

    def test_stats(self):
        s = ReliableSession("agent-1")
        s.connect("agent-2")
        msg = ReliableMessage(qos=QoS.ACKNOWLEDGED)
        s.send(msg)
        stats = s.get_stats()
        assert stats["sent"] == 1
        assert stats["inflight"] == 1

    def test_tick_drives_timeouts(self):
        s = ReliableSession("agent-1", ack_timeout=0.01, max_retries=0)
        s.connect("agent-2")
        msg = ReliableMessage(qos=QoS.ACKNOWLEDGED)
        msg.timestamp = time.time() - 1
        s.send(msg)
        time.sleep(0.02)
        s.tick()
        assert msg.status == MessageStatus.TIMED_OUT


# ─── A2RProtocol ────────────────────────────────────────────────

class TestA2RProtocol:

    def test_create_session(self):
        p = A2RProtocol("agent-1")
        session = p.create_session("agent-2")
        assert session.is_connected
        assert "agent-2" in p.active_sessions

    def test_duplicate_session(self):
        p = A2RProtocol("agent-1")
        p.create_session("agent-2")
        with pytest.raises(ValueError):
            p.create_session("agent-2")

    def test_send_receive(self):
        p = A2RProtocol("agent-1")
        p.create_session("agent-2")
        received = []
        p.on("task", lambda m: received.append(m))

        msg = p.send("agent-2", {"action": "greet"})
        assert msg is not None

        # Simulate receive on the other end
        session = p.get_session("agent-2")
        incoming = ReliableMessage(
            source_id="agent-2",
            payload={"type": "task", "action": "reply"},
        )
        session.receive(incoming)
        assert len(received) == 1
        assert received[0].payload["action"] == "reply"

    def test_send_to_unknown(self):
        p = A2RProtocol("agent-1")
        result = p.send("unknown", {"x": 1})
        assert result is None

    def test_close_session(self):
        p = A2RProtocol("agent-1")
        p.create_session("agent-2")
        p.close_session("agent-2")
        assert "agent-2" not in p.active_sessions

    def test_close_all(self):
        p = A2RProtocol("agent-1")
        p.create_session("a")
        p.create_session("b")
        p.close_all()
        assert len(p.active_sessions) == 0

    def test_tick(self):
        p = A2RProtocol("agent-1")
        p.create_session("agent-2")
        p.tick()  # should not raise

    def test_stats(self):
        p = A2RProtocol("agent-1")
        p.create_session("agent-2")
        stats = p.get_stats()
        assert stats["sessions"] == 1
        assert "agent-2" in stats["per_session"]
