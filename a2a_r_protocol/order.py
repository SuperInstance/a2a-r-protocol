"""Ordering manager for in-order delivery and gap detection."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from .message import ReliableMessage


@dataclass
class SequenceGap:
    """Represents a gap in the received sequence."""
    session_id: str
    expected: int
    missing_up_to: int  # inclusive upper bound of missing range
    detected_at: float
    source_id: str = ""

    @property
    def count(self) -> int:
        return self.missing_up_to - self.expected + 1


class OrderManager:
    """Ensures in-order delivery of messages within a session.

    Tracks sequence numbers per (session, source) pair, buffers
    out-of-order messages, detects gaps, and delivers messages
    only when all prior sequences have been received.

    Args:
        max_buffer_size: Maximum buffered messages per source before
            triggering a gap flush (prevents unbounded memory growth).
        gap_timeout_seconds: Seconds to wait before declaring a gap
            permanent and skipping missing messages.
    """

    def __init__(
        self,
        max_buffer_size: int = 1000,
        gap_timeout_seconds: float = 10.0,
    ):
        self.max_buffer_size = max_buffer_size
        self.gap_timeout_seconds = gap_timeout_seconds

        # (source_id,) -> next expected sequence number
        self._next_expected: Dict[str, int] = {}
        # (source_id,) -> {seq: message}
        self._buffer: Dict[str, Dict[int, ReliableMessage]] = defaultdict(dict)
        # (source_id,) -> set of delivered sequences (for dedup)
        self._delivered: Dict[str, Set[int]] = defaultdict(set)

        # Callbacks
        self._on_gap: Optional[Callable[[SequenceGap], None]] = None
        self._on_deliver: Optional[Callable[[ReliableMessage], None]] = None

    # ── configuration ────────────────────────────────────────────

    def on_gap(self, callback: Callable[[SequenceGap], None]) -> None:
        """Register callback when a sequence gap is detected."""
        self._on_gap = callback

    def on_deliver(self, callback: Callable[[ReliableMessage], None]) -> None:
        """Register callback when an in-order message is delivered."""
        self._on_deliver = callback

    # ── sequence tracking ────────────────────────────────────────

    def next_sequence(self, source_id: str) -> int:
        """Get the next sequence number to assign for a source.

        Returns the next sequence number and advances the counter.
        """
        key = source_id
        if key not in self._next_expected:
            self._next_expected[key] = 0
        seq = self._next_expected[key]
        self._next_expected[key] = seq + 1
        return seq

    def current_expected(self, source_id: str) -> int:
        """Get the current expected sequence number without advancing it."""
        return self._next_expected.get(source_id, 0)

    def set_expected(self, source_id: str, seq: int) -> None:
        """Force-set the next expected sequence (e.g. after recovery)."""
        self._next_expected[source_id] = seq

    # ── receive / ordering ───────────────────────────────────────

    def receive(self, message: ReliableMessage) -> List[ReliableMessage]:
        """Process an incoming ordered message.

        Returns a list of messages now ready for in-order delivery
        (may be empty if the message is buffered or a duplicate).
        """
        if not message.requires_ordering:
            # Not ordered — deliver immediately
            return [message]

        source = message.source_id
        seq = message.sequence
        key = source

        # Duplicate check
        if seq in self._delivered[key]:
            return []

        expected = self._next_expected.get(key, 0)

        # Case 1: exact expected sequence — deliver it and drain buffer
        if seq == expected:
            delivered = self._deliver_sequence(key, message)
            return delivered

        # Case 2: future sequence — buffer it, report gap
        if seq > expected:
            self._buffer[key][seq] = message
            gap = SequenceGap(
                session_id=key,
                expected=expected,
                missing_up_to=seq - 1,
                detected_at=__import__("time").time(),
                source_id=source,
            )
            if self._on_gap:
                self._on_gap(gap)
            # Safety: flush if buffer is too large
            if len(self._buffer[key]) >= self.max_buffer_size:
                return self._flush_gap(key)
            return []

        # Case 3: old sequence (seq < expected) — already delivered or gap resolved
        # Deliver it only if we haven't seen it
        if seq not in self._delivered[key]:
            self._delivered[key].add(seq)
            return [message]
        return []

    def _deliver_sequence(self, key: str, message: ReliableMessage) -> List[ReliableMessage]:
        """Deliver a message and drain any buffered continuations."""
        delivered: List[ReliableMessage] = []
        expected = message.sequence
        self._delivered[key].add(expected)
        self._next_expected[key] = expected + 1
        delivered.append(message)

        if self._on_deliver:
            self._on_deliver(message)

        # Drain buffered messages
        while self._next_expected[key] in self._buffer[key]:
            seq = self._next_expected[key]
            msg = self._buffer[key].pop(seq)
            self._delivered[key].add(seq)
            self._next_expected[key] = seq + 1
            delivered.append(msg)
            if self._on_deliver:
                self._on_deliver(msg)

        return delivered

    def _flush_gap(self, key: str) -> List[ReliableMessage]:
        """Force-skip a gap and deliver buffered messages."""
        delivered: List[ReliableMessage] = []
        expected = self._next_expected.get(key, 0)

        # Find lowest buffered sequence
        if not self._buffer[key]:
            return delivered

        lowest_buffered = min(self._buffer[key].keys())

        # Mark gap sequences as "delivered" (skipped)
        for seq in range(expected, lowest_buffered):
            self._delivered[key].add(seq)

        self._next_expected[key] = lowest_buffered

        # Now deliver from lowest buffered onward
        while self._next_expected[key] in self._buffer[key]:
            seq = self._next_expected[key]
            msg = self._buffer[key].pop(seq)
            self._delivered[key].add(seq)
            self._next_expected[key] = seq + 1
            delivered.append(msg)
            if self._on_deliver:
                self._on_deliver(msg)

        return delivered

    # ── query ────────────────────────────────────────────────────

    def buffered_count(self, source_id: str = "") -> int:
        if source_id:
            return len(self._buffer.get(source_id, {}))
        return sum(len(buf) for buf in self._buffer.values())

    def get_gaps(self, source_id: str) -> List[int]:
        """Return currently missing sequence numbers for a source."""
        expected = self._next_expected.get(source_id, 0)
        buffered_seqs = set(self._buffer.get(source_id, {}).keys())
        delivered_seqs = self._delivered.get(source_id, set())
        max_seq = max(buffered_seqs) if buffered_seqs else expected - 1

        gaps = []
        for seq in range(expected, max_seq + 1):
            if seq not in buffered_seqs and seq not in delivered_seqs:
                gaps.append(seq)
        return gaps

    def reset(self, source_id: str = "") -> None:
        """Reset ordering state for a source (or all sources)."""
        if source_id:
            self._next_expected.pop(source_id, None)
            self._buffer.pop(source_id, None)
            self._delivered.pop(source_id, None)
        else:
            self._next_expected.clear()
            self._buffer.clear()
            self._delivered.clear()
