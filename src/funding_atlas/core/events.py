"""The event log schema.

Two data paths, deliberately separated by rate:

  * Market data (trades, book snapshots, mids) is high rate and goes straight
    to Parquet with no per-record validation. See `recorder.writers`.

  * Everything that explains a decision is low rate and lands here: signals,
    risk verdicts, order intents, acknowledgements, fills, funding payments,
    data gaps, errors. These are validated, append-only and never mutated.

The second path is the audit trail. "Why did the bot do that?" is answered by
replaying it, which is only possible if nothing is ever edited in place.

Two fields that exist from the first commit because retrofitting them is
expensive:

  `account_id`  — multi-tenancy. Even with one account today, every event is
                  scoped, so serving other people later does not mean rewriting
                  the core.
  `ts_ingest_ns` — our clock alongside the venue's `ts_event_ns`. The gap
                  between them is observable latency, and it is also how a
                  replay proves it never read the future.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class EventType(StrEnum):
    # data plane
    DATA_GAP = "data_gap"
    STREAM_UP = "stream_up"
    STREAM_DOWN = "stream_down"
    # decision plane
    SIGNAL = "signal"
    RISK_VERDICT = "risk_verdict"
    ORDER_INTENT = "order_intent"
    ORDER_ACK = "order_ack"
    ORDER_REJECT = "order_reject"
    ORDER_CANCEL = "order_cancel"
    FILL = "fill"
    # accounting plane
    FUNDING_PAYMENT = "funding_payment"
    POSITION_SNAPSHOT = "position_snapshot"
    RECONCILE_MISMATCH = "reconcile_mismatch"
    # control plane
    KILL_SWITCH = "kill_switch"
    ERROR = "error"


class Event(BaseModel):
    """Base event. Immutable by construction."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    seq: int = Field(description="Monotonic per-account sequence number")
    type: EventType
    account_id: str
    venue: str
    ts_event_ns: int = Field(description="Venue-stamped time, ns since epoch")
    ts_ingest_ns: int = Field(description="Our clock when we saw it, ns since epoch")
    payload: dict[str, Any] = Field(default_factory=dict)

    @property
    def latency_ns(self) -> int:
        return self.ts_ingest_ns - self.ts_event_ns


class DataGap(BaseModel):
    """A hole in the record.

    Book data cannot be backfilled, so a gap is permanent and must be visible
    to research rather than buried in a log file. Any backtest crossing a gap
    for an asset it trades is invalid, and the engine is expected to refuse it.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    stream: str
    asset: str | None = None
    from_ns: int
    to_ns: int
    reason: Literal["disconnect", "silence", "shutdown", "error", "backpressure"]
    detail: str = ""

    @property
    def duration_s(self) -> float:
        return (self.to_ns - self.from_ns) / 1e9
