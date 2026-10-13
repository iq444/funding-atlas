"""Time.

Every component reads time from a Clock, never from `time.time()` directly.
That single rule is what makes a backtest and a live run share one code path:
swapping the clock swaps the mode.

It is also the look-ahead guard. In simulation the clock only advances when the
data feed advances it, so a strategy physically cannot read a price stamped
later than the moment it is reasoning about.
"""

from __future__ import annotations

import asyncio
import time
from typing import Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    def now_ns(self) -> int:
        """Current time, nanoseconds since the Unix epoch."""
        ...

    async def sleep(self, seconds: float) -> None:
        """Wait. In simulation this returns immediately and moves the clock."""
        ...


class LiveClock:
    """Wall clock. Used by the recorder and by live trading."""

    __slots__ = ()

    def now_ns(self) -> int:
        return time.time_ns()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


class SimClock:
    """Simulated clock, driven by the replayed data feed.

    `set_now_ns` is called by the feed as it emits each event, so time moves in
    data order rather than in real time. A backtest therefore runs as fast as
    the disk allows while remaining causally honest.
    """

    __slots__ = ("_now_ns",)

    def __init__(self, start_ns: int = 0) -> None:
        self._now_ns = start_ns

    def now_ns(self) -> int:
        return self._now_ns

    def set_now_ns(self, ns: int) -> None:
        if ns < self._now_ns:
            raise ValueError(
                f"clock moved backwards: {self._now_ns} -> {ns}. "
                "Events must be replayed in non-decreasing time order."
            )
        self._now_ns = ns

    async def sleep(self, seconds: float) -> None:
        self._now_ns += int(seconds * 1_000_000_000)
