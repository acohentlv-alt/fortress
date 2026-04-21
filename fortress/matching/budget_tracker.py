"""Per-batch budget tracker for Gemini shadow judge.

One instance per batch. Thread/task-safe via asyncio.Lock.
"""

from __future__ import annotations

import asyncio


class BudgetTracker:
    """Track USD spend for one batch's Gemini calls with a hard cap."""

    def __init__(self, cap_usd: float):
        self._cap = cap_usd
        self._spent = 0.0
        self._calls = 0
        self._hit_cap = False
        self._lock = asyncio.Lock()

    @property
    def spent(self) -> float:
        return self._spent

    @property
    def calls(self) -> int:
        return self._calls

    @property
    def hit_cap(self) -> bool:
        return self._hit_cap

    async def would_exceed(self, cost_usd: float) -> bool:
        """Return True if spending `cost_usd` more would exceed the cap."""
        async with self._lock:
            if self._spent + cost_usd > self._cap:
                self._hit_cap = True
                return True
            return False

    async def spend(self, cost_usd: float) -> None:
        """Record spend. Caller is expected to have checked `would_exceed` first."""
        async with self._lock:
            self._spent += cost_usd
            self._calls += 1
