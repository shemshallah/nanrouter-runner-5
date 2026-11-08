"""Soft 7 GiB memory budget accounting for one runner-ring process."""
from __future__ import annotations

import threading

GIB = 1024 ** 3
DEFAULT_BUDGET = 7 * GIB


class MemoryBudgetError(RuntimeError):
    pass


class MemoryBudget:
    def __init__(self, capacity_bytes: int = DEFAULT_BUDGET):
        if isinstance(capacity_bytes, bool) or not isinstance(capacity_bytes, int):
            raise MemoryBudgetError("capacity_bytes must be an integer")
        if capacity_bytes < 1:
            raise MemoryBudgetError("capacity_bytes must be positive")
        self.capacity_bytes = capacity_bytes
        self._used = 0
        self._labels: dict[str, int] = {}
        self._lock = threading.RLock()

    def reserve(self, byte_count: int, label: str = "default") -> None:
        if isinstance(byte_count, bool) or not isinstance(byte_count, int) or byte_count < 0:
            raise MemoryBudgetError("byte_count must be a non-negative integer")
        if not isinstance(label, str) or not label or len(label) > 64:
            raise MemoryBudgetError("label must be a bounded non-empty string")
        with self._lock:
            if self._used + byte_count > self.capacity_bytes:
                raise MemoryBudgetError("7 GiB runner memory budget would be exceeded")
            self._used += byte_count
            self._labels[label] = self._labels.get(label, 0) + byte_count

    def release(self, byte_count: int, label: str = "default") -> None:
        if isinstance(byte_count, bool) or not isinstance(byte_count, int) or byte_count < 0:
            raise MemoryBudgetError("byte_count must be a non-negative integer")
        with self._lock:
            current = self._labels.get(label, 0)
            if byte_count > current:
                raise MemoryBudgetError("release exceeds the reserved amount")
            self._used = max(0, self._used - byte_count)
            if current == byte_count:
                self._labels.pop(label, None)
            else:
                self._labels[label] = current - byte_count

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "capacity_bytes": self.capacity_bytes,
                "used_bytes": self._used,
                "free_bytes": self.capacity_bytes - self._used,
                "labels": dict(sorted(self._labels.items())),
            }
