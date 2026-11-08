"""NaN routing fabric owned by the one active runner-ring slot.

The six runner repositories do not receive per-runner HW addresses.  The ring
as a whole is ``HW5:0``.  Only the slot holding the atomic Koyeb lease runs the
fabric; the other five are hot standbys.  HW0:0 is the network port and HW2:0
is the HERMES endpoint.
"""
from __future__ import annotations

import threading
from typing import Any

from vendor.point_fabric import (
    CentralPointFabric,
    FabricError,
    FabricRoutingError,
    PointFrame,
    frame_from_json,
    frames_to_json,
)

RING_HW = 5
NETWORK_HW = 0
HERMES_HW = 2
RING_ENDPOINT = "HW5:0"
RING_ADDRESS = "nan://GM0:0::SC0:0::MC0:0::BS0:0::HW5:0"
ACTIVE_POINTS = (NETWORK_HW, HERMES_HW, RING_HW)


class SlotFabricError(RuntimeError):
    pass


class SlotFabric:
    def __init__(self, *, database: str | None = None):
        self.fabric = CentralPointFabric(database=database)
        self._active = False
        self._lock = threading.RLock()

    @property
    def active(self) -> bool:
        return self._active

    def activate(self) -> None:
        with self._lock:
            for point in ACTIVE_POINTS:
                try:
                    self.fabric.point(point)
                except FabricRoutingError:
                    self.fabric.register(point, status="active")
                else:
                    self.fabric.heartbeat(point, status="active")
            self._active = True

    def deactivate(self) -> None:
        with self._lock:
            self._active = False
            try:
                self.fabric.heartbeat(RING_HW, status="degraded")
            except FabricError:
                pass

    def require_active(self) -> None:
        if not self._active:
            raise SlotFabricError("this runner slot is not the active ring owner")

    def register_point(self, hw: int, *, role: str | None = None,
                       status: str = "active") -> dict[str, Any]:
        self.require_active()
        return self.fabric.register(hw, role=role, status=status)

    def point_monitor(self, hw: int) -> dict[str, Any]:
        return self.fabric.point_monitor(hw)

    def submit_json(self, value: Any) -> dict[str, Any]:
        self.require_active()
        frame = frame_from_json(value)
        return self.fabric.intake(frame)

    def submit(self, frame: PointFrame) -> dict[str, Any]:
        self.require_active()
        return self.fabric.intake(frame)

    def drain(self, hw: int = RING_HW, limit: int = 64) -> list[dict[str, Any]]:
        self.require_active()
        return frames_to_json(self.fabric.drain(hw, limit))

    def export_state(self) -> dict[str, Any]:
        self.require_active()
        return self.fabric.export_state(max_frames=1024)

    def import_state(self, state: Any) -> dict[str, Any]:
        return self.fabric.import_state(state)

    def monitor(self) -> dict[str, Any]:
        return {
            "schema": "nanverse.runner-ring-fabric.v1",
            "active": self._active,
            "ring_hw": RING_HW,
            "ring_endpoint": RING_ENDPOINT,
            "ring_address": RING_ADDRESS,
            "active_points": list(ACTIVE_POINTS),
            "fabric": self.fabric.monitor(),
        }

    def close(self) -> None:
        with self._lock:
            self._active = False
            self.fabric.close()
