import copy
import json
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from memory_budget import GIB, MemoryBudget, MemoryBudgetError
from ring_runner import (
    RingConfig,
    RingConfigError,
    RunnerRingCoordinator,
    make_handler,
)
from slot_fabric import RING_HW, SlotFabric
from vendor.point_fabric import Nan16DEWord, PointFrame

ACL = "a" * 64
TOKEN = "r" * 40


class FakeAuthority:
    def __init__(self):
        self.tick = 0
        self.lease_row = None
        self.last_epoch = 0
        self.checkpoint_row = None
        self.handoffs = []

    def clock(self):
        self.tick += 1
        return {"nanclock_tick": self.tick}

    def lease(self, *, slot, base_epoch, endpoint, ttl_seconds=30):
        if self.lease_row is not None and self.lease_row["slot"] != slot:
            return {"acquired": False, "active_slot": self.lease_row["slot"],
                    "epoch": self.lease_row["epoch"], "nanclock_tick": self.tick}
        if self.lease_row is not None and self.lease_row["slot"] == slot:
            return {"acquired": True, "renewed": True, **self.lease_row,
                    "nanclock_tick": self.tick}
        old_epoch = max(
            self.lease_row["epoch"] if self.lease_row else 0, self.last_epoch
        )
        epoch = max(old_epoch + 1, base_epoch + 1, 1)
        self.last_epoch = epoch
        self.lease_row = {
            "slot": slot, "epoch": epoch, "lease_id": f"{slot:032x}",
            "endpoint": endpoint, "expires_at": 10 ** 12, "active_slot": slot,
            "active": True,
        }
        self.handoffs.append((slot, epoch))
        return {"acquired": True, "renewed": False, **self.lease_row,
                "nanclock_tick": self.tick}

    def renew(self, *, slot, lease_id, ttl_seconds=30):
        if self.lease_row is None or self.lease_row["slot"] != slot:
            raise RuntimeError("lease mismatch")
        return {**self.lease_row, "nanclock_tick": self.tick}

    def release(self, *, slot, lease_id):
        if self.lease_row is None or self.lease_row["slot"] != slot:
            raise RuntimeError("lease mismatch")
        self.last_epoch = self.lease_row["epoch"]
        self.lease_row = None
        return {"released": True}

    def checkpoint(self, *, slot, lease_id, state, nanclock_tick):
        self.checkpoint_row = {
            "slot": slot, "state": copy.deepcopy(state),
            "nanclock_tick": nanclock_tick, "state_digest": "d" * 64,
        }
        return {"checkpointed": True, "state_digest": "d" * 64}

    def read_checkpoint(self):
        return self.checkpoint_row


def frame(source=0, destination=RING_HW, tick=1):
    return PointFrame(
        request_id=1, source_hw=source, destination_hw=destination,
        nanclock_tick=tick, kind="nannet.route", acl_digest=ACL,
        word=Nan16DEWord.make(0x7FF1, 0xFFF8, 0x12345678),
    )


class ConfigTests(unittest.TestCase):
    def test_slot_file_and_https_rules(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "slot").write_text("2\n")
            config = RingConfig(
                slot=2, portal_url="https://portal.example",
                ring_token="t" * 40, advertise_url="https://runner-2.example",
                state_dir=root / "state",
            )
            self.assertEqual(config.slot, 2)
            self.assertEqual(config.memory_budget_bytes, 7 * GIB)
            with self.assertRaises(RingConfigError):
                RingConfig(
                    slot=2, portal_url="http://remote.example",
                    ring_token="t" * 40, advertise_url="https://runner-2.example",
                    state_dir=root / "state",
                )

    def test_no_hw_identity_is_required(self):
        config = RingConfig(
            slot=0, portal_url="https://portal.example", ring_token="t" * 40,
            advertise_url="https://runner-0.example", state_dir="/tmp/ring",
        )
        self.assertFalse(hasattr(config, "hw"))


class MemoryBudgetTests(unittest.TestCase):
    def test_seven_gib_cap(self):
        budget = MemoryBudget(7 * GIB)
        budget.reserve(GIB, "fabric")
        self.assertEqual(budget.stats()["used_bytes"], GIB)
        with self.assertRaises(MemoryBudgetError):
            budget.reserve(7 * GIB, "too-much")
        budget.release(GIB, "fabric")
        self.assertEqual(budget.stats()["used_bytes"], 0)


class CoordinatorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.authority = FakeAuthority()
        self.config = RingConfig(
            slot=0, portal_url="https://portal.example", ring_token="t" * 40,
            advertise_url="https://runner-0.example",
            state_dir=Path(self.tmp.name) / "state", poll_seconds=1,
        )

    def make(self, slot=0):
        config = RingConfig(
            slot=slot, portal_url="https://portal.example", ring_token="t" * 40,
            advertise_url=f"https://runner-{slot}.example",
            state_dir=Path(self.tmp.name) / f"state-{slot}",
            poll_seconds=1,
        )
        fabric = SlotFabric(database=str(Path(self.tmp.name) / f"fabric-{slot}.sqlite"))
        coordinator = RunnerRingCoordinator(
            config, self.authority, fabric, MemoryBudget(7 * GIB)
        )
        return coordinator, fabric

    def test_lease_activates_ring_and_routes_nan(self):
        coordinator, fabric = self.make(0)
        health = coordinator.tick()
        self.assertTrue(health["active"])
        self.assertEqual(health["phase"], "active")
        self.assertEqual(health["ring_hw"], RING_HW)
        self.assertTrue(fabric.active)
        receipt = fabric.submit(frame(source=0, destination=RING_HW))
        self.assertEqual(receipt["central_status"], "delivered")
        drained = fabric.drain(RING_HW, 1)
        self.assertEqual(drained[0]["word"]["raw_hex"], "7ff178563412f8ff")

    def test_standby_does_not_activate(self):
        first, first_fabric = self.make(0)
        first.tick()
        second, second_fabric = self.make(1)
        health = second.tick()
        self.assertFalse(health["active"])
        self.assertEqual(health["phase"], "standby")
        self.assertFalse(second_fabric.active)

    def test_handoff_preserves_ring_inbox(self):
        first, first_fabric = self.make(0)
        first.tick()
        first_fabric.submit(frame(source=0, destination=RING_HW))
        first.handoff()
        self.assertFalse(first_fabric.active)

        second, second_fabric = self.make(1)
        health = second.tick()
        self.assertTrue(health["active"])
        self.assertEqual(health["epoch"], 2)
        drained = second_fabric.drain(RING_HW, 1)
        self.assertEqual(len(drained), 1)
        self.assertEqual(drained[0]["destination_hw"], RING_HW)

    def test_checkpoint_is_published(self):
        coordinator, fabric = self.make(0)
        coordinator.tick()
        result = coordinator.checkpoint()
        self.assertEqual(result["phase"], "active")
        self.assertIsNotNone(self.authority.checkpoint_row)
        self.assertEqual(self.authority.checkpoint_row["slot"], 0)


class HealthServerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        config = RingConfig(
            slot=0, portal_url="https://portal.example", ring_token=TOKEN,
            advertise_url="https://runner-0.example",
            state_dir=Path(self.tmp.name) / "state",
        )
        self.fabric = SlotFabric(database=str(Path(self.tmp.name) / "fabric.sqlite"))
        self.coordinator = RunnerRingCoordinator(
            config, FakeAuthority(), self.fabric, MemoryBudget(7 * GIB)
        )
        self.coordinator.tick()
        self.server = ThreadingHTTPServer(
            ("127.0.0.1", 0), make_handler(self.coordinator, TOKEN)
        )
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.thread.join)
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def _post(self, path, value, token=TOKEN):
        request = Request(
            self.base + path, data=json.dumps(value).encode(), method="POST",
            headers={"Content-Type": "application/json",
                     "Authorization": "Bearer " + token},
        )
        with urlopen(request, timeout=3) as response:
            return response.status, json.loads(response.read())

    def test_healthz_and_submit_drain(self):
        with urlopen(self.base + "/healthz", timeout=3) as response:
            health = json.loads(response.read())
        self.assertEqual(health["schema"], "nanverse.runner-ring-health.v1")
        self.assertTrue(health["active"])
        self.assertEqual(health["memory"]["capacity_bytes"], 7 * GIB)
        status, receipt = self._post("/v1/ring/submit", {
            "frame": {
                "request_id": 2, "source_hw": 0, "destination_hw": RING_HW,
                "nanclock_tick": 1, "kind": "nannet.route",
                "acl_digest": ACL, "word_hex": "7ff112345678f8ff",
            }
        })
        self.assertEqual(status, 200)
        self.assertEqual(receipt["central_status"], "delivered")
        status, drained = self._post("/v1/ring/drain", {"hw": RING_HW, "limit": 2})
        self.assertEqual(status, 200)
        self.assertEqual(drained["count"], 1)

    def test_register_extra_zero_point_and_route_to_it(self):
        status, registered = self._post("/v1/ring/register", {
            "hw": 9, "role": "general-hardware-node", "status": "active",
        })
        self.assertEqual(status, 200)
        self.assertTrue(registered["registered"])
        status, receipt = self._post("/v1/ring/submit", {
            "frame": {
                "request_id": 3, "source_hw": 0, "destination_hw": 9,
                "nanclock_tick": 2, "kind": "nannet.route",
                "acl_digest": ACL, "word_hex": "7ff112345678f8ff",
            }
        })
        self.assertEqual(status, 200)
        self.assertEqual(receipt["central_status"], "delivered")
        status, drained = self._post("/v1/ring/drain", {"hw": 9, "limit": 2})
        self.assertEqual(status, 200)
        self.assertEqual(drained["count"], 1)
        request = Request(self.base + "/v1/ring/points",
                          headers={"Authorization": "Bearer " + TOKEN})
        with urlopen(request, timeout=3) as response:
            points = json.loads(response.read())
        self.assertGreaterEqual(points["fabric"]["registered"], 4)

    def test_auth_required(self):
        with self.assertRaises(HTTPError) as err:
            self._post("/v1/ring/submit", {"frame": {}}, token="x" * 40)
        self.assertEqual(err.exception.code, 401)


if __name__ == "__main__":
    unittest.main()
