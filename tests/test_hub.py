import json
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from hub import Hub, make_handler
from runner import FabricClient
from vendor.point_fabric import CentralPointFabric, Nan16DEWord, PointFrame

ACL = "a" * 64
TOKEN = "h" * 40


def make_frame(source=2, destination=5):
    return PointFrame(
        request_id=7, source_hw=source, destination_hw=destination,
        nanclock_tick=3, kind="hermes.nan", acl_digest=ACL,
        word=Nan16DEWord.make(0x7FF1, 0xFFF8, 0x01020304),
    )


class HubTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.fabric = CentralPointFabric(database=Path(self.tmp.name) / "hub.sqlite")
        self.fabric.register(0, status="active")
        self.fabric.register(2, status="active")
        self.hub = Hub(self.fabric, TOKEN)
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(self.hub))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.thread.join)
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def test_agent_register_intake_and_drain(self):
        client = FabricClient(self.base, TOKEN)
        client.register(5, role="runner-0")
        frame = make_frame()
        receipt = client.intake({
            "request_id": frame.request_id,
            "source_hw": frame.source_hw,
            "destination_hw": frame.destination_hw,
            "nanclock_tick": frame.nanclock_tick,
            "kind": frame.kind,
            "acl_digest": frame.acl_digest,
            "word_hex": frame.word.pack().hex(),
        })
        self.assertEqual(receipt["central_status"], "delivered")
        frames = client.drain(5, 1)
        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0]["word"]["raw_hex"], frame.word.pack().hex())

    def test_healthz_and_monitor_require_token(self):
        with urlopen(self.base + "/healthz", timeout=3) as response:
            health = json.loads(response.read())
        self.assertEqual(health["schema"], "nanverse.runner-hub.v1")
        with self.assertRaises(HTTPError) as err:
            urlopen(self.base + "/v1/fabric/points", timeout=3)
        self.assertEqual(err.exception.code, 401)

    def test_raw_frame_hex_round_trip(self):
        client = FabricClient(self.base, TOKEN)
        client.register(5, role="runner-0")
        frame = make_frame()
        receipt = client._request("POST", "/v1/fabric/intake", {
            "frame_hex": frame.encode().hex(),
        })
        self.assertEqual(receipt["central_status"], "delivered")


if __name__ == "__main__":
    unittest.main()
