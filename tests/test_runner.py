import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from runner import (
    FabricClient,
    FabricClientError,
    HealthServer,
    LocalStore,
    RunnerAgent,
    RunnerConfig,
    RunnerConfigError,
)

ACL = "a" * 64
WORD = "7ff112345678f8ff"


def frame(frame_id="f" * 64, tick=4, kind="hermes.nan"):
    return {
        "frame_id": frame_id,
        "request_id": 1,
        "source_hw": 2,
        "destination_hw": 5,
        "nanclock_tick": tick,
        "kind": kind,
        "acl_digest": ACL,
        "word": {"raw_hex": WORD},
    }


class FakeClient:
    def __init__(self, frames=None):
        self.calls = []
        self.frames = list(frames or [])
        self.registered = []
        self.heartbeats = []
        self.intakes = []

    def register(self, hw, *, role, status="active"):
        self.calls.append(("register", hw, role, status))
        self.registered.append(hw)
        return {"registered": True}

    def heartbeat(self, hw, *, status="active"):
        self.calls.append(("heartbeat", hw, status))
        self.heartbeats.append(hw)
        return {"heartbeat": True}

    def drain(self, hw, limit=64):
        self.calls.append(("drain", hw, limit))
        result, self.frames = self.frames[:limit], self.frames[limit:]
        return result

    def intake(self, value):
        self.calls.append(("intake", value))
        self.intakes.append(value)
        return {"central_status": "delivered"}

    def monitor(self):
        return {"pending": 0}


class ConfigTests(unittest.TestCase):
    def test_identity_files_and_defaults(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "slot").write_text("3\n")
            (root / "hw").write_text("8\n")
            with patch.dict(os.environ, {
                "NANROUTER_URL": "https://router.example",
                "NANROUTER_FABRIC_TOKEN": "t" * 40,
                "RUNNER_STATE_DIR": str(root / "state"),
            }, clear=False):
                config = RunnerConfig.from_env(root)
        self.assertEqual(config.slot, 3)
        self.assertEqual(config.hw, 8)
        self.assertEqual(config.central_url, "https://router.example")
        self.assertEqual(config.name, "nanrouter-runner-3")

    def test_remote_http_is_rejected(self):
        with self.assertRaises(RunnerConfigError):
            RunnerConfig(
                slot=0, hw=5, central_url="http://router.example",
                token="t" * 40, state_dir="/tmp/x",
            )

    def test_short_token_is_rejected(self):
        with self.assertRaises(RunnerConfigError):
            RunnerConfig(
                slot=0, hw=5, central_url="https://router.example",
                token="short", state_dir="/tmp/x",
            )

    def test_reserved_hw_is_rejected(self):
        with self.assertRaises(RunnerConfigError):
            RunnerConfig(
                slot=0, hw=2, central_url="https://router.example",
                token="t" * 40, state_dir="/tmp/x",
            )


class AgentTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config = RunnerConfig(
            slot=0, hw=5, central_url="https://router.example",
            token="t" * 40, state_dir=Path(self.tmp.name) / "state",
            poll_seconds=1,
        )

    def test_register_poll_store_and_health(self):
        client = FakeClient([frame()])
        agent = RunnerAgent(self.config, client, LocalStore(self.config.state_dir))
        agent.register()
        result = agent.run_once()
        self.assertEqual(result["frames"], 1)
        self.assertEqual(client.registered, [5])
        self.assertEqual(client.heartbeats, [5])
        self.assertEqual(agent.stats.frames_received, 1)
        self.assertEqual(agent.stats.last_frame_id, "f" * 64)
        self.assertTrue(agent.ready)
        health = agent.health()
        self.assertEqual(health["endpoint"], "HW5:0")
        self.assertNotIn("t" * 40, json.dumps(health))

    def test_relay_is_explicit(self):
        config = RunnerConfig(
            slot=0, hw=5, central_url="https://router.example",
            token="t" * 40, state_dir=Path(self.tmp.name) / "state",
            relay_destination=0,
        )
        client = FakeClient([frame()])
        agent = RunnerAgent(
            config, client, LocalStore(config.state_dir),
            request_id=lambda: 99,
        )
        agent.run_once()
        self.assertEqual(len(client.intakes), 1)
        forwarded = client.intakes[0]
        self.assertEqual(forwarded["source_hw"], 5)
        self.assertEqual(forwarded["destination_hw"], 0)
        self.assertEqual(forwarded["word_hex"], WORD)
        self.assertEqual(forwarded["request_id"], 99)
        self.assertEqual(agent.stats.frames_relayed, 1)

    def test_kind_prefixed_route_table(self):
        config = RunnerConfig(
            slot=0, hw=5, central_url="https://router.example",
            token="t" * 40, state_dir=Path(self.tmp.name) / "state",
            routes={"hermes.": 2, "vector": 0},
        )
        client = FakeClient([frame(kind="hermes.nan"), frame(kind="vector")])
        agent = RunnerAgent(config, client, LocalStore(config.state_dir))
        agent.run_once()
        self.assertEqual([item["destination_hw"] for item in client.intakes], [2, 0])
        self.assertEqual(agent.stats.frames_relayed, 2)

    def test_route_table_avoids_self_loop(self):
        config = RunnerConfig(
            slot=0, hw=5, central_url="https://router.example",
            token="t" * 40, state_dir=Path(self.tmp.name) / "state",
            routes={"hermes.": 5},
        )
        client = FakeClient([frame(kind="hermes.nan")])
        agent = RunnerAgent(config, client, LocalStore(config.state_dir))
        agent.run_once()
        self.assertEqual(client.intakes, [])

    def test_received_frame_journal_is_durable(self):
        store = LocalStore(self.config.state_dir)
        store.append(frame())
        line = (self.config.state_dir / "frames.jsonl").read_text()
        self.assertIn("nanverse.runner-frame-log.v1", line)
        self.assertEqual(store.count, 1)

    def test_health_server(self):
        client = FakeClient()
        agent = RunnerAgent(self.config, client, LocalStore(self.config.state_dir))
        agent.register()
        agent.run_once()
        server = HealthServer(agent, "127.0.0.1", 0).start()
        self.addCleanup(server.stop)
        import urllib.request
        with urllib.request.urlopen(
            f"http://127.0.0.1:{server.port}/healthz", timeout=3
        ) as response:
            value = json.loads(response.read())
        self.assertEqual(value["schema"], "nanverse.runner-health.v1")
        self.assertTrue(value["ready"])


class ClientTests(unittest.TestCase):
    def test_client_repr_never_contains_token(self):
        client = FabricClient("https://router.example", "secret" * 8)
        self.assertNotIn("secret", repr(client))


if __name__ == "__main__":
    unittest.main()
