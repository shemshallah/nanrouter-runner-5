#!/usr/bin/env python3
"""NaNVerse six-slot runner-ring node.

All six ``nanrouter-runner-0..5`` repositories run this same program.  A slot
file (0..5) is the only identity difference.  The runners are the internal
BSR0..BSR5 members of MC0:0; they do **not** receive individual HW addresses.
The ring as a whole is ``HW5:0`` for NaN routing.

Exactly one slot holds the atomic Koyeb runner-ring lease.  The active slot
runs the NaN routing fabric; the other five are hot standbys.  Koyeb supplies
a strictly monotone nanclock tick and the lease/epoch transaction.  On expiry,
release or explicit handoff, the next slot takes over, restores the bounded
checkpoint and continues without a separate HW address.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlsplit

from memory_budget import DEFAULT_BUDGET, MemoryBudget, MemoryBudgetError
from ring_authority_client import RingAuthorityClient, RingAuthorityError
from slot_fabric import RING_ENDPOINT, RING_HW, SlotFabric, SlotFabricError

SCHEMA = "nanverse.runner-ring-node.v1"
HEALTH_SCHEMA = "nanverse.runner-ring-health.v1"
MAX_BODY_BYTES = 2_097_152
SLOT_COUNT = 6
TOKEN_MIN = 32


class RingConfigError(ValueError):
    pass


def _int(value: Any, name: str, minimum: int, maximum: int) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise RingConfigError(f"{name} must be an integer") from exc
    if not minimum <= number <= maximum:
        raise RingConfigError(f"{name} must be in {minimum}..{maximum}")
    return number


def _https(value: str, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise RingConfigError(f"{name} is required")
    parts = urlsplit(value)
    if parts.scheme not in ("https", "http") or not parts.hostname:
        raise RingConfigError(f"{name} must be an http(s) URL")
    host = (parts.hostname or "").lower()
    if parts.scheme != "https" and host not in ("127.0.0.1", "localhost", "::1"):
        raise RingConfigError(f"{name} must use HTTPS except on loopback")
    return value.rstrip("/")


def _read_slot(root: Path) -> int:
    target = root / "slot"
    if not target.is_file():
        raise RingConfigError("slot file is required")
    text = target.read_text(encoding="utf-8").strip()
    if not text:
        raise RingConfigError("slot file is empty")
    return _int(text, "slot", 0, SLOT_COUNT - 1)


@dataclass
class RingConfig:
    slot: int
    portal_url: str
    ring_token: str
    advertise_url: str
    state_dir: Path
    poll_seconds: float = 2.0
    ttl_seconds: int = 30
    checkpoint_seconds: float = 5.0
    memory_budget_bytes: int = DEFAULT_BUDGET
    health_host: str = "0.0.0.0"
    health_port: int = 8080

    def __post_init__(self) -> None:
        self.slot = _int(self.slot, "slot", 0, SLOT_COUNT - 1)
        self.portal_url = _https(self.portal_url, "portal_url")
        if not isinstance(self.ring_token, str) or len(self.ring_token) < TOKEN_MIN:
            raise RingConfigError(f"ring token must be at least {TOKEN_MIN} characters")
        self.advertise_url = _https(self.advertise_url, "advertise_url")
        self.state_dir = Path(self.state_dir)
        if not 0.25 <= self.poll_seconds <= 60:
            raise RingConfigError("poll_seconds must be in 0.25..60")
        self.ttl_seconds = _int(self.ttl_seconds, "ttl_seconds", 10, 300)
        if not 1 <= self.checkpoint_seconds <= 120:
            raise RingConfigError("checkpoint_seconds must be in 1..120")
        self.memory_budget_bytes = _int(
            self.memory_budget_bytes, "memory_budget_bytes", 1, 64 * 1024 ** 3
        )
        self.health_port = _int(self.health_port, "health_port", 1, 65535)

    @classmethod
    def from_env(cls, root: str | Path | None = None, *, slot: int | None = None) -> "RingConfig":
        base = Path(root) if root is not None else Path(__file__).resolve().parent
        configured_slot = slot
        if configured_slot is None:
            configured_slot = os.environ.get("NANROUTER_RUNNER_SLOT") or _read_slot(base)
        port = os.environ.get("PORT", os.environ.get("NANROUTER_RUNNER_HEALTH_PORT", "8080"))
        advertise = os.environ.get("NANROUTER_RUNNER_URL") or f"http://127.0.0.1:{port}"
        state_dir = os.environ.get("NANROUTER_RUNNER_STATE_DIR") or str(
            Path.home() / ".local" / "share" / f"nanrouter-runner-{configured_slot}"
        )
        return cls(
            slot=_int(configured_slot, "slot", 0, SLOT_COUNT - 1),
            portal_url=os.environ.get("NANROUTER_RING_URL", ""),
            ring_token=os.environ.get("NANROUTER_RING_TOKEN", ""),
            advertise_url=advertise,
            state_dir=Path(state_dir),
            poll_seconds=float(os.environ.get("NANROUTER_RUNNER_POLL_SECONDS", "2")),
            ttl_seconds=_int(os.environ.get("NANROUTER_RUNNER_TTL_SECONDS", "30"), "ttl_seconds", 10, 300),
            checkpoint_seconds=float(os.environ.get("NANROUTER_RUNNER_CHECKPOINT_SECONDS", "5")),
            memory_budget_bytes=_int(
                os.environ.get("NANROUTER_RUNNER_MEMORY_BYTES", str(DEFAULT_BUDGET)),
                "memory_budget_bytes", 1, 64 * 1024 ** 3,
            ),
            health_host=os.environ.get("NANROUTER_RUNNER_HEALTH_HOST", "0.0.0.0"),
            health_port=_int(port, "health_port", 1, 65535),
        )


@dataclass
class CoordinatorStats:
    started_at: int = field(default_factory=lambda: int(time.time()))
    phase: str = "standby"
    ticks: int = 0
    leases: int = 0
    handoffs: int = 0
    checkpoints: int = 0
    errors: int = 0
    last_error: str | None = None
    last_nanclock_tick: int = 0
    last_tick_at: int = 0
    epoch: int = 0
    active_slot: int | None = None
    has_lease: bool = False
    last_checkpoint_at: int = 0


class RunnerRingCoordinator:
    def __init__(
        self,
        config: RingConfig,
        client: RingAuthorityClient,
        fabric: SlotFabric,
        budget: MemoryBudget,
        *,
        clock=time.time,
    ):
        self.config = config
        self.client = client
        self.fabric = fabric
        self.budget = budget
        self._clock = clock
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self.stats = CoordinatorStats()
        self._lease: dict[str, Any] | None = None

    @property
    def active(self) -> bool:
        with self._lock:
            return self._lease is not None and self.fabric.active

    def _error(self, exc: BaseException) -> None:
        with self._lock:
            self.stats.errors += 1
            self.stats.last_error = f"{exc.__class__.__name__}: {str(exc)[:200]}"

    def _checkpoint_locked(self) -> None:
        if self._lease is None:
            return
        state = self.fabric.export_state()
        size = len(json.dumps(state, sort_keys=True, separators=(",", ":")).encode())
        self.budget.reserve(size, "ring-checkpoint")
        try:
            result = self.client.checkpoint(
                slot=self.config.slot,
                lease_id=self._lease["lease_id"],
                state=state,
                nanclock_tick=self.stats.last_nanclock_tick,
            )
        finally:
            self.budget.release(size, "ring-checkpoint")
        self.stats.checkpoints += 1
        self.stats.last_checkpoint_at = int(self._clock())
        self._lease["checkpoint_digest"] = result.get("state_digest")

    def _step_down_locked(self, reason: str) -> None:
        lease = self._lease
        if lease is not None:
            try:
                self._checkpoint_locked()
            except Exception as exc:  # noqa: BLE001 - best effort before handoff
                self._error(exc)
            try:
                self.client.release(slot=self.config.slot, lease_id=lease["lease_id"])
            except Exception as exc:  # noqa: BLE001 - expiry will free the slot
                self._error(exc)
        self.fabric.deactivate()
        self._lease = None
        self.stats.phase = "standby"
        self.stats.has_lease = False
        self.stats.active_slot = None
        with self._lock:
            self.stats.handoffs += 1
            self.stats.last_error = reason

    def _activate_locked(self, lease: dict[str, Any]) -> None:
        checkpoint = None
        try:
            checkpoint = self.client.read_checkpoint()
        except RingAuthorityError as exc:
            self._error(exc)
        if checkpoint and isinstance(checkpoint.get("state"), dict):
            try:
                self.fabric.import_state(checkpoint["state"])
            except Exception as exc:  # noqa: BLE001 - fail closed to a fresh ring
                self._error(exc)
        self.fabric.activate()
        self._lease = lease
        self.stats.phase = "active"
        self.stats.has_lease = True
        self.stats.active_slot = self.config.slot
        self.stats.epoch = int(lease.get("epoch", 0))
        self.stats.leases += 1
        self.stats.handoffs += 1

    def tick(self) -> dict[str, Any]:
        try:
            clock = self.client.clock()
            tick = int(clock["nanclock_tick"])
        except (RingAuthorityError, KeyError, TypeError, ValueError) as exc:
            self._error(exc)
            return self.health()
        with self._lock:
            self.stats.ticks += 1
            self.stats.last_nanclock_tick = max(self.stats.last_nanclock_tick, tick)
            self.stats.last_tick_at = int(self._clock())
            if self._lease is not None:
                try:
                    renewed = self.client.renew(
                        slot=self.config.slot,
                        lease_id=self._lease["lease_id"],
                        ttl_seconds=self.config.ttl_seconds,
                    )
                except (RingAuthorityError, KeyError, TypeError, ValueError) as exc:
                    self._error(exc)
                    self._step_down_locked("renew-failed")
                    return self.health()
                self._lease.update(renewed)
                self.stats.epoch = int(renewed.get("epoch", self.stats.epoch))
                self.stats.has_lease = True
                self.stats.phase = "active"
                self.stats.active_slot = self.config.slot
                if self._clock() - self.stats.last_checkpoint_at >= self.config.checkpoint_seconds:
                    try:
                        self._checkpoint_locked()
                    except Exception as exc:  # noqa: BLE001 - next tick retries
                        self._error(exc)
                return self.health()
            try:
                lease = self.client.lease(
                    slot=self.config.slot,
                    base_epoch=self.stats.epoch,
                    endpoint=self.config.advertise_url,
                    ttl_seconds=self.config.ttl_seconds,
                )
            except (RingAuthorityError, KeyError, TypeError, ValueError) as exc:
                self._error(exc)
                return self.health()
            if lease.get("acquired"):
                self._activate_locked(lease)
            else:
                self.stats.phase = "standby"
                self.stats.has_lease = False
                self.stats.active_slot = lease.get("active_slot")
                self.stats.epoch = int(lease.get("epoch", self.stats.epoch))
            return self.health()

    def checkpoint(self) -> dict[str, Any]:
        with self._lock:
            if self._lease is None:
                raise SlotFabricError("this slot has no active lease")
            self._checkpoint_locked()
            return self.health()

    def handoff(self) -> dict[str, Any]:
        with self._lock:
            self._step_down_locked("explicit-handoff")
            return self.health()

    def run_forever(self) -> None:
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception as exc:  # noqa: BLE001 - the ring must stay alive
                self._error(exc)
            self._stop.wait(self.config.poll_seconds)
        with self._lock:
            if self._lease is not None:
                self._step_down_locked("shutdown")

    def stop(self) -> None:
        self._stop.set()

    def health(self) -> dict[str, Any]:
        with self._lock:
            return {
                "schema": HEALTH_SCHEMA,
                "node": SCHEMA,
                "slot": self.config.slot,
                "ring_endpoint": RING_ENDPOINT,
                "ring_hw": RING_HW,
                "phase": self.stats.phase,
                "active": self.active,
                "has_lease": self.stats.has_lease,
                "epoch": self.stats.epoch,
                "active_slot": self.stats.active_slot,
                "nanclock_tick": self.stats.last_nanclock_tick,
                "ticks": self.stats.ticks,
                "leases": self.stats.leases,
                "handoffs": self.stats.handoffs,
                "checkpoints": self.stats.checkpoints,
                "errors": self.stats.errors,
                "last_error": self.stats.last_error,
                "last_tick_at": self.stats.last_tick_at,
                "last_checkpoint_at": self.stats.last_checkpoint_at,
                "started_at": self.stats.started_at,
                "memory": self.budget.stats(),
                "fabric": self.fabric.monitor(),
            }


def make_handler(coordinator: RunnerRingCoordinator, token: str):
    class Handler(BaseHTTPRequestHandler):
        server_version = "NaNVerseRunnerRing/1"

        def log_message(self, fmt: str, *args: Any) -> None:
            super().log_message(fmt, *args)

        def _json(self, status: int, value: dict[str, Any]) -> None:
            raw = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(raw)

        def _body(self) -> dict[str, Any]:
            if self.headers.get_content_type() != "application/json":
                raise ValueError("Content-Type must be application/json")
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError as exc:
                raise ValueError("invalid content length") from exc
            if not 1 <= length <= MAX_BODY_BYTES:
                raise ValueError("request body is empty or exceeds 2 MiB")
            raw = self.rfile.read(length)
            try:
                value = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValueError("request must be valid UTF-8 JSON") from exc
            if not isinstance(value, dict):
                raise ValueError("request JSON must be an object")
            return value

        def _auth(self) -> bool:
            import hmac
            header = self.headers.get("Authorization", "")
            if not header.startswith("Bearer ") or not hmac.compare_digest(
                header[7:], token
            ):
                self._json(401, {"error": "runner ring authorization required"})
                return False
            return True

        def do_GET(self) -> None:
            path = urlsplit(self.path).path
            if path == "/healthz":
                self._json(200, coordinator.health())
            elif path == "/readyz":
                health = coordinator.health()
                ready = health["last_tick_at"] > 0
                self._json(200 if ready else 503, health)
            elif path == "/metrics":
                self._json(200, coordinator.health())
            elif path == "/v1/ring/status":
                if self._auth():
                    self._json(200, coordinator.health())
            elif path == "/v1/ring/clock":
                if self._auth():
                    try:
                        self._json(200, coordinator.client.clock())
                    except RingAuthorityError as exc:
                        self._json(503, {"error": str(exc)[:200]})
            elif path == "/v1/ring/points":
                if self._auth():
                    self._json(200, coordinator.fabric.monitor())
            else:
                self._json(404, {"error": "not found"})

        def do_POST(self) -> None:
            path = urlsplit(self.path).path
            if not self._auth():
                return
            try:
                body = self._body()
                if path == "/v1/ring/submit":
                    if not coordinator.active:
                        self._json(409, {"error": "this slot is not active"})
                        return
                    self._json(200, coordinator.fabric.submit_json(body.get("frame", body)))
                elif path == "/v1/ring/drain":
                    if not coordinator.active:
                        self._json(409, {"error": "this slot is not active"})
                        return
                    hw = int(body.get("hw", RING_HW))
                    limit = int(body.get("limit", 64))
                    frames = coordinator.fabric.drain(hw, limit)
                    self._json(200, {
                        "schema": "nanverse.runner-ring-drain.v1",
                        "ring_endpoint": RING_ENDPOINT,
                        "count": len(frames),
                        "frames": frames,
                    })
                elif path == "/v1/ring/register":
                    if not coordinator.active:
                        self._json(409, {"error": "this slot is not active"})
                        return
                    self._json(200, coordinator.fabric.register_point(
                        int(body["hw"]), role=body.get("role"),
                        status=body.get("status", "active")))
                elif path == "/v1/ring/handoff":
                    self._json(200, coordinator.handoff())
                elif path == "/v1/ring/checkpoint":
                    self._json(200, coordinator.checkpoint())
                else:
                    self._json(404, {"error": "not found"})
            except (SlotFabricError, MemoryBudgetError) as exc:
                self._json(409, {"error": str(exc)[:200]})
            except (ValueError, TypeError, KeyError) as exc:
                self._json(400, {"error": str(exc)[:200]})

    return Handler


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--slot", type=int, default=None)
    parser.add_argument("--portal-url", default=None)
    parser.add_argument("--advertise-url", default=None)
    parser.add_argument("--state-dir", default=None)
    parser.add_argument("--health-port", type=int, default=None)
    parser.add_argument("--no-health-server", action="store_true")
    args = parser.parse_args(argv)
    try:
        config = RingConfig.from_env(slot=args.slot)
        if args.portal_url:
            config.portal_url = _https(args.portal_url, "portal_url")
        if args.advertise_url:
            config.advertise_url = _https(args.advertise_url, "advertise_url")
        if args.state_dir:
            config.state_dir = Path(args.state_dir)
        if args.health_port is not None:
            config.health_port = _int(args.health_port, "health_port", 1, 65535)
    except (RingConfigError, ValueError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}), file=sys.stderr)
        return 2
    config.state_dir.mkdir(parents=True, exist_ok=True)
    client = RingAuthorityClient(config.portal_url, config.ring_token)
    fabric = SlotFabric(database=str(config.state_dir / "ring-fabric.sqlite"))
    budget = MemoryBudget(config.memory_budget_bytes)
    coordinator = RunnerRingCoordinator(config, client, fabric, budget)
    server = None
    try:
        if args.once:
            result = coordinator.tick()
            print(json.dumps({"ok": True, **result}, sort_keys=True))
            return 0
        if not args.no_health_server:
            server = ThreadingHTTPServer(
                (config.health_host, config.health_port),
                make_handler(coordinator, config.ring_token),
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            print(json.dumps({
                "ok": True, "event": "ring-node-listening",
                "slot": config.slot, "port": server.server_address[1],
                "ring_endpoint": RING_ENDPOINT,
                "portal_url": config.portal_url,
            }, sort_keys=True))
        coordinator.run_forever()
        return 0
    except KeyboardInterrupt:
        return 0
    finally:
        coordinator.stop()
        if server is not None:
            server.shutdown()
            server.server_close()
        fabric.close()


if __name__ == "__main__":
    raise SystemExit(main())
