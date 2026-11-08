#!/usr/bin/env python3
"""NaNVerse 24/7 zero-point runner agent.

This process owns one ``HWn:0`` zero point.  It registers/heartbeats with the
central NaNROUTER zero-point fabric, drains NaN16DE frames into a durable local
journal, and optionally relays frames to an explicitly configured destination.

It is deliberately not a model trainer and not a math engine.  NaN word bytes
are preserved exactly; the agent performs no arithmetic on them.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import signal
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

SCHEMA = "nanverse.runner-agent.v1"
HEALTH_SCHEMA = "nanverse.runner-health.v1"
MAX_BODY_BYTES = 2_097_152
SLOT_MIN = 0
SLOT_MAX = 5
HW_MIN = 5
HW_MAX = 255
TOKEN_MIN = 32


class RunnerConfigError(ValueError):
    """The runner configuration is incomplete or unsafe."""


class FabricClientError(RuntimeError):
    """The central fabric rejected or could not serve a request."""


def _bounded_text(value: Any, name: str, maximum: int = 128) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise RunnerConfigError(f"{name} must be a non-empty string <= {maximum} characters")
    return value


def _int(value: Any, name: str, minimum: int, maximum: int) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise RunnerConfigError(f"{name} must be an integer") from exc
    if not minimum <= number <= maximum:
        raise RunnerConfigError(f"{name} must be in {minimum}..{maximum}")
    return number


def _read_identity_file(root: Path, name: str) -> str | None:
    target = root / name
    if not target.is_file():
        return None
    text = target.read_text(encoding="utf-8").strip()
    return text or None


def _local_url(value: str) -> bool:
    host = (urlsplit(value).hostname or "").lower()
    return host in ("127.0.0.1", "localhost", "::1")


@dataclass
class RunnerConfig:
    slot: int
    hw: int
    central_url: str
    token: str
    state_dir: Path
    poll_seconds: float = 5.0
    relay_destination: int | None = None
    routes: dict[str, int] = field(default_factory=dict)
    health_host: str = "0.0.0.0"
    health_port: int = 8080
    name: str = "nanrouter-runner"

    def __post_init__(self) -> None:
        self.slot = _int(self.slot, "slot", SLOT_MIN, SLOT_MAX)
        self.hw = _int(self.hw, "hw", HW_MIN, HW_MAX)
        if not isinstance(self.central_url, str):
            raise RunnerConfigError("central_url must be a string")
        self.central_url = self.central_url.rstrip("/")
        parts = urlsplit(self.central_url)
        if parts.scheme not in ("https", "http") or not parts.hostname:
            raise RunnerConfigError("central_url must be an http(s) URL")
        if parts.scheme != "https" and not _local_url(self.central_url):
            raise RunnerConfigError("central_url must use HTTPS except on loopback")
        if not isinstance(self.token, str) or len(self.token) < TOKEN_MIN:
            raise RunnerConfigError(f"fabric token must be at least {TOKEN_MIN} characters")
        self.state_dir = Path(self.state_dir)
        if self.poll_seconds < 0.25 or self.poll_seconds > 300:
            raise RunnerConfigError("poll_seconds must be in 0.25..300")
        if self.relay_destination is not None:
            self.relay_destination = _int(self.relay_destination, "relay_destination", 0, 255)
        if not isinstance(self.routes, dict) or len(self.routes) > 32:
            raise RunnerConfigError("routes must be an object with at most 32 entries")
        normalized_routes: dict[str, int] = {}
        for prefix, destination in self.routes.items():
            if not isinstance(prefix, str) or not 1 <= len(prefix) <= 64:
                raise RunnerConfigError("route prefixes must be bounded strings")
            normalized_routes[prefix] = _int(destination, "route destination", 0, 255)
        self.routes = normalized_routes
        self.name = _bounded_text(self.name, "name", 64)

    @classmethod
    def from_env(
        cls,
        root: str | Path | None = None,
        *,
        slot: int | None = None,
        hw: int | None = None,
        central_url: str | None = None,
        state_dir: str | Path | None = None,
    ) -> "RunnerConfig":
        base = Path(root) if root is not None else Path(__file__).resolve().parent
        configured_slot = slot
        if configured_slot is None:
            configured_slot = os.environ.get("RUNNER_SLOT") or _read_identity_file(base, "slot")
        configured_hw = hw
        if configured_hw is None:
            configured_hw = os.environ.get("RUNNER_HW") or _read_identity_file(base, "hw")
        configured_url = central_url or os.environ.get("NANROUTER_URL", "")
        token = os.environ.get("NANROUTER_FABRIC_TOKEN", "")
        configured_state = state_dir or os.environ.get("RUNNER_STATE_DIR")
        slot_number = _int(configured_slot if configured_slot is not None else 0, "slot", SLOT_MIN, SLOT_MAX)
        if configured_state is None:
            configured_state = Path.home() / ".local" / "share" / f"nanrouter-runner-{slot_number}"
        relay = os.environ.get("RUNNER_RELAY_DESTINATION") or None
        routes: dict[str, int] = {}
        raw_routes = os.environ.get("RUNNER_ROUTES", "").strip()
        if raw_routes:
            try:
                parsed_routes = json.loads(raw_routes)
            except json.JSONDecodeError as exc:
                raise RunnerConfigError("RUNNER_ROUTES must be a JSON object") from exc
            if not isinstance(parsed_routes, dict):
                raise RunnerConfigError("RUNNER_ROUTES must be a JSON object")
            routes = parsed_routes
        return cls(
            slot=slot_number,
            hw=_int(configured_hw if configured_hw is not None else HW_MIN + slot_number, "hw", HW_MIN, HW_MAX),
            central_url=configured_url,
            token=token,
            state_dir=Path(configured_state),
            poll_seconds=float(os.environ.get("RUNNER_POLL_SECONDS", "5")),
            relay_destination=None if relay is None else _int(relay, "relay_destination", 0, 255),
            routes=routes,
            health_host=os.environ.get("RUNNER_HEALTH_HOST", "0.0.0.0"),
            health_port=_int(os.environ.get("PORT", os.environ.get("RUNNER_HEALTH_PORT", "8080")), "health_port", 1, 65535),
            name=os.environ.get("RUNNER_NAME", f"nanrouter-runner-{slot_number}"),
        )


class FabricClient:
    """Small JSON client for the central zero-point fabric."""

    def __init__(self, base_url: str, token: str, *, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self._token = token
        self.timeout = timeout

    def _request(self, method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        raw = None if body is None else json.dumps(body, sort_keys=True).encode("utf-8")
        headers = {"Authorization": "Bearer " + self._token}
        if raw is not None:
            headers["Content-Type"] = "application/json"
        request = Request(self.base_url + path, data=raw, headers=headers, method=method)
        try:
            with urlopen(request, timeout=self.timeout) as response:
                payload = response.read(MAX_BODY_BYTES + 1)
                if len(payload) > MAX_BODY_BYTES:
                    raise FabricClientError("central fabric response exceeds 2 MiB")
                value = json.loads(payload) if payload else {}
        except HTTPError as exc:
            detail = exc.read(512).decode("utf-8", "replace")
            raise FabricClientError(f"central fabric HTTP {exc.code}: {detail[:320]}") from None
        except (URLError, TimeoutError, OSError) as exc:
            raise FabricClientError(f"central fabric unavailable: {exc.__class__.__name__}") from None
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FabricClientError("central fabric returned invalid JSON") from exc
        if not isinstance(value, dict):
            raise FabricClientError("central fabric response must be a JSON object")
        return value

    def register(self, hw: int, *, role: str, status: str = "active") -> dict[str, Any]:
        return self._request("POST", f"/v1/fabric/points/{hw}/register", {
            "role": role, "status": status,
        })

    def heartbeat(self, hw: int, *, status: str = "active") -> dict[str, Any]:
        return self._request("POST", f"/v1/fabric/points/{hw}/heartbeat", {
            "status": status,
        })

    def drain(self, hw: int, limit: int = 64) -> list[dict[str, Any]]:
        value = self._request("POST", f"/v1/fabric/points/{hw}/drain", {"limit": limit})
        frames = value.get("frames", [])
        if not isinstance(frames, list):
            raise FabricClientError("central fabric drain frames must be a list")
        return frames

    def intake(self, frame: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", "/v1/fabric/intake", frame)

    def monitor(self) -> dict[str, Any]:
        return self._request("GET", "/v1/fabric/points")

    def __repr__(self) -> str:
        return f"FabricClient(base_url={self.base_url!r})"


class LocalStore:
    """Append-only durable JSONL journal for drained frames."""

    def __init__(self, state_dir: str | Path):
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.path = self.state_dir / "frames.jsonl"
        self._lock = threading.Lock()
        self._count = 0

    def append(self, frame: dict[str, Any]) -> None:
        record = {
            "schema": "nanverse.runner-frame-log.v1",
            "received_at": int(time.time()),
            "frame": frame,
        }
        line = json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"
        with self._lock:
            with open(self.path, "a", encoding="utf-8") as handle:
                handle.write(line)
                handle.flush()
                os.fsync(handle.fileno())
            self._count += 1

    @property
    def count(self) -> int:
        return self._count


@dataclass
class RunnerStats:
    registered: bool = False
    started_at: int = field(default_factory=lambda: int(time.time()))
    polls: int = 0
    heartbeats: int = 0
    frames_received: int = 0
    frames_relayed: int = 0
    errors: int = 0
    last_poll_at: int = 0
    last_error: str | None = None
    last_frame_id: str | None = None
    last_nanclock_tick: int = 0
    central_pending: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": HEALTH_SCHEMA,
            "registered": self.registered,
            "started_at": self.started_at,
            "polls": self.polls,
            "heartbeats": self.heartbeats,
            "frames_received": self.frames_received,
            "frames_relayed": self.frames_relayed,
            "errors": self.errors,
            "last_poll_at": self.last_poll_at,
            "last_error": self.last_error,
            "last_frame_id": self.last_frame_id,
            "last_nanclock_tick": self.last_nanclock_tick,
            "central_pending": self.central_pending,
        }


class RunnerAgent:
    """Register, heartbeat, drain, persist and optionally relay one zero point."""

    def __init__(
        self,
        config: RunnerConfig,
        client: FabricClient,
        store: LocalStore,
        *,
        clock: Callable[[], float] = time.time,
        request_id: Callable[[], int] | None = None,
    ) -> None:
        self.config = config
        self.client = client
        self.store = store
        self._clock = clock
        self._request_id = request_id or (lambda: int(time.time_ns()) & ((1 << 63) - 1))
        self._lock = threading.RLock()
        self.stats = RunnerStats()
        self._stop = threading.Event()

    @property
    def ready(self) -> bool:
        if not self.stats.registered:
            return False
        if self.stats.last_poll_at == 0:
            return False
        return self._clock() - self.stats.last_poll_at <= max(30.0, self.config.poll_seconds * 3)

    def register(self) -> dict[str, Any]:
        result = self.client.register(
            self.config.hw, role=self.config.name, status="active"
        )
        with self._lock:
            self.stats.registered = True
            self.stats.last_error = None
        return result

    def _route_destination(self, frame: dict[str, Any]) -> int | None:
        if self.config.relay_destination is not None:
            return self.config.relay_destination
        kind = frame.get("kind")
        if not isinstance(kind, str):
            return None
        for prefix in sorted(self.config.routes, key=len, reverse=True):
            if kind.startswith(prefix):
                destination = self.config.routes[prefix]
                return None if destination == self.config.hw else destination
        return None

    def _relay(self, frame: dict[str, Any]) -> None:
        destination = self._route_destination(frame)
        if destination is None:
            return
        word = frame.get("word")
        word_hex = word.get("raw_hex") if isinstance(word, dict) else None
        if not isinstance(word_hex, str):
            raise FabricClientError("drained frame has no NaN16DE word")
        tick = frame.get("nanclock_tick", 0)
        if not isinstance(tick, int) or tick < 0:
            tick = 0
        forwarded = {
            "request_id": self._request_id(),
            "source_hw": self.config.hw,
            "destination_hw": destination,
            "nanclock_tick": max(tick, int(self._clock())),
            "kind": frame.get("kind", "relay"),
            "acl_digest": frame.get("acl_digest"),
            "word_hex": word_hex,
            "origin": self.config.name,
        }
        self.client.intake(forwarded)
        with self._lock:
            self.stats.frames_relayed += 1

    def run_once(self, limit: int = 64) -> dict[str, Any]:
        with self._lock:
            self.stats.polls += 1
        self.client.heartbeat(self.config.hw)
        with self._lock:
            self.stats.heartbeats += 1
        frames = self.client.drain(self.config.hw, limit)
        for frame in frames:
            self.store.append(frame)
            with self._lock:
                self.stats.frames_received += 1
                self.stats.last_frame_id = frame.get("frame_id")
                tick = frame.get("nanclock_tick")
                if isinstance(tick, int):
                    self.stats.last_nanclock_tick = max(self.stats.last_nanclock_tick, tick)
            self._relay(frame)
        with self._lock:
            self.stats.last_poll_at = int(self._clock())
            self.stats.last_error = None
        return {
            "schema": SCHEMA,
            "slot": self.config.slot,
            "hw": self.config.hw,
            "endpoint": f"HW{self.config.hw}:0",
            "frames": len(frames),
            "relayed": self.stats.frames_relayed,
            "stored": self.store.count,
        }

    def run_forever(self) -> None:
        backoff = self.config.poll_seconds
        while not self._stop.is_set():
            try:
                if not self.stats.registered:
                    self.register()
                self.run_once()
                backoff = self.config.poll_seconds
            except Exception as exc:  # noqa: BLE001 - a runner must stay alive
                with self._lock:
                    self.stats.errors += 1
                    self.stats.last_error = f"{exc.__class__.__name__}: {str(exc)[:200]}"
                self._stop.wait(backoff)
                backoff = min(max(backoff * 2, self.config.poll_seconds), 60.0)
                continue
            self._stop.wait(self.config.poll_seconds)

    def stop(self) -> None:
        self._stop.set()

    def health(self) -> dict[str, Any]:
        with self._lock:
            value = {
                **self.stats.to_dict(),
                "slot": self.config.slot,
                "hw": self.config.hw,
                "endpoint": f"HW{self.config.hw}:0",
                "central_url": self.config.central_url,
                "relay_destination": self.config.relay_destination,
                "routes": dict(self.config.routes),
                "ready": self.ready,
            }
        return value


def make_health_handler(agent: RunnerAgent):
    class Handler(BaseHTTPRequestHandler):
        server_version = "NaNVerseRunner/1"

        def log_message(self, fmt: str, *args: Any) -> None:
            # Health paths carry no credentials; keep the default line format.
            super().log_message(fmt, *args)

        def _json(self, status: int, value: dict[str, Any]) -> None:
            raw = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self) -> None:
            path = urlsplit(self.path).path
            if path == "/healthz":
                self._json(200, agent.health())
            elif path == "/readyz":
                self._json(200 if agent.ready else 503, agent.health())
            elif path == "/metrics":
                self._json(200, agent.health())
            else:
                self._json(404, {"error": "not found"})

        def do_POST(self) -> None:
            self._json(405, {"error": "method not allowed"})

    return Handler


class HealthServer:
    def __init__(self, agent: RunnerAgent, host: str, port: int):
        self.httpd = ThreadingHTTPServer((host, port), make_health_handler(agent))
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    @property
    def port(self) -> int:
        return int(self.httpd.server_address[1])

    def start(self) -> "HealthServer":
        self.thread.start()
        return self

    def stop(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=5)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="register and poll once, then exit")
    parser.add_argument("--slot", type=int, default=None)
    parser.add_argument("--hw", type=int, default=None)
    parser.add_argument("--central-url", default=None)
    parser.add_argument("--state-dir", default=None)
    parser.add_argument("--health-port", type=int, default=None)
    parser.add_argument("--no-health-server", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        config = RunnerConfig.from_env(
            slot=args.slot,
            hw=args.hw,
            central_url=args.central_url,
            state_dir=args.state_dir,
        )
        if args.health_port is not None:
            config.health_port = _int(args.health_port, "health_port", 1, 65535)
    except (RunnerConfigError, ValueError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}), file=sys.stderr)
        return 2
    client = FabricClient(config.central_url, config.token)
    store = LocalStore(config.state_dir)
    agent = RunnerAgent(config, client, store)
    try:
        if args.once:
            agent.register()
            print(json.dumps({"ok": True, **agent.run_once()}, sort_keys=True))
            return 0
        server = None if args.no_health_server else HealthServer(
            agent, config.health_host, config.health_port
        ).start()
        if server is not None:
            print(json.dumps({
                "ok": True, "event": "health-listening",
                "port": server.port, "slot": config.slot, "hw": config.hw,
            }, sort_keys=True))
        agent.register()
        print(json.dumps({
            "ok": True, "event": "registered",
            "slot": config.slot, "hw": config.hw,
            "central_url": config.central_url,
        }, sort_keys=True))
        agent.run_forever()
        return 0
    except KeyboardInterrupt:
        return 0
    finally:
        agent.stop()


if __name__ == "__main__":
    raise SystemExit(main())
