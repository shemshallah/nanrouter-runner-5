#!/usr/bin/env python3
"""Standalone NaNVerse central zero-point fabric hub.

Run this on a non-Koyeb 24/7 runner (normally ``nanrouter-runner-0``).  Koyeb
stays the NANNET portal; the runner ring owns monitoring, translation and
NaN-to-NaN routing.  The hub uses the same bounded fabric protocol and the
same payload records as NaNROUTER's in-process service.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import signal
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlsplit

from vendor.point_fabric import (
    CentralPointFabric,
    FabricCapacityError,
    FabricError,
    FabricFrameError,
    FabricRoutingError,
    PointFrame,
    frame_from_json,
    frames_to_json,
)

SCHEMA = "nanverse.runner-hub.v1"
MAX_BODY_BYTES = 2_097_152
TOKEN_MIN = 32


class HubConfigError(ValueError):
    pass


def _token() -> str:
    token = os.environ.get("NANROUTER_FABRIC_TOKEN", "")
    if len(token) < TOKEN_MIN:
        raise HubConfigError(f"NANROUTER_FABRIC_TOKEN must be at least {TOKEN_MIN} characters")
    return token


def _int(value: Any, name: str, minimum: int, maximum: int) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise HubConfigError(f"{name} must be an integer") from exc
    if not minimum <= number <= maximum:
        raise HubConfigError(f"{name} must be in {minimum}..{maximum}")
    return number


def _active_points(value: str) -> tuple[int, ...]:
    points: list[int] = []
    for piece in str(value).split(","):
        piece = piece.strip()
        if not piece:
            continue
        point = _int(piece, "active point", 0, 255)
        if point not in points:
            points.append(point)
    return tuple(points)


class Hub:
    def __init__(self, fabric: CentralPointFabric, token: str):
        self.fabric = fabric
        self.token = token
        self.started_at = int(__import__("time").time())

    def summary(self) -> dict[str, Any]:
        monitor = self.fabric.monitor()
        return {
            "schema": SCHEMA,
            "fabric": monitor["fabric"],
            "registered": monitor["registered"],
            "active": monitor["active"],
            "queued": monitor["queued"],
            "pending": monitor["pending"],
            "received": monitor["received"],
            "delivered": monitor["delivered"],
            "rejected": monitor["rejected"],
            "journal": monitor["journal"],
            "started_at": self.started_at,
        }

    def authorized(self, header: str | None) -> bool:
        import hmac
        return hmac.compare_digest(header or "", "Bearer " + self.token)


def make_handler(hub: Hub):
    class Handler(BaseHTTPRequestHandler):
        server_version = "NaNVerseHub/1"

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
                raise FabricFrameError("Content-Type must be application/json")
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError as exc:
                raise FabricFrameError("invalid content length") from exc
            if not 1 <= length <= MAX_BODY_BYTES:
                raise FabricFrameError("request body is empty or exceeds 2 MiB")
            raw = self.rfile.read(length)
            try:
                value = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise FabricFrameError("request must be valid UTF-8 JSON") from exc
            if not isinstance(value, dict):
                raise FabricFrameError("request JSON must be an object")
            return value

        def _authorized(self) -> bool:
            if hub.authorized(self.headers.get("Authorization")):
                return True
            self._json(401, {"error": "fabric authorization required"})
            return False

        def _point(self) -> tuple[int, str] | None:
            match = re.fullmatch(
                r"/v1/fabric/points/(\d{1,3})(?:/(monitor|drain|register|heartbeat))?",
                urlsplit(self.path).path,
            )
            if match is None:
                return None
            return int(match.group(1)), match.group(2) or "monitor"

        def _fabric_error(self, exc: FabricError) -> None:
            if isinstance(exc, FabricCapacityError):
                self._json(429, {"error": str(exc)[:200]})
            elif isinstance(exc, FabricRoutingError):
                self._json(409, {"error": str(exc)[:200]})
            else:
                self._json(400, {"error": str(exc)[:200]})

        def do_GET(self) -> None:
            path = urlsplit(self.path).path
            if path == "/healthz":
                self._json(200, hub.summary())
                return
            if not self._authorized():
                return
            if path == "/v1/fabric/points":
                self._json(200, hub.fabric.monitor())
                return
            point = self._point()
            if point is not None and point[1] == "monitor":
                try:
                    self._json(200, hub.fabric.point_monitor(point[0]))
                except FabricRoutingError as exc:
                    self._json(404, {"error": str(exc)[:200]})
                return
            self._json(404, {"error": "not found"})

        def do_POST(self) -> None:
            path = urlsplit(self.path).path
            if path in ("/v1/fabric/intake", "/v1/fabric/route"):
                if not self._authorized():
                    return
                try:
                    body = self._body()
                    if "frame_hex" in body:
                        raw = body["frame_hex"]
                        if not isinstance(raw, str) or len(raw) > 512 or len(raw) % 2:
                            raise FabricFrameError("frame_hex must be bounded even-length hexadecimal")
                        frame = PointFrame.decode(bytes.fromhex(raw))
                    elif "frame" in body:
                        frame = frame_from_json(body["frame"])
                    else:
                        frame = frame_from_json(body)
                    self._json(200, hub.fabric.intake(frame))
                except FabricError as exc:
                    self._fabric_error(exc)
                except (ValueError, TypeError) as exc:
                    self._json(400, {"error": str(exc)[:200]})
                return
            point = self._point()
            if point is None:
                self._json(404, {"error": "not found"})
                return
            if not self._authorized():
                return
            hw, action = point
            try:
                body = self._body() if self.headers.get("Content-Length") not in (None, "0") else {}
                if action == "drain":
                    frames = hub.fabric.drain(hw, body.get("limit", 64))
                    self._json(200, {
                        "schema": "nanrouter.zero-point-drain.v1",
                        "endpoint": f"HW{hw}:0",
                        "count": len(frames),
                        "frames": frames_to_json(frames),
                    })
                elif action == "register":
                    self._json(200, hub.fabric.register(
                        hw, role=body.get("role"), status=body.get("status", "active")
                    ))
                elif action == "heartbeat":
                    self._json(200, hub.fabric.heartbeat(
                        hw, status=body.get("status", "active")
                    ))
                else:
                    self._json(405, {"error": "monitor is read-only; use GET"})
            except FabricError as exc:
                self._fabric_error(exc)
            except (ValueError, TypeError) as exc:
                self._json(400, {"error": str(exc)[:200]})

    return Handler


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=os.environ.get("HUB_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=os.environ.get("PORT", "8080"))
    parser.add_argument("--database", default=os.environ.get("FABRIC_DB", "/data/point-fabric.sqlite"))
    parser.add_argument("--active-points", default=os.environ.get("HUB_ACTIVE_POINTS", "0,2"))
    args = parser.parse_args(argv)
    try:
        token = _token()
        port = _int(args.port, "port", 1, 65535)
        active = _active_points(args.active_points)
    except HubConfigError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}), file=sys.stderr)
        return 2
    fabric = CentralPointFabric(database=args.database)
    for point in active:
        fabric.register(point, status="active")
    hub = Hub(fabric, token)
    server = ThreadingHTTPServer((args.host, port), make_handler(hub))
    stop = threading.Event()

    def _shutdown(_signum, _frame):
        stop.set()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(json.dumps({
        "ok": True, "event": "hub-listening", "host": args.host, "port": port,
        "active_points": list(active), "schema": SCHEMA,
    }, sort_keys=True))
    stop.wait()
    server.shutdown()
    server.server_close()
    fabric.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
