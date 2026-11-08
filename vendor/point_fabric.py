"""NaNVerse zero-point translation fabric.

Every registered hardware node has one zero point (``HWn:0``).  A
:class:`PointTranslationModule` runs at that position, accepts exact NaN16DE
words, monitors its bounded inbox, and either delivers frames to its local
consumer or hands them to the central :class:`CentralPointFabric`.

The central fabric is a control/transport object only:

* it does not evaluate NaN words;
* it does not infer a route from a byte offset or a runner slot;
* it preserves the NaN16DE word bytes exactly;
* it persists pending frames when a journal path is supplied.

``HW0:0`` is the Bootstrap network port.  ``HW2:0`` is the HERMES endpoint.
Frames addressed to ``HW0:0`` are egress; frames carrying ``HW0:0`` as their
source are ingress.  All other points are ordinary registered hardware nodes
in the ``HW5:0``..``HW255:0`` allocation range.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import hashlib
import re
import sqlite3
import struct
import threading
import time
from pathlib import Path
from typing import Any, Iterable

FABRIC_SCHEMA = "nanrouter.zero-point-fabric.v1"
FRAME_SCHEMA = "nanrouter.zero-point-frame.v1"
MONITOR_SCHEMA = "nanrouter.zero-point-monitor.v1"

FRAME_MAGIC = b"NPF1"
FRAME_VERSION = 1
MAX_KIND_BYTES = 64
MAX_FRAME_BYTES = 256
MAX_INBOX = 4096
MAX_PENDING = 4096

NAN16DE_HEADS = (0x7FF1, 0xFFF1, 0x7FF8, 0xFFF8)
NAN16DE_HEADS_HEX = tuple(hex(head) for head in NAN16DE_HEADS)

# Fixed BS0:0 hardware-node roles.  The zero point of each node is the
# translation-module position.  Points 5..255 are registered on demand.
FIXED_ROLES = {
    0: "bootstrap-hardware-node",
    1: "personal-phone",
    2: "hermes-hardware-node",
    3: "clocked-hardware-node",
    4: "wordnet-universe",
}
FIXED_STATUS = {
    0: "active",
    1: "paired-device-only",
    2: "paired-device-only",
    3: "paired-device-only",
    4: "reserved",
}
REGISTRATION_MIN = 5
REGISTRATION_MAX = 255

_ACL = re.compile(r"^[0-9a-f]{64}$")
_KIND = re.compile(r"^[A-Za-z0-9_.:-]{1,64}$")
_ORIGIN = re.compile(r"^[A-Za-z0-9_.:-]{1,64}$")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")


class FabricError(ValueError):
    """Base class for zero-point fabric validation and routing failures."""


class FabricFrameError(FabricError):
    """A NaN16DE word or point frame is malformed."""


class FabricRoutingError(FabricError):
    """A frame cannot be routed to the named zero point."""


class FabricCapacityError(FabricError):
    """A bounded inbox or pending queue is full."""


def _uint(value: Any, name: str, maximum: int = (1 << 64) - 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise FabricFrameError(f"{name} must be an integer")
    if not 0 <= value <= maximum:
        raise FabricFrameError(f"{name} must be in 0..{maximum}")
    return value


def _hw(value: Any, name: str = "hardware point") -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise FabricFrameError(f"{name} must be an integer")
    if not 0 <= value <= 255:
        raise FabricFrameError(f"{name} must be in 0..255")
    return value


def _text(value: Any, name: str, pattern: re.Pattern[str]) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise FabricFrameError(f"{name} has an unsupported value")
    return value


def _digest(value: Any, name: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise FabricFrameError(f"{name} must be 64 lowercase hexadecimal characters")
    return value


@dataclass(frozen=True)
class Nan16DEWord:
    """One exact NaN16DE word (far BE, payload LE, near LE).

    The class validates the two legal NaN heads and the 32-bit payload.  It
    performs no arithmetic on the payload.
    """

    far: int
    near: int
    payload: int

    def __post_init__(self) -> None:
        if self.far not in NAN16DE_HEADS or self.near not in NAN16DE_HEADS:
            raise FabricFrameError("NaN16DE far/near head is not a legal NaN head")
        if isinstance(self.payload, bool) or not isinstance(self.payload, int) \
                or not 0 <= self.payload <= 0xFFFFFFFF:
            raise FabricFrameError("NaN16DE payload must be a 32-bit unsigned value")

    @classmethod
    def parse(cls, raw: bytes | bytearray | memoryview) -> "Nan16DEWord":
        data = bytes(raw)
        if len(data) != 8:
            raise FabricFrameError("NaN16DE word must be exactly 8 bytes")
        return cls(
            int.from_bytes(data[0:2], "big"),
            int.from_bytes(data[6:8], "little"),
            int.from_bytes(data[2:6], "little"),
        )

    @classmethod
    def make(cls, far: int, near: int, payload: int) -> "Nan16DEWord":
        return cls(far, near, payload)

    def pack(self) -> bytes:
        return (
            self.far.to_bytes(2, "big")
            + self.payload.to_bytes(4, "little")
            + self.near.to_bytes(2, "little")
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "raw_hex": self.pack().hex(),
            "far": hex(self.far),
            "near": hex(self.near),
            "payload": self.payload,
            "byte_order": "far:BE,payload:LE,near:LE",
        }


@dataclass(frozen=True)
class PointFrame:
    """A bounded transport frame between zero points.

    ``acl_digest`` is a SHA-256 hex label for the caller's authorization
    context.  It is evidence metadata, not a replacement for the authoritative
    portal token/lease/ACL check.
    """

    request_id: int
    source_hw: int
    destination_hw: int
    nanclock_tick: int
    kind: str
    acl_digest: str
    word: Nan16DEWord
    origin: str = "zero-point"

    def __post_init__(self) -> None:
        object.__setattr__(self, "request_id", _uint(self.request_id, "request_id"))
        object.__setattr__(self, "source_hw", _hw(self.source_hw, "source_hw"))
        object.__setattr__(
            self, "destination_hw", _hw(self.destination_hw, "destination_hw")
        )
        object.__setattr__(
            self, "nanclock_tick", _uint(self.nanclock_tick, "nanclock_tick")
        )
        object.__setattr__(self, "kind", _text(self.kind, "kind", _KIND))
        object.__setattr__(self, "acl_digest", _digest(self.acl_digest, "acl_digest"))
        if not isinstance(self.word, Nan16DEWord):
            raise FabricFrameError("word must be a Nan16DEWord")
        object.__setattr__(self, "origin", _text(self.origin, "origin", _ORIGIN))

    @property
    def frame_id(self) -> str:
        return hashlib.sha256(self.encode()).hexdigest()

    def encode(self) -> bytes:
        kind = self.kind.encode("ascii")
        body = (
            FRAME_MAGIC
            + bytes((FRAME_VERSION, self.source_hw, self.destination_hw, len(kind)))
            + struct.pack(">QQ", self.request_id, self.nanclock_tick)
            + bytes.fromhex(self.acl_digest)
            + self.word.pack()
            + kind
        )
        return body + hashlib.sha256(body).digest()

    @classmethod
    def decode(cls, raw: bytes | bytearray | memoryview) -> "PointFrame":
        data = bytes(raw)
        if len(data) < 4 + 4 + 16 + 32 + 8 + 1 + 32:
            raise FabricFrameError("point frame is too short")
        if data[:4] != FRAME_MAGIC:
            raise FabricFrameError("point frame magic is invalid")
        version = data[4]
        if version != FRAME_VERSION:
            raise FabricFrameError("point frame version is unsupported")
        source_hw, destination_hw, kind_len = data[5], data[6], data[7]
        if not 1 <= kind_len <= MAX_KIND_BYTES:
            raise FabricFrameError("point frame kind length is invalid")
        request_id, tick = struct.unpack_from(">QQ", data, 8)
        acl = data[24:56]
        word = Nan16DEWord.parse(data[56:64])
        kind = data[64:64 + kind_len]
        body_end = 64 + kind_len
        if len(data) != body_end + 32:
            raise FabricFrameError("point frame has trailing or missing bytes")
        if hashlib.sha256(data[:body_end]).digest() != data[body_end:]:
            raise FabricFrameError("point frame digest mismatch")
        try:
            kind_text = kind.decode("ascii")
        except UnicodeDecodeError as exc:
            raise FabricFrameError("point frame kind must be ASCII") from exc
        return cls(
            request_id=request_id,
            source_hw=source_hw,
            destination_hw=destination_hw,
            nanclock_tick=tick,
            kind=kind_text,
            acl_digest=acl.hex(),
            word=word,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": FRAME_SCHEMA,
            "frame_id": self.frame_id,
            "request_id": self.request_id,
            "source_hw": self.source_hw,
            "source": f"HW{self.source_hw}:0",
            "destination_hw": self.destination_hw,
            "destination": f"HW{self.destination_hw}:0",
            "nanclock_tick": self.nanclock_tick,
            "kind": self.kind,
            "acl_digest": self.acl_digest,
            "origin": self.origin,
            "word": self.word.to_dict(),
        }


class PointTranslationModule:
    """The bounded translation/monitor instance at one ``HWn:0`` position."""

    def __init__(
        self,
        hw: int,
        role: str,
        status: str,
        *,
        inbox_limit: int = MAX_INBOX,
    ) -> None:
        self.hw = _hw(hw)
        self.role = role
        self.status = status
        if not 1 <= inbox_limit <= MAX_INBOX:
            raise FabricError(f"inbox_limit must be in 1..{MAX_INBOX}")
        self.inbox_limit = inbox_limit
        self._inbox: deque[PointFrame] = deque()
        self._lock = threading.RLock()
        self._received = 0
        self._delivered = 0
        self._rejected = 0
        self._last_tick = 0
        self._last_frame_id: str | None = None

    @property
    def endpoint(self) -> str:
        return f"HW{self.hw}:0"

    @property
    def address(self) -> str:
        return (
            "nan://GM0:0::SC0:0::MC0:0::BS0:0"
            f"::HW{self.hw}:0"
        )

    @property
    def active(self) -> bool:
        return self.status == "active"

    def accept(self, frame: PointFrame) -> dict[str, Any]:
        if not isinstance(frame, PointFrame):
            raise FabricFrameError("frame must be a PointFrame")
        if frame.destination_hw != self.hw:
            raise FabricRoutingError(
                f"{self.endpoint} cannot accept a frame for HW{frame.destination_hw}:0"
            )
        if not self.active:
            raise FabricRoutingError(
                f"{self.endpoint} is {self.status!r}, not active"
            )
        with self._lock:
            if len(self._inbox) >= self.inbox_limit:
                self._rejected += 1
                raise FabricCapacityError(f"{self.endpoint} inbox is full")
            self._inbox.append(frame)
            self._received += 1
            self._last_tick = max(self._last_tick, frame.nanclock_tick)
            self._last_frame_id = frame.frame_id
            return {
                "accepted": True,
                "status": "queued",
                "endpoint": self.endpoint,
                "frame_id": self._last_frame_id,
                "queued": len(self._inbox),
            }

    def drain(self, limit: int = 64) -> tuple[PointFrame, ...]:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1024:
            raise FabricError("drain limit must be in 1..1024")
        with self._lock:
            count = min(limit, len(self._inbox))
            frames = tuple(self._inbox.popleft() for _ in range(count))
            self._delivered += len(frames)
            return frames

    def monitor(self) -> dict[str, Any]:
        with self._lock:
            return {
                "schema": MONITOR_SCHEMA,
                "endpoint": self.endpoint,
                "address": self.address,
                "hw": self.hw,
                "role": self.role,
                "status": self.status,
                "active": self.active,
                "queued": len(self._inbox),
                "inbox_limit": self.inbox_limit,
                "received": self._received,
                "delivered": self._delivered,
                "rejected": self._rejected,
                "last_nanclock_tick": self._last_tick,
                "last_frame_id": self._last_frame_id,
            }


class FabricJournal:
    """Durable pending-frame journal for restart-safe zero-point delivery."""

    def __init__(self, database: str | Path) -> None:
        self.database = str(database)
        Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=FULL")
            db.execute(
                """CREATE TABLE IF NOT EXISTS zero_point_frames (
                    frame_id TEXT PRIMARY KEY,
                    source_hw INTEGER NOT NULL,
                    destination_hw INTEGER NOT NULL,
                    nanclock_tick TEXT NOT NULL,
                    state TEXT NOT NULL,
                    frame BLOB NOT NULL,
                    created_at TEXT NOT NULL
                )"""
            )
            db.execute(
                "CREATE INDEX IF NOT EXISTS zero_point_pending "
                "ON zero_point_frames(state, destination_hw)"
            )

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.database, timeout=15, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=15000")
        return db

    def record(self, frame: PointFrame, state: str = "pending") -> None:
        if state not in ("pending", "delivered"):
            raise FabricError("invalid journal state")
        with self._connect() as db:
            db.execute(
                """INSERT OR REPLACE INTO zero_point_frames
                   (frame_id, source_hw, destination_hw, nanclock_tick,
                    state, frame, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, datetime('now'))""",
                (
                    frame.frame_id,
                    frame.source_hw,
                    frame.destination_hw,
                    str(frame.nanclock_tick),
                    state,
                    frame.encode(),
                ),
            )

    def mark_delivered(self, frame_id: str) -> None:
        if not isinstance(frame_id, str) or _DIGEST.fullmatch(frame_id) is None:
            raise FabricError("invalid frame id")
        with self._connect() as db:
            db.execute(
                "UPDATE zero_point_frames SET state='delivered' WHERE frame_id=?",
                (frame_id,),
            )

    def pending(self, destination_hw: int | None = None) -> tuple[PointFrame, ...]:
        with self._connect() as db:
            if destination_hw is None:
                rows = db.execute(
                    "SELECT frame FROM zero_point_frames WHERE state='pending' "
                    "ORDER BY created_at, frame_id"
                ).fetchall()
            else:
                _hw(destination_hw)
                rows = db.execute(
                    "SELECT frame FROM zero_point_frames WHERE state='pending' "
                    "AND destination_hw=? ORDER BY created_at, frame_id",
                    (destination_hw,),
                ).fetchall()
        return tuple(PointFrame.decode(row["frame"]) for row in rows)

    def stats(self) -> dict[str, Any]:
        with self._connect() as db:
            rows = db.execute(
                "SELECT state, COUNT(*) AS count FROM zero_point_frames GROUP BY state"
            ).fetchall()
        counts = {row["state"]: row["count"] for row in rows}
        return {
            "schema": "nanrouter.zero-point-journal.v1",
            "pending": counts.get("pending", 0),
            "delivered": counts.get("delivered", 0),
        }


class CentralPointFabric:
    """Central zero-point intake, monitor, and NaN-to-NaN routing authority."""

    def __init__(
        self,
        *,
        database: str | Path | None = None,
        inbox_limit: int = MAX_INBOX,
        pending_limit: int = MAX_PENDING,
        now_tick: Any = None,
    ) -> None:
        if not 1 <= pending_limit <= MAX_PENDING:
            raise FabricError(f"pending_limit must be in 1..{MAX_PENDING}")
        self.inbox_limit = inbox_limit
        self.pending_limit = pending_limit
        self._now_tick = now_tick or (lambda: int(time.time()))
        self._lock = threading.RLock()
        self._points: dict[int, PointTranslationModule] = {}
        self._pending: deque[PointFrame] = deque()
        self._journal = FabricJournal(database) if database is not None else None
        self._received = 0
        self._delivered = 0
        self._pending_events = 0
        self._rejected = 0
        self._routes: list[dict[str, Any]] = []
        self._build_fixed_points()
        self._restore_pending()

    def _build_fixed_points(self) -> None:
        for hw, role in FIXED_ROLES.items():
            self._points[hw] = PointTranslationModule(
                hw, role, FIXED_STATUS[hw], inbox_limit=self.inbox_limit
            )

    def _restore_pending(self) -> None:
        if self._journal is None:
            return
        for frame in self._journal.pending():
            if len(self._pending) >= self.pending_limit:
                break
            self._pending.append(frame)

    def point(self, hw: int) -> PointTranslationModule:
        hw = _hw(hw)
        with self._lock:
            module = self._points.get(hw)
            if module is None:
                raise FabricRoutingError(f"HW{hw}:0 is not registered")
            return module

    def register(
        self,
        hw: int,
        *,
        role: str | None = None,
        status: str = "active",
    ) -> dict[str, Any]:
        hw = _hw(hw)
        if status not in (
            "active", "paired-device-only", "reserved", "degraded", "revoked"
        ):
            raise FabricError("unsupported zero-point status")
        with self._lock:
            if hw in FIXED_ROLES:
                expected = FIXED_ROLES[hw]
                if role is not None and role != expected:
                    raise FabricError(
                        f"HW{hw}:0 role is fixed as {expected!r}"
                    )
                role = expected
            else:
                if hw < REGISTRATION_MIN or hw > REGISTRATION_MAX:
                    raise FabricError(
                        f"only HW{REGISTRATION_MIN}..HW{REGISTRATION_MAX} may register"
                    )
                role = role or "general-hardware-node"
                if not isinstance(role, str) or _KIND.fullmatch(role) is None:
                    raise FabricError("role has an unsupported value")
            module = PointTranslationModule(
                hw, role, status, inbox_limit=self.inbox_limit
            )
            self._points[hw] = module
            moved = self._deliver_pending(hw, module)
            return {
                "registered": True,
                "point": module.monitor(),
                "pending_delivered": moved,
            }

    def _deliver_pending(self, hw: int, module: PointTranslationModule) -> int:
        """Replay central pending frames into an active destination module."""
        if not module.active:
            return 0
        moved = 0
        remaining: deque[PointFrame] = deque()
        while self._pending:
            frame = self._pending.popleft()
            if frame.destination_hw != hw:
                remaining.append(frame)
                continue
            try:
                module.accept(frame)
            except FabricCapacityError:
                remaining.append(frame)
            else:
                moved += 1
                if self._journal is not None:
                    self._journal.mark_delivered(frame.frame_id)
        self._pending = remaining
        return moved

    def heartbeat(self, hw: int, *, status: str = "active") -> dict[str, Any]:
        """Update a zero point's liveness without discarding its inbox."""
        hw = _hw(hw)
        if status not in (
            "active", "paired-device-only", "reserved", "degraded", "revoked"
        ):
            raise FabricError("unsupported zero-point status")
        with self._lock:
            if hw not in self._points:
                return self.register(hw, status=status)
            module = self._points[hw]
            module.status = status
            moved = self._deliver_pending(hw, module)
            return {
                "heartbeat": True,
                "point": module.monitor(),
                "pending_delivered": moved,
            }

    def intake(self, frame: PointFrame) -> dict[str, Any]:
        if not isinstance(frame, PointFrame):
            raise FabricFrameError("frame must be a PointFrame")
        with self._lock:
            self._received += 1
            if frame.source_hw not in self._points:
                self._rejected += 1
                raise FabricRoutingError(
                    f"source HW{frame.source_hw}:0 is not registered"
                )
            source = self._points[frame.source_hw]
            if not source.active and frame.origin != "network":
                self._rejected += 1
                raise FabricRoutingError(
                    f"source {source.endpoint} is {source.status!r}, not active"
                )
            destination = self._points.get(frame.destination_hw)
            if destination is not None and destination.active:
                try:
                    receipt = destination.accept(frame)
                except (FabricRoutingError, FabricCapacityError):
                    self._rejected += 1
                    raise
                self._delivered += 1
                if self._journal is not None:
                    self._journal.record(frame, "delivered")
                self._routes.append({
                    "frame_id": frame.frame_id,
                    "source_hw": frame.source_hw,
                    "destination_hw": frame.destination_hw,
                    "status": "delivered",
                    "nanclock_tick": frame.nanclock_tick,
                })
                self._trim_routes()
                return {
                    **receipt,
                    "central_status": "delivered",
                    "source": f"HW{frame.source_hw}:0",
                    "destination": f"HW{frame.destination_hw}:0",
                }
            if frame.destination_hw in FIXED_ROLES:
                self._rejected += 1
                raise FabricRoutingError(
                    f"destination HW{frame.destination_hw}:0 is not active"
                )
            if len(self._pending) >= self.pending_limit:
                self._rejected += 1
                raise FabricCapacityError("central pending queue is full")
            self._pending.append(frame)
            self._pending_events += 1
            if self._journal is not None:
                self._journal.record(frame, "pending")
            self._routes.append({
                "frame_id": frame.frame_id,
                "source_hw": frame.source_hw,
                "destination_hw": frame.destination_hw,
                "status": "pending",
                "nanclock_tick": frame.nanclock_tick,
            })
            self._trim_routes()
            return {
                "accepted": True,
                "status": "pending",
                "central_status": "pending",
                "frame_id": frame.frame_id,
                "source": f"HW{frame.source_hw}:0",
                "destination": f"HW{frame.destination_hw}:0",
                "pending": len(self._pending),
            }

    def route(self, frame: PointFrame) -> dict[str, Any]:
        """Alias of :meth:`intake`; routing is intake plus destination select."""
        return self.intake(frame)

    def drain(self, hw: int, limit: int = 64) -> tuple[PointFrame, ...]:
        module = self.point(hw)
        frames = module.drain(limit)
        if self._journal is not None:
            for frame in frames:
                self._journal.mark_delivered(frame.frame_id)
        return frames

    def _trim_routes(self, maximum: int = 256) -> None:
        if len(self._routes) > maximum:
            del self._routes[: len(self._routes) - maximum]

    def monitor(self) -> dict[str, Any]:
        with self._lock:
            points = [self._points[hw].monitor() for hw in sorted(self._points)]
            return {
                "schema": MONITOR_SCHEMA,
                "fabric": FABRIC_SCHEMA,
                "points": points,
                "registered": len(self._points),
                "active": sum(1 for point in points if point["active"]),
                "queued": sum(point["queued"] for point in points),
                "pending": len(self._pending),
                "received": self._received,
                "delivered": self._delivered,
                "pending_events": self._pending_events,
                "rejected": self._rejected,
                "journal": None if self._journal is None else self._journal.stats(),
                "recent_routes": list(self._routes[-32:]),
                "nan16de_heads": list(NAN16DE_HEADS_HEX),
            }

    def point_monitor(self, hw: int) -> dict[str, Any]:
        return self.point(hw).monitor()

    def close(self) -> None:
        with self._lock:
            self._points.clear()
            self._pending.clear()


def frame_from_json(value: Any) -> PointFrame:
    """Build a frame from a bounded JSON object used by the HTTP adapter."""
    if not isinstance(value, dict):
        raise FabricFrameError("frame must be a JSON object")
    word_raw = value.get("word_hex")
    if not isinstance(word_raw, str) or len(word_raw) != 16:
        raise FabricFrameError("word_hex must be exactly 16 hexadecimal characters")
    try:
        word = Nan16DEWord.parse(bytes.fromhex(word_raw))
    except ValueError as exc:
        raise FabricFrameError("word_hex is not valid hexadecimal") from exc
    return PointFrame(
        request_id=value.get("request_id"),
        source_hw=value.get("source_hw"),
        destination_hw=value.get("destination_hw"),
        nanclock_tick=value.get("nanclock_tick"),
        kind=value.get("kind"),
        acl_digest=value.get("acl_digest"),
        word=word,
        origin=value.get("origin", "zero-point"),
    )


def frames_to_json(frames: Iterable[PointFrame]) -> list[dict[str, Any]]:
    return [
        {
            **frame.to_dict(),
            "frame_hex": frame.encode().hex(),
        }
        for frame in frames
    ]


__all__ = [
    "CentralPointFabric",
    "FabricCapacityError",
    "FabricError",
    "FabricFrameError",
    "FabricJournal",
    "FabricRoutingError",
    "FIXED_ROLES",
    "FIXED_STATUS",
    "NAN16DE_HEADS",
    "Nan16DEWord",
    "PointFrame",
    "PointTranslationModule",
    "REGISTRATION_MAX",
    "REGISTRATION_MIN",
    "frame_from_json",
    "frames_to_json",
]
