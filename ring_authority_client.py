"""Authenticated client for the Koyeb atomic runner-ring authority."""
from __future__ import annotations

import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

MAX_RESPONSE_BYTES = 2_097_152


class RingAuthorityError(RuntimeError):
    """The Koyeb runner-ring authority rejected or could not serve a request."""


class RingAuthorityClient:
    def __init__(self, base_url: str, token: str, *, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self._token = token
        self.timeout = timeout

    def _request(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        raw = None if body is None else json.dumps(body, sort_keys=True).encode("utf-8")
        headers = {"Authorization": "Bearer " + self._token}
        if raw is not None:
            headers["Content-Type"] = "application/json"
        request = Request(self.base_url + path, data=raw, headers=headers, method=method)
        try:
            with urlopen(request, timeout=self.timeout) as response:
                payload = response.read(MAX_RESPONSE_BYTES + 1)
                if len(payload) > MAX_RESPONSE_BYTES:
                    raise RingAuthorityError("authority response exceeds 2 MiB")
                value = json.loads(payload) if payload else {}
        except HTTPError as exc:
            detail = exc.read(512).decode("utf-8", "replace")
            raise RingAuthorityError(
                f"authority HTTP {exc.code}: {detail[:320]}"
            ) from None
        except (URLError, TimeoutError, OSError) as exc:
            raise RingAuthorityError(
                f"authority unavailable: {exc.__class__.__name__}"
            ) from None
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RingAuthorityError("authority returned invalid JSON") from exc
        if not isinstance(value, dict):
            raise RingAuthorityError("authority response must be a JSON object")
        return value

    def clock(self) -> dict[str, Any]:
        return self._request("GET", "/v1/runner-ring/clock")

    def status(self) -> dict[str, Any]:
        return self._request("GET", "/v1/runner-ring/status")

    def lease(
        self,
        *,
        slot: int,
        base_epoch: int,
        endpoint: str,
        ttl_seconds: int = 30,
    ) -> dict[str, Any]:
        return self._request("POST", "/v1/runner-ring/lease", {
            "slot": slot,
            "base_epoch": base_epoch,
            "endpoint": endpoint,
            "ttl_seconds": ttl_seconds,
        })

    def renew(self, *, slot: int, lease_id: str, ttl_seconds: int = 30) -> dict[str, Any]:
        return self._request("POST", "/v1/runner-ring/renew", {
            "slot": slot, "lease_id": lease_id, "ttl_seconds": ttl_seconds,
        })

    def release(self, *, slot: int, lease_id: str) -> dict[str, Any]:
        return self._request("POST", "/v1/runner-ring/release", {
            "slot": slot, "lease_id": lease_id,
        })

    def checkpoint(
        self, *, slot: int, lease_id: str, state: dict[str, Any], nanclock_tick: int
    ) -> dict[str, Any]:
        return self._request("POST", "/v1/runner-ring/checkpoint", {
            "slot": slot, "lease_id": lease_id, "state": state,
            "nanclock_tick": nanclock_tick,
        })

    def read_checkpoint(self) -> dict[str, Any] | None:
        value = self._request("GET", "/v1/runner-ring/checkpoint")
        checkpoint = value.get("checkpoint")
        return checkpoint if isinstance(checkpoint, dict) else None

    def __repr__(self) -> str:
        return f"RingAuthorityClient(base_url={self.base_url!r})"
