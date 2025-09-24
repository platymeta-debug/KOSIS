"""Utility helpers shared across the KOSIS pipeline."""

from __future__ import annotations

import time
from typing import Any

import requests

from .config import MAX_RETRIES, RATE_SLEEP, TIMEOUT


def get_json(url: str, params: dict[str, Any], *, verbose: bool = False) -> Any:
    """Perform a GET request with retry/backoff logic and KOSIS specific guards."""

    last_err: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        started = time.time()
        try:
            if verbose:
                debug_keys = ("method", "vwCd", "parentId", "pIndex", "pSize")
                debug_params = {k: params.get(k) for k in debug_keys}
                print(
                    f"[HTTP] GET {url} try={attempt} timeout={TIMEOUT} params={debug_params}"
                )
            response = requests.get(url, params=params, timeout=TIMEOUT)
            if verbose:
                elapsed = time.time() - started
                print(f"[HTTP] status={response.status_code} elapsed={elapsed:.2f}s")
            response.raise_for_status()
            payload = response.json()

            if isinstance(payload, dict) and payload.get("err"):
                raise RuntimeError(payload)

            time.sleep(RATE_SLEEP)
            return payload
        except Exception as exc:  # pragma: no cover - network/HTTP wrapper
            last_err = exc
            if verbose:
                print(f"[HTTP] error on try={attempt}: {exc}")
            time.sleep(min(1.0 * attempt, 3.0))

    raise RuntimeError(str(last_err))
