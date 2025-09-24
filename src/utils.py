"""Utility helpers shared across the KOSIS pipeline."""

from __future__ import annotations

import random
import time
from typing import Any

import requests

from .config import MAX_RETRIES, RATE_SLEEP, TIMEOUT


def get_json(url: str, params: dict[str, Any]) -> Any:
    """Perform a GET request with retry/backoff logic and KOSIS specific guards."""

    backoff = 0.6
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(RATE_SLEEP)
            response = requests.get(url, params=params, timeout=TIMEOUT)
            if response.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(str(response.status_code))
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict) and (payload.get("errMsg") or payload.get("errorMsg")):
                raise RuntimeError(str(payload))
            if isinstance(payload, dict) and isinstance(payload.get("list"), list):
                return payload["list"]
            return payload
        except Exception:  # pragma: no cover - thin wrapper over network I/O
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(backoff * (1.6 ** attempt) + random.uniform(0, 0.3))
