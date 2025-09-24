"""Utility helpers shared across the KOSIS pipeline."""

from __future__ import annotations


import json
import time
from typing import Any

import requests

from .config import MAX_RETRIES, RATE_SLEEP, TIMEOUT


def get_json(
    url: str,
    params: dict[str, Any],
    *,
    headers: dict[str, str] | None = None,
    verbose: bool = False,
) -> Any:
    """Perform a GET request with retry/backoff logic and KOSIS specific guards."""

    last_err: Exception | None = None
    request_headers = {"Accept": "application/json"}
    if headers:
        request_headers.update(headers)
    for attempt in range(1, MAX_RETRIES + 1):
        started = time.time()
        try:
            if verbose:
                debug_keys = ("method", "vwCd", "parentId", "pIndex", "pSize")
                debug_params = {k: params.get(k) for k in debug_keys}
                print(
                    f"[HTTP] GET {url} try={attempt} timeout={TIMEOUT} params={debug_params}"
                )

            response = requests.get(
                url, params=params, timeout=TIMEOUT, headers=request_headers
            )
            if verbose:
                elapsed = time.time() - started
                ctype = response.headers.get("Content-Type")
                print(
                    f"[HTTP] status={response.status_code} ctype={ctype} elapsed={elapsed:.2f}s"
                )
            response.raise_for_status()

            text = response.text.strip()
            if text[:1] in "[{":
                payload = json.loads(text)
            else:
                raise RuntimeError(f"non-json body: {text[:120]}...")


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
