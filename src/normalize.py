"""Normalisation helpers for aligning KOSIS payloads to the obs table."""

from __future__ import annotations

import json
import re
from typing import Iterable

import pandas as pd

from .store import upsert_obs


def period_to_anchor(prd_de: str, prd_se: str) -> tuple[str, str]:
    """Convert KOSIS period descriptors into ISO date anchors and frequency codes."""

    digits = re.sub(r"[^0-9]", "", str(prd_de))
    if prd_se == "M":
        period = pd.Period(f"{digits[:4]}-{digits[4:6]}", freq="M").to_timestamp("M").strftime("%Y-%m-%d")
        return period, "M"
    if prd_se == "Q":
        year, quarter = digits[:4], digits[-1]
        period = pd.Period(f"{year}Q{quarter}", freq="Q").to_timestamp("Q").strftime("%Y-%m-%d")
        return period, "Q"
    if prd_se == "Y":
        period = pd.Period(digits[:4], freq="A").to_timestamp("A").strftime("%Y-%m-%d")
        return period, "Y"
    return digits, prd_se


def flatten_payload(logical_name: str, rows: Iterable[dict], prd_se: str) -> pd.DataFrame:
    """Flatten the raw payload rows into the obs schema."""

    records = []
    for row in rows:
        period_value = row.get("PRD_DE") or row.get("prdDe")
        value = row.get("DT") or row.get("DATA_VALUE") or row.get("value")
        unit = row.get("UNIT_NM") or row.get("UNIT_NM_ENG") or ""
        if period_value is None or value in (None, ""):
            continue
        period, freq = period_to_anchor(str(period_value), prd_se)
        dims = {key: val for key, val in row.items() if key not in ("PRD_DE", "DT")}
        series_key = logical_name + "|" + json.dumps(dims, ensure_ascii=False, sort_keys=True)
        records.append(
            {
                "series_key": series_key,
                "period": period,
                "freq": freq,
                "value": float(value),
                "unit": unit,
                "dims": json.dumps(dims, ensure_ascii=False),
            }
        )
    return pd.DataFrame(records)


def normalize_and_store(logical_name: str, payload_rows: Iterable[dict], prd_se: str) -> None:
    """Normalise the supplied payload and persist the resulting observations."""

    frame = flatten_payload(logical_name, payload_rows, prd_se)
    if not frame.empty:
        upsert_obs(frame)
