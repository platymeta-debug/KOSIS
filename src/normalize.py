"""Payload normalisation helpers for KOSIS observations."""

from __future__ import annotations

import json
import re
from typing import Dict, List

import pandas as pd

from .validator import validate_prdse
from .store import upsert_obs

# Canonical field aliases encountered in KOSIS payloads.
VAL_KEYS = ["DT", "DATA_VALUE", "value", "dt"]
PERD_KEYS = ["PRD_DE", "prdDe", "period"]
UNIT_KEYS = ["UNIT_NM", "UNIT_NM_ENG", "unit", "unitName"]


def _pick(payload: Dict, keys: List[str], default=None):
    for key in keys:
        if key in payload and payload[key] not in (None, ""):
            return payload[key]
    return default


def _anchor_from_prd(prd_de: str, prd_se: str) -> tuple[str, str]:
    """Derive an ISO anchor date and frequency flag from the period descriptor."""

    cleaned = re.sub(r"[^0-9Qq]", "", str(prd_de))
    if prd_se == "M" and len(cleaned) >= 6:
        period = pd.Period(f"{cleaned[:4]}-{cleaned[4:6]}", freq="M").to_timestamp("M")
        return period.strftime("%Y-%m-%d"), "M"
    if prd_se == "Q" and len(cleaned) >= 5:
        match = re.match(r"^(\d{4})[Qq]([1-4])$", cleaned)
        if not match and len(cleaned) >= 6:
            year, quarter = cleaned[:4], cleaned[-1]
            candidate = f"{year}Q{quarter}"
            match = re.match(r"^(\d{4})Q([1-4])$", candidate)
        if match:
            year, quarter = match.groups()
            period = pd.Period(f"{year}Q{quarter}", freq="Q").to_timestamp("Q")
            return period.strftime("%Y-%m-%d"), "Q"
    if prd_se == "Y" and len(cleaned) >= 4:
        period = pd.Period(cleaned[:4], freq="A").to_timestamp("A")
        return period.strftime("%Y-%m-%d"), "Y"
    return cleaned, prd_se


def normalize_payload(logical_name: str, rows: List[Dict], prd_se: str) -> pd.DataFrame:
    """Convert raw payload rows into the canonical observation schema."""

    validate_prdse(prd_se)
    records: List[Dict] = []
    for row in rows:
        period_raw = _pick(row, PERD_KEYS)
        value_raw = _pick(row, VAL_KEYS)
        unit = _pick(row, UNIT_KEYS, default="")
        if period_raw in (None, "") or value_raw in (None, ""):
            continue
        try:
            value = float(str(value_raw).replace(",", ""))
        except Exception:
            continue
        period, freq = _anchor_from_prd(period_raw, prd_se)
        dims = {k: v for k, v in row.items() if k not in set(PERD_KEYS + VAL_KEYS)}
        series_key = logical_name + "|" + json.dumps(dims, ensure_ascii=False, sort_keys=True)
        records.append(
            {
                "series_key": series_key,
                "period": period,
                "freq": freq,
                "value": value,
                "unit": unit,
                "dims": json.dumps(dims, ensure_ascii=False),
            }
        )
    df = pd.DataFrame(records)
    if not df.empty:
        df = df.drop_duplicates(subset=["series_key", "period"]).sort_values(["series_key", "period"])
    return df


def store_obs(df_norm: pd.DataFrame) -> None:
    """Persist normalised observations when available."""

    if not df_norm.empty:
        upsert_obs(df_norm)
