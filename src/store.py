"""Persistence helpers for the KOSIS pipeline."""

from __future__ import annotations

import json
from typing import Iterable

import duckdb
import pandas as pd

from .config import DB_PATH


def con() -> duckdb.DuckDBPyConnection:
    """Return a new DuckDB connection bound to the configured database."""

    return duckdb.connect(DB_PATH)


def init() -> duckdb.DuckDBPyConnection:
    """Initialise storage tables for raw payloads and normalised observations."""

    connection = con()
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_kosis (
          src TEXT,
          key TEXT,
          fetched_at TIMESTAMP,
          payload JSON
        );
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS obs (
          series_key TEXT,
          period TEXT,
          freq TEXT,
          value DOUBLE,
          unit TEXT,
          dims JSON
        );
        """
    )
    return connection


def save_raw(src: str, key: str, df_json: Iterable[dict]) -> None:
    """Persist the original JSON payload for traceability."""

    connection = con()
    connection.execute(
        "INSERT INTO raw_kosis VALUES (?, ?, now(), ?)",
        [src, key, json.dumps(list(df_json), ensure_ascii=False)],
    )
    connection.close()


def upsert_obs(df: pd.DataFrame) -> None:
    """Upsert a normalised observation dataframe into DuckDB."""

    connection = con()
    connection.register("df", df)
    connection.execute(  # DuckDB v0.9+ MERGE support
        """
        CREATE TEMP TABLE t AS SELECT * FROM df;
        MERGE INTO obs o USING t s
        ON o.series_key = s.series_key AND o.period = s.period
        WHEN MATCHED THEN UPDATE SET
            value = s.value,
            unit = s.unit,
            dims = s.dims,
            freq = s.freq
        WHEN NOT MATCHED THEN INSERT (series_key, period, freq, value, unit, dims)
        VALUES (s.series_key, s.period, s.freq, s.value, s.unit, s.dims);
        """
    )
    connection.close()
