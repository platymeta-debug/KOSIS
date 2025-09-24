from __future__ import annotations
import duckdb, pandas as pd
from .config import DB_PATH

def connect():
    return duckdb.connect(DB_PATH)

def init_db():
    con = connect()
    con.execute("""
    CREATE TABLE IF NOT EXISTS series_catalog (
      catalog_id BIGINT,
      logical_name TEXT,
      org_id TEXT,
      tbl_id TEXT,
      user_stats_id TEXT,
      prd_se TEXT,
      start_prd_de TEXT,
      end_prd_de TEXT,
      dims_hint JSON,
      meta JSON
    );
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS raw_kosis (
      catalog_id BIGINT,
      fetched_at TIMESTAMP,
      payload JSON
    );
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS obs (
      catalog_id BIGINT,
      series_key TEXT,
      period TEXT,
      period_freq TEXT,
      value DOUBLE,
      dims JSON,
      unit TEXT
    );
    """)
    return con

def overwrite_catalog(df: pd.DataFrame):
    con = connect()
    con.execute("DELETE FROM series_catalog")
    con.register("df", df)
    con.execute("INSERT INTO series_catalog SELECT * FROM df")
    con.close()

def latest_periods() -> pd.DataFrame:
    con = connect()
    q = """
    SELECT catalog_id, max(period) AS last_period
    FROM obs GROUP BY 1
    """
    try:
        return con.execute(q).df()
    except Exception:
        return pd.DataFrame(columns=["catalog_id","last_period"])
