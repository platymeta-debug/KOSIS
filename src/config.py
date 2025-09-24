"""Project configuration for the KOSIS analytics pipeline skeleton."""

from __future__ import annotations

import os

KOSIS_API_KEY = os.getenv("KOSIS_API_KEY", "")
TIMEOUT = 30
MAX_RETRIES = 4
RATE_SLEEP = 0.35  # seconds between API calls

# Endpoint definitions aligned with the KOSIS OpenAPI guide.
URL_LIST = "https://kosis.kr/openapi/statisticsList.do"
URL_DATA = "https://kosis.kr/openapi/statisticsData.do"
URL_PARAM = "https://kosis.kr/openapi/Param/statisticsParameterData.do"

DB_PATH = os.getenv("KOSIS_DB", "kosis.duckdb")
RAW_DIR = "data/raw"
