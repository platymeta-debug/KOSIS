"""Lightweight configuration module for the standalone KOSIS helpers."""

from __future__ import annotations
from dotenv import load_dotenv
load_dotenv()  # 프로젝트 루트 .env 자동 로드

KOSIS_API_KEY = os.getenv("KOSIS_API_KEY", "")

import os

# NOTE: ``KOSIS_API_KEY`` *must* be configured by the caller.  The scripts keep the
# empty-string default so the configuration module can be imported safely during
# testing, but network calls will fail until the environment variable is set.
KOSIS_API_KEY = os.getenv("KOSIS_API_KEY", "")

TIMEOUT = 30
MAX_RETRIES = 4
RATE_SLEEP = 0.35  # seconds between API calls

# Endpoint definitions aligned with the official KOSIS OpenAPI guide.
URL_LIST = "https://kosis.kr/openapi/statisticsList.do"
URL_DATA = "https://kosis.kr/openapi/statisticsData.do"
URL_PARAM = "https://kosis.kr/openapi/Param/statisticsParameterData.do"
URL_META = "https://kosis.kr/openapi/statisticsData.do"

# Local DuckDB path used for staging normalised observations across the pipeline.
DB_PATH = os.getenv("KOSIS_DB_PATH", "kosis.duckdb")
