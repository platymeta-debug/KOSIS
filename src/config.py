# src/config.py  (수정본)  ← 반드시 이 줄보다 먼저는 공백/주석/문자열만 허용
from __future__ import annotations

import os

# (선택) .env 자동 로드: python-dotenv가 설치되어 있으면 사용
try:
    from dotenv import load_dotenv  # pip install python-dotenv==1.0.1
    load_dotenv()
except Exception:
    pass

# -------- 환경변수 --------
KOSIS_API_KEY = os.getenv("KOSIS_API_KEY", "")
DB_PATH       = os.getenv("KOSIS_DB", "kosis.duckdb")

# -------- 호출 설정 --------
TIMEOUT     = 20
MAX_RETRIES = 3
RATE_SLEEP  = 0.35  # 초/호출 (429 뜨면 0.6으로)

# -------- KOSIS 엔드포인트 --------
URL_LIST  = "https://kosis.kr/openapi/statisticsList.do"                  # 목록
URL_DATA  = "https://kosis.kr/openapi/statisticsData.do"                  # 자료(등록형)
URL_PARAM = "https://kosis.kr/openapi/Param/statisticsParameterData.do"   # 자료(통계표선택형)
URL_META  = "https://kosis.kr/openapi/statisticsData.do"                  # 메타(type=TBL)
