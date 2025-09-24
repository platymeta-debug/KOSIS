import os

# KOSIS
KOSIS_API_KEY = os.environ.get("KOSIS_API_KEY", "")
KOSIS_BASE_URL = "https://kosis.kr/openapi/statisticsData.do"
KOSIS_URL_DATA  = "https://kosis.kr/openapi/statisticsData.do"                 # userStatsId
KOSIS_URL_PARAM = "https://kosis.kr/openapi/Param/statisticsParameterData.do"  # orgId/tblId + objL*/itmId
KOSIS_URL_META  = "https://kosis.kr/openapi/statisticsData.do"                 # method=getMeta&type=TBL
KOSIS_URL_BIG   = "https://kosis.kr/openapi/statisticsBigData.do"              # 대용량(등록형)
KOSIS_URL_LIST  = "https://kosis.kr/openapi/statisticsList.do"                 # 목록
KOSIS_URL_SEARCH= "https://kosis.kr/openapi/statisticsSearch.do"               # 통합검색
KOSIS_TIMEOUT = int(os.environ.get("KOSIS_TIMEOUT", "30"))
KOSIS_MAX_WORKERS = int(os.environ.get("KOSIS_MAX_WORKERS", "8"))
KOSIS_RATE_SLEEP = float(os.environ.get("KOSIS_RATE_SLEEP", "0.35"))  # 초/호출

# 저장/DB
DB_PATH = os.environ.get("KOSIS_DB", "kosis.duckdb")
RAW_CACHE_DIR = os.environ.get("KOSIS_RAW_DIR", "data/raw")

# 표준 분석 주기
TARGET_FREQ = os.environ.get("TARGET_FREQ", "Q")  # 'Q' 권장
