import os

# Timezone, etc.
TIMEZONE = os.environ.get("TZ", "Asia/Seoul")

# KOSIS
KOSIS_API_KEY = os.environ.get("KOSIS_API_KEY", "")
KOSIS_BASE_URL = "https://kosis.kr/openapi/statisticsData.do"  # Placeholder

# Default params
ROLLING_WINDOW = 60  # months for rolling correlations (or quarters)
FREQ = "Q"  # default frequency
