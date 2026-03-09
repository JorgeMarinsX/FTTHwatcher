"""
FTTH Watcher — Configuration

Environment variables, connection settings, and pipeline constants.
"""

import logging
import os
import re
from pathlib import Path

RAW = Path(os.getenv("RAW_DATA_DIR", "/data/raw/acessos_banda_larga_fixa"))

# TCP keepalives prevent the connection from being silently dropped by the
# network or a firewall during multi-hour ETL runs.
DSN = " ".join([
    f"host={os.getenv('POSTGRES_HOST', 'localhost')}",
    f"port={os.getenv('POSTGRES_PORT', '5432')}",
    f"dbname={os.getenv('POSTGRES_DB', 'anatel')}",
    f"user={os.getenv('POSTGRES_USER', 'anatel')}",
    f"password={os.getenv('POSTGRES_PASSWORD', 'changeme')}",
    "keepalives=1",
    "keepalives_idle=60",
    "keepalives_interval=10",
    "keepalives_count=5",
])

# Long-format files: each row stays one row.
# Wide-format files: each row expands to N rows (one per date column).
# Keep wide batches smaller so post-unpivot size stays reasonable.
LONG_BATCH = 100_000
WIDE_BATCH =  10_000

# Seconds between connection attempts on startup (postgres may not be ready yet).
CONNECT_RETRY_DELAY = 5
CONNECT_MAX_RETRIES = 12  # up to 1 minute of waiting

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DATE_COL = re.compile(r"^\d{4}-\d{2}$")
