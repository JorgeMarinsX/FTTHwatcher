"""
FTTH Watcher — Database connection and staging operations
"""

import logging
import time
from pathlib import Path

import polars as pl
import psycopg

from config import CONNECT_MAX_RETRIES, CONNECT_RETRY_DELAY, DSN
from schema import ACESSOS_COLS

log = logging.getLogger(__name__)


def connect_with_retry() -> psycopg.Connection:
    """
    Connect to PostgreSQL, retrying on failure.
    Handles the Docker startup race where postgres isn't ready yet.
    """
    for attempt in range(1, CONNECT_MAX_RETRIES + 1):
        try:
            return psycopg.connect(DSN)
        except psycopg.OperationalError as exc:
            if attempt == CONNECT_MAX_RETRIES:
                raise
            log.warning(
                "Connection attempt %d/%d failed: %s — retrying in %ds...",
                attempt, CONNECT_MAX_RETRIES, exc, CONNECT_RETRY_DELAY,
            )
            time.sleep(CONNECT_RETRY_DELAY)


def is_file_current(conn, path: Path) -> bool:
    """Return True if path has already been loaded with its current mtime."""
    mtime = path.stat().st_mtime
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM etl_files WHERE filename = %s AND file_mtime = %s",
            (path.name, mtime),
        )
        return cur.fetchone() is not None


def record_file_load(conn, path: Path, rows_inserted: int) -> None:
    """Upsert a record in etl_files after a successful load."""
    mtime = path.stat().st_mtime
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etl_files (filename, file_mtime, rows_inserted)
            VALUES (%s, %s, %s)
            ON CONFLICT (filename) DO UPDATE
                SET file_mtime    = EXCLUDED.file_mtime,
                    loaded_at     = now(),
                    rows_inserted = EXCLUDED.rows_inserted
            """,
            (path.name, mtime, rows_inserted),
        )
    conn.commit()


def _copy_to_staging(cur, df: pl.DataFrame) -> int:
    """Stream a polars DataFrame into _staging via COPY. Returns row count."""
    cols = ", ".join(ACESSOS_COLS)
    n = 0
    with cur.copy(f"COPY _staging ({cols}) FROM STDIN") as copy:
        for row in df.iter_rows():
            copy.write_row(row)
            n += 1
    return n


def _flush_staging(cur) -> int:
    """INSERT _staging → acessos ON CONFLICT DO NOTHING. Returns inserted count."""
    cols = ", ".join(ACESSOS_COLS)
    cur.execute(f"""
        WITH ins AS (
            INSERT INTO acessos ({cols})
            SELECT {cols} FROM _staging
            ON CONFLICT DO NOTHING
            RETURNING 1
        )
        SELECT COUNT(*) FROM ins
    """)
    return cur.fetchone()[0]


def load_batch(conn, df: pl.DataFrame) -> tuple[int, int]:
    """TRUNCATE staging → COPY batch → INSERT → commit. Returns (staged, inserted)."""
    with conn.cursor() as cur:
        cur.execute("TRUNCATE _staging")
        staged = _copy_to_staging(cur, df)
        inserted = _flush_staging(cur)
    conn.commit()
    return staged, inserted
