"""
FTTH Watcher — ANATEL Fixed Broadband ETL

Streams all raw ANATEL CSVs into PostgreSQL using polars for fast,
memory-efficient batch processing. Memory usage is bounded to ~batch_size
rows at any point regardless of file size.

File formats handled:
  Long (main files):    Ano;Mês;...;Acessos  — already tidy, two schema variants
  Wide (_Colunas files): fixed attrs + YYYY-MM date cols — unpivoted via polars

NOTE: _Colunas files contain access data in wide/pivoted layout, not column
metadata as the file name implies. Both are loaded into acessos; the unique
index handles any overlap between them via ON CONFLICT DO NOTHING.
"""

import logging
import os
import sys

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from config import RAW
from db import connect_with_retry
from loaders import discover_access_files, load_access_file, load_densidades, load_totais
from schema import DDL_ETL_FILES, DDL_INDEXES, DDL_STAGING, DDL_TABLES

log = logging.getLogger(__name__)


def main() -> None:
    log.info("Connecting to PostgreSQL at %s...", os.getenv("POSTGRES_HOST", "localhost"))
    conn = connect_with_retry()

    try:
        # Schema setup — all statements are idempotent (IF NOT EXISTS)
        with conn.cursor() as cur:
            cur.execute(DDL_ETL_FILES)
            cur.execute(DDL_TABLES)
            cur.execute(DDL_STAGING)
        conn.commit()

        # Single-source tables: skip if file unchanged, TRUNCATE+reload if updated
        load_totais(conn, RAW / "Acessos_Banda_Larga_Fixa_Total.csv")
        load_densidades(conn, RAW / "Densidade_Banda_Larga_Fixa.csv")

        # Access files: discovered dynamically — no hardcoded names or years.
        # Each file is committed in batches; etl_files records completion only on
        # success, so a crash mid-file causes a clean re-run (ON CONFLICT DO NOTHING
        # absorbs already-inserted rows).
        files  = discover_access_files(RAW)
        failed = []

        with logging_redirect_tqdm():
            with tqdm(files, desc="files", unit="file", position=0, dynamic_ncols=True) as file_bar:
                for i, path in enumerate(file_bar, 1):
                    file_bar.set_description(f"[{i}/{len(files)}] {path.name}")
                    try:
                        load_access_file(conn, path, position=1)
                    except Exception:
                        conn.rollback()
                        log.exception("[%d/%d] %s — failed, continuing.", i, len(files), path.name)
                        failed.append(path.name)

        # Post-load indexes — CREATE INDEX IF NOT EXISTS is a no-op after first run
        with conn.cursor() as cur:
            cur.execute(DDL_INDEXES)
        conn.commit()

    finally:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS _staging")
        conn.commit()
        conn.close()

    if failed:
        log.error("%d file(s) failed: %s", len(failed), ", ".join(failed))
        sys.exit(1)

    log.info("ETL complete.")


if __name__ == "__main__":
    main()
