"""
FTTH Watcher — Conexão com o banco de dados e operações de staging
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
    Conecta ao PostgreSQL, repetindo em caso de falha.
    Trata a condição de corrida na inicialização do Docker quando o postgres ainda não está pronto.
    """
    for attempt in range(1, CONNECT_MAX_RETRIES + 1):
        try:
            return psycopg.connect(DSN)
        except psycopg.OperationalError as exc:
            if attempt == CONNECT_MAX_RETRIES:
                raise
            log.warning(
                "Tentativa de conexão %d/%d falhou: %s — tentando novamente em %ds...",
                attempt, CONNECT_MAX_RETRIES, exc, CONNECT_RETRY_DELAY,
            )
            time.sleep(CONNECT_RETRY_DELAY)


def is_file_current(conn, path: Path) -> bool:
    """Retorna True se o arquivo já foi carregado com o mtime atual."""
    mtime = path.stat().st_mtime
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM etl_files WHERE filename = %s AND file_mtime = %s",
            (path.name, mtime),
        )
        return cur.fetchone() is not None


def record_file_load(conn, path: Path, rows_inserted: int) -> None:
    """Registra (upsert) em etl_files após um carregamento bem-sucedido."""
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
    """
    Carrega em massa um DataFrame polars em _staging via COPY CSV.

    polars.write_csv() serializa o lote inteiro em Rust de uma vez,
    eliminando o loop Python de 100k iterações que era o principal gargalo.
    Uma única chamada copy.write() envia todo o buffer ao PostgreSQL.

    Tratamento de NULL: _finalize() já substitui todas as strings vazias por None,
    então null_value="" é seguro — qualquer campo vazio na saída é um NULL genuíno.
    """
    cols = ", ".join(ACESSOS_COLS)
    csv_data = df.write_csv(separator=",", null_value="", include_header=False)
    with cur.copy(f"COPY _staging ({cols}) FROM STDIN (FORMAT CSV, NULL '')") as copy:
        copy.write(csv_data)
    return len(df)


def _flush_staging(cur) -> int:
    """INSERT _staging → acessos ON CONFLICT DO NOTHING. Retorna a contagem de linhas inseridas."""
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
    """TRUNCATE staging → COPY batch → INSERT → commit. Retorna (staged, inserted)."""
    with conn.cursor() as cur:
        cur.execute("TRUNCATE _staging")
        staged = _copy_to_staging(cur, df)
        inserted = _flush_staging(cur)
    conn.commit()
    return staged, inserted
