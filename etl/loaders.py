"""
FTTH Watcher — File-level loaders for each data source
"""

import logging
from pathlib import Path

import polars as pl
from tqdm import tqdm

from config import DATE_COL, LONG_BATCH, WIDE_BATCH
from db import is_file_current, load_batch, record_file_load
from transforms import _strip_bom, read_batched, transform_long, transform_wide

log = logging.getLogger(__name__)


def discover_access_files(raw: Path) -> list[Path]:
    """
    Return all access data files under raw/ in deterministic load order:
    main files (sorted by name) followed by _Colunas files (sorted by name).
    The Total file is excluded — it is loaded separately via load_totais.
    """
    all_files     = sorted(raw.glob("Acessos_Banda_Larga_Fixa_*.csv"))
    main_files    = [f for f in all_files if "_Colunas" not in f.name and "_Total" not in f.name]
    colunas_files = [f for f in all_files if "_Colunas" in f.name]
    return main_files + colunas_files


def load_access_file(conn, path: Path, position: int = 1) -> None:
    """
    Auto-detect long vs wide format, process in batches, stream into acessos.
    Skips the file if it hasn't changed since the last successful load.
    position — tqdm nesting position for the inner progress bar (default 1).
    """
    if is_file_current(conn, path):
        log.info("%s — unchanged, skipping.", path.name)
        return

    fonte      = path.name
    total_staged = total_inserted = 0

    # Peek at the header to choose format and batch size
    with path.open(encoding="utf-8-sig") as f:
        raw_header = f.readline().rstrip("\n")
    headers   = [h.lstrip("\ufeff") for h in raw_header.split(";")]
    date_cols = [h for h in headers if DATE_COL.match(h)]
    is_wide   = bool(date_cols)
    batch_size = WIDE_BATCH if is_wide else LONG_BATCH

    log.info("%s — format=%s  size=%.1f MB",
             fonte, "wide" if is_wide else "long", path.stat().st_size / 1_048_576)

    with tqdm(
        desc=fonte,
        unit=" rows",
        unit_scale=True,
        dynamic_ncols=True,
        position=position,
        leave=False,
    ) as bar:
        for df in read_batched(path, batch_size):
            df = transform_wide(df, date_cols, fonte) if is_wide else transform_long(df, fonte)

            if df.is_empty():
                continue

            staged, inserted  = load_batch(conn, df)
            total_staged     += staged
            total_inserted   += inserted
            bar.update(staged)

    log.info("%s — done. staged=%d  inserted=%d  discarded=%d",
             fonte, total_staged, total_inserted, total_staged - total_inserted)

    record_file_load(conn, path, total_inserted)


def load_totais(conn, path: Path) -> None:
    if is_file_current(conn, path):
        log.info("totais — unchanged, skipping.")
        return

    df = pl.read_csv(path, separator=";", encoding="utf8", infer_schema_length=0)
    df = _strip_bom(df)
    df = df.rename({"Ano": "ano", "Mês": "mes", "Acessos": "acessos"})
    df = df.with_columns([
        pl.col("ano").cast(pl.Int16),
        pl.col("mes").cast(pl.Int16),
        pl.col("acessos").cast(pl.Int64),
    ])
    rows = df.rows()
    with conn.cursor() as cur:
        cur.execute("TRUNCATE totais")
        cur.executemany(
            "INSERT INTO totais (ano, mes, acessos) VALUES (%s, %s, %s)",
            rows,
        )
    conn.commit()
    log.info("totais — %d rows loaded.", len(rows))
    record_file_load(conn, path, len(rows))


def load_densidades(conn, path: Path) -> None:
    if is_file_current(conn, path):
        log.info("densidades — unchanged, skipping.")
        return

    # Header: Ano;Mês;UF;Município;Código IBGE;Densidade;Nível Geográfico Densidade
    df = pl.read_csv(path, separator=";", encoding="utf8", infer_schema_length=0)
    df = _strip_bom(df)
    df = df.rename({
        "Ano":                         "ano",
        "Mês":                         "mes",
        "UF":                          "uf",
        "Município":                   "municipio",
        "Código IBGE":                 "ibge_s",
        "Densidade":                   "densidade_s",
        "Nível Geográfico Densidade":  "nivel",
    })
    df = df.with_columns([
        pl.col("ano").cast(pl.Int16),
        pl.col("mes").cast(pl.Int16),
        pl.col("uf").replace("", None),
        pl.col("municipio").replace("", None),
        pl.col("ibge_s").str.strip_chars().cast(pl.Int32, strict=False).alias("ibge"),
        pl.col("densidade_s")
          .str.replace(",", ".", literal=True)
          .cast(pl.Float64, strict=False)
          .alias("densidade"),
        pl.col("nivel").replace("", None),
    ]).select(["ano", "mes", "uf", "municipio", "ibge", "densidade", "nivel"])

    with conn.cursor() as cur:
        cur.execute("TRUNCATE densidades")
        with cur.copy(
            "COPY densidades (ano, mes, uf, municipio, ibge, densidade, nivel)"
            " FROM STDIN"
        ) as copy:
            for row in df.iter_rows():
                copy.write_row(row)
    conn.commit()
    log.info("densidades — %d rows loaded.", len(df))
    record_file_load(conn, path, len(df))
