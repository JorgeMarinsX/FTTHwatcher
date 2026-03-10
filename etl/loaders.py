"""
FTTH Watcher — Carregadores por arquivo para cada fonte de dados
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
    Retorna todos os arquivos de dados de acesso em raw/ em ordem de carga determinística:
    arquivos principais (ordenados por nome) seguidos dos arquivos _Colunas (ordenados por nome).
    O arquivo Total é excluído — é carregado separadamente via load_totais.
    """
    all_files     = sorted(raw.glob("Acessos_Banda_Larga_Fixa_*.csv"))
    main_files    = [f for f in all_files if "_Colunas" not in f.name and "_Total" not in f.name]
    colunas_files = [f for f in all_files if "_Colunas" in f.name]
    return main_files + colunas_files


def load_access_file(conn, path: Path, position: int = 1) -> None:
    """
    Detecta automaticamente o formato longo ou largo, processa em lotes e transmite para acessos.
    Ignora o arquivo se não houve alteração desde o último carregamento bem-sucedido.
    position — posição de aninhamento do tqdm para a barra de progresso interna (padrão 1).
    """
    if is_file_current(conn, path):
        log.info("%s — sem alterações, ignorando.", path.name)
        return

    fonte      = path.name
    total_staged = total_inserted = 0

    # Lê o cabeçalho para determinar o formato e o tamanho do lote
    with path.open(encoding="utf-8-sig") as f:
        raw_header = f.readline().rstrip("\n")
    headers   = [h.lstrip("\ufeff") for h in raw_header.split(";")]
    date_cols = [h for h in headers if DATE_COL.match(h)]
    is_wide   = bool(date_cols)
    batch_size = WIDE_BATCH if is_wide else LONG_BATCH

    log.info("%s — formato=%s  tamanho=%.1f MB",
             fonte, "largo" if is_wide else "longo", path.stat().st_size / 1_048_576)

    with tqdm(
        desc=fonte,
        unit=" linhas",
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

    log.info("%s — concluído. staged=%d  inseridos=%d  descartados=%d",
             fonte, total_staged, total_inserted, total_staged - total_inserted)

    record_file_load(conn, path, total_inserted)


def load_totais(conn, path: Path) -> None:
    if is_file_current(conn, path):
        log.info("totais — sem alterações, ignorando.")
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
    log.info("totais — %d linhas carregadas.", len(rows))
    record_file_load(conn, path, len(rows))


def load_densidades(conn, path: Path) -> None:
    if is_file_current(conn, path):
        log.info("densidades — sem alterações, ignorando.")
        return

    # Cabeçalho: Ano;Mês;UF;Município;Código IBGE;Densidade;Nível Geográfico Densidade
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
    log.info("densidades — %d linhas carregadas.", len(df))
    record_file_load(conn, path, len(df))
