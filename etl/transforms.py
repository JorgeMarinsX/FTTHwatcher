"""
FTTH Watcher — polars helpers and format-specific transformations
"""

from pathlib import Path

import polars as pl

from schema import ACESSOS_COLS, RENAME


def _strip_bom(df: pl.DataFrame) -> pl.DataFrame:
    first = df.columns[0]
    if first.startswith("\ufeff"):
        df = df.rename({first: first.lstrip("\ufeff")})
    return df


def read_batched(path: Path, batch_size: int):
    """
    Yield polars DataFrames of up to batch_size rows.
    All columns read as strings (infer_schema_length=0) for safety —
    CNPJ and IBGE codes would be corrupted by integer inference.
    """
    reader = pl.read_csv_batched(
        path,
        separator=";",
        encoding="utf8",
        infer_schema_length=0,
        batch_size=batch_size,
        ignore_errors=True,
    )
    first_batch = True
    while True:
        batches = reader.next_batches(1)
        if not batches:
            break
        df = batches[0]
        if first_batch:
            df = _strip_bom(df)
            first_batch = False
        yield df


def _ensure_cols(df: pl.DataFrame, cols: dict[str, pl.DataType]) -> pl.DataFrame:
    """Add columns as null if not present (handles format differences)."""
    for name, dtype in cols.items():
        if name not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=dtype).alias(name))
    return df


def _finalize(df: pl.DataFrame, fonte: str) -> pl.DataFrame:
    """
    Apply all type casts and normalizations after renaming.
    Expects internal column names (post-RENAME).
    """
    df = _ensure_cols(df, {
        "grupo_economico":  pl.Utf8,
        "porte":            pl.Utf8,
        "faixa_velocidade": pl.Utf8,
        "velocidade_s":     pl.Utf8,
        "tecnologia":       pl.Utf8,
        "meio_acesso":      pl.Utf8,
        "tipo_pessoa":      pl.Utf8,
        "tipo_produto":     pl.Utf8,
        "ibge_s":           pl.Utf8,
    })

    df = df.with_columns([
        pl.col("ano").cast(pl.Int16),
        pl.col("mes").cast(pl.Int16),
        pl.col("acessos_s").cast(pl.Int32, strict=False).alias("acessos"),

        # IBGE: cast to int, null if not a valid number
        pl.col("ibge_s").str.strip_chars().cast(pl.Int32, strict=False).alias("ibge"),

        # Velocidade: decimal comma → decimal point
        pl.col("velocidade_s")
          .str.replace(",", ".", literal=True)
          .cast(pl.Float64, strict=False)
          .alias("velocidade_mbps"),

        # CNPJ: strip all non-digits
        pl.col("cnpj").str.replace_all(r"\D", "", literal=False),

        # Empty string → null for optional text columns
        pl.col("grupo_economico").replace("", None),
        pl.col("porte").replace("", None),
        pl.col("faixa_velocidade").replace("", None),
        pl.col("tecnologia").replace("", None),
        pl.col("meio_acesso").replace("", None),
        pl.col("tipo_pessoa").replace("", None),
        pl.col("tipo_produto").replace("", None),

        pl.lit(fonte).alias("fonte"),
    ])

    # Drop rows missing required fields
    df = df.filter(
        pl.col("empresa").is_not_null() & pl.col("empresa").ne("") &
        pl.col("uf").is_not_null()       & pl.col("uf").ne("") &
        pl.col("municipio").is_not_null() & pl.col("municipio").ne("") &
        pl.col("cnpj").str.len_chars().gt(0) &
        pl.col("acessos").is_not_null()
    )

    return df.select(ACESSOS_COLS)


def transform_long(df: pl.DataFrame, fonte: str) -> pl.DataFrame:
    """Long-format batch: rename headers, finalize."""
    rename = {k: v for k, v in RENAME.items() if k in df.columns}
    df = df.rename(rename)
    return _finalize(df, fonte)


def transform_wide(df: pl.DataFrame, date_cols: list[str], fonte: str) -> pl.DataFrame:
    """
    Wide/pivoted batch: unpivot date columns then finalize.
    Each input row expands to up to len(date_cols) output rows.
    Nulls and zeros are dropped (sparse data by design).
    """
    fixed = [c for c in df.columns if c not in date_cols]

    df = df.unpivot(
        on=date_cols,
        index=fixed,
        variable_name="periodo",
        value_name="acessos_s",
    )

    # Drop null/empty/zero access values (wide files are sparse)
    df = df.filter(
        pl.col("acessos_s").is_not_null() &
        pl.col("acessos_s").ne("") &
        pl.col("acessos_s").ne("0")
    )

    if df.is_empty():
        return df

    # Extract ano/mes from the period column header (e.g. "2007-03")
    df = df.with_columns([
        pl.col("periodo").str.slice(0, 4).cast(pl.Int16).alias("ano"),
        pl.col("periodo").str.slice(5, 2).cast(pl.Int16).alias("mes"),
    ])

    rename = {k: v for k, v in RENAME.items() if k in df.columns and v not in ("ano", "mes")}
    df = df.rename(rename)
    return _finalize(df, fonte)
