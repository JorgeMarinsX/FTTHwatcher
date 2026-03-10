"""
FTTH Watcher — utilitários polars e transformações específicas por formato
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
    Gera DataFrames polars com até batch_size linhas.
    Todas as colunas são lidas como strings (infer_schema_length=0) por segurança —
    códigos de CNPJ e IBGE seriam corrompidos pela inferência de inteiros.
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
    """Adiciona colunas como nulas se não estiverem presentes (trata diferenças de formato)."""
    for name, dtype in cols.items():
        if name not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=dtype).alias(name))
    return df


def _finalize(df: pl.DataFrame, fonte: str) -> pl.DataFrame:
    """
    Aplica todos os casts de tipo e normalizações após a renomeação.
    Espera nomes internos de colunas (pós-RENAME).
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

        # IBGE: converte para int, nulo se não for um número válido
        pl.col("ibge_s").str.strip_chars().cast(pl.Int32, strict=False).alias("ibge"),

        # Velocidade: vírgula decimal → ponto decimal
        pl.col("velocidade_s")
          .str.replace(",", ".", literal=True)
          .cast(pl.Float64, strict=False)
          .alias("velocidade_mbps"),

        # CNPJ: remove todos os não-dígitos
        pl.col("cnpj").str.replace_all(r"\D", "", literal=False),

        # String vazia → nulo para colunas de texto opcionais
        pl.col("grupo_economico").replace("", None),
        pl.col("porte").replace("", None),
        pl.col("faixa_velocidade").replace("", None),
        pl.col("tecnologia").replace("", None),
        pl.col("meio_acesso").replace("", None),
        pl.col("tipo_pessoa").replace("", None),
        pl.col("tipo_produto").replace("", None),

        pl.lit(fonte).alias("fonte"),
    ])

    # Remove linhas com campos obrigatórios ausentes
    df = df.filter(
        pl.col("empresa").is_not_null() & pl.col("empresa").ne("") &
        pl.col("uf").is_not_null()       & pl.col("uf").ne("") &
        pl.col("municipio").is_not_null() & pl.col("municipio").ne("") &
        pl.col("cnpj").str.len_chars().gt(0) &
        pl.col("acessos").is_not_null()
    )

    return df.select(ACESSOS_COLS)


def transform_long(df: pl.DataFrame, fonte: str) -> pl.DataFrame:
    """Lote no formato longo: renomeia cabeçalhos e finaliza."""
    rename = {k: v for k, v in RENAME.items() if k in df.columns}
    df = df.rename(rename)
    return _finalize(df, fonte)


def transform_wide(df: pl.DataFrame, date_cols: list[str], fonte: str) -> pl.DataFrame:
    """
    Lote no formato largo/pivotado: desagrega colunas de data e finaliza.
    Cada linha de entrada se expande para até len(date_cols) linhas de saída.
    Nulos e zeros são descartados (dados esparsos por design).
    """
    fixed = [c for c in df.columns if c not in date_cols]

    df = df.unpivot(
        on=date_cols,
        index=fixed,
        variable_name="periodo",
        value_name="acessos_s",
    )

    # Descarta valores de acesso nulos/vazios/zero (arquivos largos são esparsos)
    df = df.filter(
        pl.col("acessos_s").is_not_null() &
        pl.col("acessos_s").ne("") &
        pl.col("acessos_s").ne("0")
    )

    if df.is_empty():
        return df

    # Extrai ano/mes do cabeçalho da coluna de período (ex.: "2007-03")
    df = df.with_columns([
        pl.col("periodo").str.slice(0, 4).cast(pl.Int16).alias("ano"),
        pl.col("periodo").str.slice(5, 2).cast(pl.Int16).alias("mes"),
    ])

    rename = {k: v for k, v in RENAME.items() if k in df.columns and v not in ("ano", "mes")}
    df = df.rename(rename)
    return _finalize(df, fonte)
