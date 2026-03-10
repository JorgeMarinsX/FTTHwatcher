"""
FTTH Watcher — ETL de Banda Larga Fixa ANATEL

Processa todos os CSVs brutos da ANATEL em lotes para o PostgreSQL usando polars,
com uso de memória limitado a ~batch_size linhas por vez, independente do tamanho do arquivo.

Formatos suportados:
  Longo (arquivos principais): Ano;Mês;...;Acessos  — já no formato tidy, duas variantes de esquema
  Largo (arquivos _Colunas):   atributos fixos + colunas de datas YYYY-MM — desagregado via polars

NOTA: os arquivos _Colunas contêm dados de acesso no formato largo/pivotado, não metadados
de colunas como o nome do arquivo sugere. Ambos são carregados em acessos; o índice único
trata qualquer sobreposição via ON CONFLICT DO NOTHING.
"""

import logging
import os
import sys

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from config import ANATEL_ZIP_URL, DOWNLOAD_ENABLED, RAW
from db import connect_with_retry
from download import ensure_raw_data
from loaders import discover_access_files, load_access_file, load_densidades, load_totais
from schema import DDL_ETL_FILES, DDL_INDEXES, DDL_STAGING, DDL_TABLES

log = logging.getLogger(__name__)


def main() -> None:
    if DOWNLOAD_ENABLED:
        ensure_raw_data(RAW, ANATEL_ZIP_URL)

    log.info("Conectando ao PostgreSQL em %s...", os.getenv("POSTGRES_HOST", "localhost"))
    conn = connect_with_retry()

    try:
        # Configuração do esquema — todos os comandos são idempotentes (IF NOT EXISTS)
        with conn.cursor() as cur:
            cur.execute(DDL_ETL_FILES)
            cur.execute(DDL_TABLES)
            cur.execute(DDL_STAGING)
        conn.commit()

        # Tabelas de fonte única: ignora se o arquivo não mudou, TRUNCATE+recarga se atualizado
        load_totais(conn, RAW / "Acessos_Banda_Larga_Fixa_Total.csv")
        load_densidades(conn, RAW / "Densidade_Banda_Larga_Fixa.csv")

        # Arquivos de acesso: descobertos dinamicamente — sem nomes ou anos fixos.
        # Cada arquivo é commitado em lotes; etl_files registra conclusão apenas em
        # caso de sucesso, então uma falha no meio do arquivo gera reprocessamento
        # limpo (ON CONFLICT DO NOTHING absorve linhas já inseridas).
        files  = discover_access_files(RAW)
        failed = []

        with logging_redirect_tqdm():
            with tqdm(files, desc="arquivos", unit="arquivo", position=0, dynamic_ncols=True) as file_bar:
                for i, path in enumerate(file_bar, 1):
                    file_bar.set_description(f"[{i}/{len(files)}] {path.name}")
                    try:
                        load_access_file(conn, path, position=1)
                    except Exception:
                        conn.rollback()
                        log.exception("[%d/%d] %s — falhou, continuando.", i, len(files), path.name)
                        failed.append(path.name)

        # Índices pós-carga — CREATE INDEX IF NOT EXISTS não faz nada após a primeira execução
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
        log.error("%d arquivo(s) com falha: %s", len(failed), ", ".join(failed))
        sys.exit(1)

    log.info("ETL concluído.")


if __name__ == "__main__":
    main()
