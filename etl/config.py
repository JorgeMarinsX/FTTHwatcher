"""
FTTH Watcher — Configuração

Variáveis de ambiente, configurações de conexão e constantes do pipeline.
"""

import logging
import os
import re
import time
from pathlib import Path

RAW = Path(os.getenv("RAW_DATA_DIR", "/data/raw/acessos_banda_larga_fixa"))

ANATEL_ZIP_URL = os.getenv(
    "ANATEL_ZIP_URL",
    "https://www.anatel.gov.br/dadosabertos/paineis_de_dados/acessos/acessos_banda_larga_fixa.zip",
)

# Define como "false" para pular o download automático (útil quando os arquivos já estão montados).
DOWNLOAD_ENABLED = os.getenv("DOWNLOAD_DATA", "true").strip().lower() not in ("false", "0", "no")

# TCP keepalives evitam que a conexão seja encerrada silenciosamente pela
# rede ou firewall durante execuções do ETL de várias horas.
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

# Arquivos no formato longo: cada linha permanece como uma linha.
# Arquivos no formato largo: cada linha se expande para N linhas (uma por coluna de data).
# Lotes do formato largo são menores para que o tamanho após o unpivot fique razoável.
LONG_BATCH = 100_000
WIDE_BATCH =  10_000

# Segundos entre tentativas de conexão na inicialização (postgres pode não estar pronto ainda).
CONNECT_RETRY_DELAY = 5
CONNECT_MAX_RETRIES = 12  # até 1 minuto de espera

logging.Formatter.converter = time.localtime  # usa horário local em vez de UTC
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DATE_COL = re.compile(r"^\d{4}-\d{2}$")
