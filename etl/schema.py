"""
FTTH Watcher — DDL e mapeamentos de colunas
"""

DDL_ETL_FILES = """
CREATE TABLE IF NOT EXISTS etl_files (
    filename      TEXT        PRIMARY KEY,
    file_mtime    DOUBLE PRECISION NOT NULL,
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    rows_inserted BIGINT      NOT NULL
);
"""

DDL_TABLES = """
CREATE TABLE IF NOT EXISTS acessos (
    id               BIGSERIAL PRIMARY KEY,
    ano              SMALLINT      NOT NULL,
    mes              SMALLINT      NOT NULL,
    grupo_economico  TEXT,
    empresa          TEXT          NOT NULL,
    cnpj             CHAR(14)      NOT NULL,
    porte            TEXT,
    uf               CHAR(2)       NOT NULL,
    municipio        TEXT          NOT NULL,
    ibge             INTEGER,
    faixa_velocidade TEXT,
    velocidade_mbps  NUMERIC(18,6),
    tecnologia       TEXT,
    meio_acesso      TEXT,
    tipo_pessoa      TEXT,
    tipo_produto     TEXT,
    acessos          INTEGER       NOT NULL,
    fonte            TEXT
);

-- Amplia velocidade_mbps se foi criada com a precisão antiga NUMERIC(12,6).
ALTER TABLE acessos ALTER COLUMN velocidade_mbps TYPE NUMERIC(18,6);

-- COALESCE na expressão do índice trata NULLs para que todas as linhas participem.
CREATE UNIQUE INDEX IF NOT EXISTS acessos_uq ON acessos (
    ano, mes, cnpj,
    COALESCE(ibge, 0),
    COALESCE(faixa_velocidade, ''),
    COALESCE(tecnologia, ''),
    COALESCE(meio_acesso, '')
);

CREATE TABLE IF NOT EXISTS totais (
    ano     SMALLINT NOT NULL,
    mes     SMALLINT NOT NULL,
    acessos BIGINT   NOT NULL,
    PRIMARY KEY (ano, mes)
);

CREATE TABLE IF NOT EXISTS densidades (
    id        BIGSERIAL PRIMARY KEY,
    ano       SMALLINT      NOT NULL,
    mes       SMALLINT      NOT NULL,
    uf        TEXT,
    municipio TEXT,
    ibge      INTEGER,
    densidade NUMERIC(18,12),
    nivel     TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS densidades_uq ON densidades (
    ano, mes, COALESCE(ibge, 0)
);
"""

# UNLOGGED: sem overhead de WAL nas escritas de staging — adequado para uma tabela temporária de ETL.
DDL_STAGING = """
DROP TABLE IF EXISTS _staging;
CREATE UNLOGGED TABLE _staging (
    ano              SMALLINT,
    mes              SMALLINT,
    grupo_economico  TEXT,
    empresa          TEXT,
    cnpj             TEXT,
    porte            TEXT,
    uf               TEXT,
    municipio        TEXT,
    ibge             INTEGER,
    faixa_velocidade TEXT,
    velocidade_mbps  DOUBLE PRECISION,
    tecnologia       TEXT,
    meio_acesso      TEXT,
    tipo_pessoa      TEXT,
    tipo_produto     TEXT,
    acessos          INTEGER,
    fonte            TEXT
);
"""

DDL_INDEXES = """
CREATE INDEX IF NOT EXISTS acessos_ano_mes    ON acessos (ano, mes);
CREATE INDEX IF NOT EXISTS acessos_uf         ON acessos (uf);
CREATE INDEX IF NOT EXISTS acessos_cnpj       ON acessos (cnpj);
CREATE INDEX IF NOT EXISTS acessos_ibge       ON acessos (ibge);
CREATE INDEX IF NOT EXISTS acessos_tecnologia ON acessos (tecnologia);
"""

ACESSOS_COLS = [
    "ano", "mes", "grupo_economico", "empresa", "cnpj", "porte",
    "uf", "municipio", "ibge", "faixa_velocidade", "velocidade_mbps",
    "tecnologia", "meio_acesso", "tipo_pessoa", "tipo_produto",
    "acessos", "fonte",
]

# Cabeçalho CSV → nome interno. Cobre variantes long e wide.
# Apenas as colunas presentes são renomeadas.
RENAME = {
    "Ano":                    "ano",
    "Mês":                    "mes",
    "Grupo Econômico":        "grupo_economico",
    "Empresa":                "empresa",
    "CNPJ":                   "cnpj",
    "Porte da Prestadora":    "porte",
    "UF":                     "uf",
    "Município":              "municipio",
    "Código IBGE Município":  "ibge_s",
    "Faixa de Velocidade":    "faixa_velocidade",
    "Velocidade":             "velocidade_s",
    "Tecnologia":             "tecnologia",
    "Meio de Acesso":         "meio_acesso",
    "Tipo de Pessoa":         "tipo_pessoa",
    "Tipo de Produto":        "tipo_produto",
    "Acessos":                "acessos_s",
}
