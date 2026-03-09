# 📡 FTTH Watcher

> Pipeline ETL para ingestão dos dados públicos de **Banda Larga Fixa** da ANATEL no PostgreSQL.

---

## 📋 Visão Geral

O **FTTH Watcher** processa os arquivos CSV disponibilizados pela ANATEL com dados históricos de acessos de banda larga fixa no Brasil (2007–presente), normalizando dois formatos distintos de arquivo e carregando tudo em um banco PostgreSQL estruturado e consultável.

Os dados cobrem **todas as prestadoras cadastradas**, por município, UF, tecnologia, faixa de velocidade e período — permitindo análises históricas granulares do mercado de telecomunicações brasileiro.

---

## 🗂️ Dados Carregados

| Tabela | Descrição |
|---|---|
| `acessos` | Registros individuais de acesso por prestadora, município, tecnologia e período |
| `totais` | Total consolidado de acessos por ano/mês |
| `densidades` | Densidade de banda larga por município e UF |

### Colunas principais de `acessos`

| Coluna | Tipo | Descrição |
|---|---|---|
| `ano` / `mes` | `SMALLINT` | Período de referência |
| `empresa` | `TEXT` | Nome da prestadora |
| `cnpj` | `CHAR(14)` | CNPJ da prestadora |
| `uf` | `CHAR(2)` | Unidade federativa |
| `municipio` | `TEXT` | Nome do município |
| `ibge` | `INTEGER` | Código IBGE do município |
| `tecnologia` | `TEXT` | Ex.: `FTTB`, `FTTH`, `xDSL`, `cabo` |
| `faixa_velocidade` | `TEXT` | Faixa contratada |
| `velocidade_mbps` | `NUMERIC` | Velocidade em Mbps (arquivos 2021+) |
| `acessos` | `INTEGER` | Quantidade de acessos |

---

## 🏗️ Arquitetura

```
raw/
└── acessos_banda_larga_fixa/
    ├── Acessos_Banda_Larga_Fixa_YYYY-YYYY.csv        ← formato longo (tidy)
    ├── Acessos_Banda_Larga_Fixa_YYYY-YYYY_Colunas.csv ← formato largo (pivotado)
    ├── Acessos_Banda_Larga_Fixa_Total.csv
    └── Densidade_Banda_Larga_Fixa.csv

etl/
├── main.py        ← orquestração principal
├── loaders.py     ← leitura e carga dos arquivos
├── transforms.py  ← normalização e unpivot
├── schema.py      ← DDL das tabelas
├── db.py          ← conexão com retry
└── config.py      ← variáveis de ambiente
```

### Fluxo de processamento

```
CSV (disco)
    │
    ▼
polars read_csv_batched          ← streaming, sem carregar o arquivo inteiro
    │
    ▼
normalização / unpivot           ← formato largo → longo via polars
    │
    ▼
TRUNCATE _staging                ← tabela UNLOGGED (sem WAL overhead)
    │
    ▼
COPY batch → _staging
    │
    ▼
INSERT INTO acessos              ← ON CONFLICT DO NOTHING (deduplicação)
    │
    ▼
COMMIT
```

Após todos os arquivos, os **índices secundários** são criados (`ano_mes`, `uf`, `cnpj`, `ibge`, `tecnologia`).

---

## 📦 Formatos de Arquivo

Os arquivos da ANATEL vêm em dois layouts:

**Formato longo** (`*_YYYY.csv`) — já normalizado:
```
Ano;Mês;Grupo Econômico;Empresa;CNPJ;Porte;UF;Município;IBGE;Faixa;Tecnologia;Meio;Acessos
```

**Formato largo** (`*_Colunas.csv`) — colunas de datas pivotadas:
```
Empresa;CNPJ;UF;Município;...;2021-01;2021-02;2021-03;...
```
> O ETL realiza o `unpivot` automaticamente via `polars`, transformando cada coluna de data em uma linha.

---

## ⚙️ Stack

![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![Polars](https://img.shields.io/badge/Polars-1.x-CD792C?logo=polars&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-compose-2496ED?logo=docker&logoColor=white)
![uv](https://img.shields.io/badge/uv-package_manager-DE5FE9)

| Componente | Tecnologia |
|---|---|
| Linguagem | Python 3.12 |
| Banco de dados | PostgreSQL 16 |
| Processamento | [Polars](https://pola.rs/) — streaming em batches |
| Driver DB | psycopg v3 (binary) |
| Gerenciador de pacotes | [uv](https://github.com/astral-sh/uv) |
| Containers | Docker Compose |

---

## 🔑 Variáveis de Ambiente

Copie `.env.example` para `.env` e ajuste conforme necessário:

| Variável | Padrão | Descrição |
|---|---|---|
| `POSTGRES_HOST` | `postgres` | Host do banco (dentro do compose: nome do serviço) |
| `POSTGRES_PORT` | `5432` | Porta do PostgreSQL |
| `POSTGRES_DB` | `anatel` | Nome do banco |
| `POSTGRES_USER` | `anatel` | Usuário |
| `POSTGRES_PASSWORD` | `changeme` | Senha |

---

## 📊 Fonte dos Dados

Os dados são disponibilizados publicamente pela **ANATEL** (Agência Nacional de Telecomunicações) através do portal de dados abertos:

> [https://dados.anatel.gov.br/](https://dados.anatel.gov.br/)

Série histórica de acessos de **banda larga fixa** desde **2007** até o presente, com atualizações mensais.

---

## 📄 Licença

Dados: domínio público (ANATEL / governo federal brasileiro).
Código: MIT.
