"""
FTTH Watcher — Download dos dados brutos ANATEL

Baixa e extrai o ZIP de dados se o diretório raw estiver vazio.
Invocado automaticamente pelo ETL antes de qualquer processamento.
"""

import logging
import tempfile
import urllib.request
import zipfile
from pathlib import Path

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

log = logging.getLogger(__name__)

_CHUNK = 1 * 1024 * 1024  # 1 MB — granularidade fina para barra fluida


def ensure_raw_data(raw_dir: Path, url: str) -> None:
    """
    Garante que *raw_dir* contém arquivos CSV da ANATEL.

    Se o diretório já tiver arquivos .csv, não faz nada.
    Caso contrário, baixa *url* (um ZIP), extrai no diretório pai de *raw_dir*
    e remove o arquivo temporário.
    """
    if _has_csv_files(raw_dir):
        log.info("Dados brutos já presentes em %s — download ignorado.", raw_dir)
        return

    log.info("Diretório %s vazio. Iniciando download de:\n  %s", raw_dir, url)

    # O ZIP da ANATEL despeja os arquivos diretamente na raiz, sem subdiretório.
    # Extraímos para dentro de raw_dir para manter a estrutura esperada.
    raw_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False, dir=raw_dir) as tmp:
        tmp_path = Path(tmp.name)

    try:
        with logging_redirect_tqdm():
            _download(url, tmp_path)
            _extract(tmp_path, raw_dir)
    finally:
        tmp_path.unlink(missing_ok=True)

    if not _has_csv_files(raw_dir):
        raise RuntimeError(
            f"Extração concluída, mas nenhum CSV encontrado em {raw_dir}."
        )

    log.info("Dados prontos em %s.", raw_dir)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _has_csv_files(directory: Path) -> bool:
    return directory.is_dir() and any(directory.glob("*.csv"))


def _download(url: str, dest: Path) -> None:
    log.info("Conectando a %s …", url)
    with urllib.request.urlopen(url) as resp:
        total = int(resp.headers.get("Content-Length") or 0)

        with (
            tqdm(
                total=total or None,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc="download",
                dynamic_ncols=True,
            ) as bar,
            open(dest, "wb") as fh,
        ):
            while chunk := resp.read(_CHUNK):
                fh.write(chunk)
                bar.update(len(chunk))

    log.info("Download concluído.")


def _extract(zip_path: Path, dest_dir: Path) -> None:
    log.info("Extraindo ZIP …")
    with zipfile.ZipFile(zip_path) as zf:
        entries = zf.infolist()
        total_bytes = sum(e.file_size for e in entries)
        with tqdm(
            total=total_bytes,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc="extração",
            dynamic_ncols=True,
        ) as bar:
            for entry in entries:
                bar.set_postfix_str(entry.filename, refresh=False)
                zf.extract(entry, dest_dir)
                bar.update(entry.file_size)
    log.info("Extração concluída.")
