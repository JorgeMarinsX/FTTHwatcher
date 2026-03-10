FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV UV_PROJECT_ENVIRONMENT=/venv

COPY pyproject.toml ./
RUN uv sync --no-dev --no-install-project

COPY *.py ./

CMD ["/venv/bin/python", "main.py"]
