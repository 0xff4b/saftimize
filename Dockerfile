FROM ghcr.io/astral-sh/uv:0.12.23 AS uv
FROM python:3.13-slim
COPY --from=uv /uv /uvx /usr/local/bin/
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy DAGSTER_HOME=/var/dagster
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project
COPY src/ingest ./src/ingest
RUN uv sync --locked --no-dev
COPY docker/dagster.yaml /etc/saftimize/dagster.yaml
COPY docker/workspace.yaml /app/workspace.yaml
COPY docker/backfill.example.yaml /app/backfill.example.yaml
RUN useradd --create-home --uid 10001 pipeline && mkdir -p /var/dagster && chown pipeline:pipeline /var/dagster
ENV PATH="/app/.venv/bin:$PATH"
USER pipeline
CMD ["saftimize-ingest", "entsoe"]
