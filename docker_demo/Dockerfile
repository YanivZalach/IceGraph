# syntax=docker/dockerfile:1
FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system \
    "pyspark[connect]==3.5.4" \
    "pyarrow==23.0.1" \
    "pandas==2.3.3" \
    "numpy==1.26.4" \
    "setuptools>=75.0.0"

COPY create_mock_tables.py .

CMD ["python", "create_mock_tables.py"]
