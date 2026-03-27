FROM node:20-slim AS frontend-builder
WORKDIR /build

COPY frontend/package*.json ./
RUN npm ci
COPY frontend ./
RUN npm run build

FROM python:3.9-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app
COPY backend/pyproject.toml backend/uv.lock ./

ENV UV_COMPILE_BYTECODE=1 
RUN uv sync --frozen --no-dev --no-install-project

FROM python:3.9-slim
WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"

COPY backend ./backend
COPY images ./images

EXPOSE 5000

COPY --from=frontend-builder /dist ./backend/static

CMD ["python", "backend/main.py"]
