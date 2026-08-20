FROM node:24-slim AS frontend-builder
ARG APP_VERSION=dev
ENV VITE_APP_VERSION=$APP_VERSION
WORKDIR /build

COPY frontend/package*.json ./
RUN npm ci
COPY frontend ./
COPY claude-plugin/skills/icegraph/SKILL.md /claude-plugin/skills/icegraph/SKILL.md
RUN npm run build

FROM python:3.12-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app
COPY backend/pyproject.toml backend/uv.lock ./

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

FROM python:3.12-slim
WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"

COPY backend ./backend
COPY images ./images
COPY LICENSE LICENSE-MIT ./

ENV PRODUCTION_MODE=true

EXPOSE 5050

COPY --from=frontend-builder /dist ./backend/static

CMD ["python", "backend/main.py"]
