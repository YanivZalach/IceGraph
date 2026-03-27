# Stage 1: Build the React frontend
FROM node:20-slim AS frontend-builder
WORKDIR /build
# Install deps first for better layer caching
COPY icegraph/frontend/package*.json ./
RUN npm ci
COPY icegraph/frontend ./
# vite outDir is '../static/react' relative to frontend/, so output lands at /static/react
RUN npm run build

# Stage 2: Build the virtual environment using uv
FROM python:3.9-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app
COPY pyproject.toml uv.lock ./

ENV UV_COMPILE_BYTECODE=1 

# Create virtual environment and install dependencies without copying the application yet
RUN uv sync --frozen --no-dev --no-install-project

# Stage 2: Minimal runtime image
FROM python:3.9-slim

WORKDIR /app

# Copy the pre-built virtual environment from the builder stage
COPY --from=builder /app/.venv /app/.venv

# Add virtual environment to PATH
ENV PATH="/app/.venv/bin:$PATH"

# Copy application files
COPY pyproject.toml ./
COPY icegraph ./icegraph
COPY images ./images

EXPOSE 5000

# Inject the pre-built React bundle (compiled in Stage 1)
COPY --from=frontend-builder /static/react ./icegraph/static/react

# Run the application
CMD ["python", "icegraph/main.py"]
