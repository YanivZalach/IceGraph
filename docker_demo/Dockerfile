FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1

RUN uv pip install --system \
    "arrow==1.4.0" \
    "grpc-status==0.0.1" \
    "grpcio==1.78.0" \
    "numpy<2.0.0" \
    "pandas==2.3.3" \
    "pyarrow>=21.0.0" \
    "pyspark[connect]==3.5.4" \
    "python-dotenv==1.2.1"

COPY create_mock_tables.py .

CMD ["python", "create_mock_tables.py"]