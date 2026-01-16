FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock ./

RUN uv pip install --system --no-cache .

COPY services/common /app/services/common
COPY services/inference-api/main.py /app/main.py
COPY services/inference-api/features.py /app/features.py
COPY services/inference-api/derive.py /app/derive.py

ENV PYTHONPATH=/app

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]