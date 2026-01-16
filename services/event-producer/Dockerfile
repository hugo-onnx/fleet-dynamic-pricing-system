FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ librdkafka-dev && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./

RUN uv pip install --system --no-cache .

COPY services/common /app/services/common
COPY services/event-producer/main.py /app/services/event-producer/main.py

ENV PYTHONPATH=/app

CMD ["python", "services/event-producer/main.py"]