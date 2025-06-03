# Билдер
FROM python:3.10-slim AS builder
WORKDIR /app

COPY requirements.txt .

RUN python -m venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

# Ставим зависимости через кеш
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip \
    && pip install --prefer-binary --no-cache-dir --no-input --progress-bar off -r requirements.txt

# Явно грузим модель
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('sentence-transformers/LaBSE')"

# Финальный образ
FROM python:3.10-slim

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

COPY --from=builder /root/.cache/huggingface/hub /root/.cache/huggingface/hub

COPY . .

RUN chmod +x entrypoint.sh

ENV PYTHONPATH="/app:$PYTHONPATH" \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

CMD ["./entrypoint.sh"]
