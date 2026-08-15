# Builder stage
FROM python:3.10-slim AS builder
WORKDIR /app

COPY requirements.txt .

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip \
    && pip install --prefer-binary --no-cache-dir -r requirements.txt

# Загрузка и сохранение модели явно
RUN mkdir -p /app/model && \
    python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('sentence-transformers/LaBSE').save('/app/model')"

# Final stage
FROM python:3.10-slim
WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /app/model /app/model
COPY . .

RUN chmod +x entrypoint.sh

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH="/app:$PYTHONPATH" \
    PYTHONUNBUFFERED=1

CMD ["./entrypoint.sh"]
