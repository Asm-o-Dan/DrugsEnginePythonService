# Builder stage
FROM python:3.10-slim AS builder
WORKDIR /app

COPY requirements.txt .

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip \
    && pip install --prefer-binary --no-cache-dir -r requirements.txt

# Final stage
FROM python:3.10-slim
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv
COPY . .

RUN chmod +x entrypoint.sh

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH="/app:$PYTHONPATH" \
    PYTHONUNBUFFERED=1

CMD ["./entrypoint.sh"]
