#!/bin/bash
set -e
# Ждём Qdrant (без curl: проверка TCP-порта средствами bash)
until (echo > /dev/tcp/${QDRANT_HOST:-qdrant}/${QDRANT_PORT:-6333}) 2>/dev/null; do
  echo "Waiting for Qdrant..."
  sleep 2
done
exec python -m app.main
