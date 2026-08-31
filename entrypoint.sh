#!/bin/bash
set -e

until python -c "import urllib.request; urllib.request.urlopen('http://qdrant:6333/readyz', timeout=2)" > /dev/null 2>&1; do
  echo "Waiting for Qdrant..."
  sleep 2
done

echo "Qdrant is ready. Starting Python embedding & consumer service..."
exec python app/main.py