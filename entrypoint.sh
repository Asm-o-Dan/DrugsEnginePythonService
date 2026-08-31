#!/bin/bash
# Запускаем оба скрипта в фоновом режиме
python main.py &
until python -c "import urllib.request; urllib.request.urlopen('http://qdrant:6333/readyz', timeout=2)" > /dev/null 2>&1; do
  echo "Waiting for Qdrant..."
  sleep 2
done
python app/main.py &


# Ждём завершения любого из процессов
wait -n
exit $?