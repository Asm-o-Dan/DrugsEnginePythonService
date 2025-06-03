#!/bin/bash
# Запускаем оба скрипта в фоновом режиме
python main.py &
until curl -s http://qdrant:6333/readyz; do
  echo "Waiting for Qdrant..."
  sleep 2
done
python app/main.py &


# Ждём завершения любого из процессов
wait -n
exit $?