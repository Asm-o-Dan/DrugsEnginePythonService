"""
Точка входа сервиса: HTTP API (uvicorn) + потребители Kafka и RabbitMQ в фоновых потоках.
Модель LaBSE загружается один раз и используется всеми компонентами.

Запуск: python -m app.main
"""
import logging
import os
import threading

import uvicorn

from app.api import create_app
from app.config import QDRANT_HOST, QDRANT_PORT
from app.mq.kafka_consumer import start_kafka_consumer
from app.mq.rabbit_consumer import start_rabbit_consumer
from app.services.qdrant_service import QdrantService
from app.services.vectorization_service import VectorizationService

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("Main")


def _run_forever(name: str, target, *args):
    def runner():
        try:
            logger.info("Запуск %s", name)
            target(*args)
        except Exception:
            logger.exception("%s остановлен из-за ошибки", name)

    threading.Thread(target=runner, name=name, daemon=True).start()


def main():
    vector_service = VectorizationService()
    qdrant_service = QdrantService(QDRANT_HOST, QDRANT_PORT)
    qdrant_service.ensure_collection("drug_collection", vector_service.get_vector_size())

    if os.getenv("KAFKA_ENABLED", "true").lower() == "true":
        _run_forever("kafka-consumer", start_kafka_consumer, vector_service, qdrant_service)
    if os.getenv("RABBITMQ_ENABLED", "false").lower() == "true":
        _run_forever("rabbitmq-consumer", start_rabbit_consumer, vector_service, qdrant_service)

    uvicorn.run(
        create_app(vector_service, qdrant_service),
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        log_level=os.getenv("LOG_LEVEL", "info").lower(),
    )


if __name__ == "__main__":
    main()
