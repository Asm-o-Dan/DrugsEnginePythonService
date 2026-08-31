import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import os

# Добавляем родительскую директорию в sys.path при прямом запуске
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from app.services.vectorization_service import VectorizationService
    from app.services.qdrant_service import QdrantService
    from app.config import QDRANT_HOST, QDRANT_PORT
    from app.mq.kafka_consumer import start_kafka_consumer
    from app.mq.rabbit_consumer import start_rabbit_consumer
    from app.health_server import start_health_server
except ImportError:
    from services.vectorization_service import VectorizationService
    from services.qdrant_service import QdrantService
    from config import QDRANT_HOST, QDRANT_PORT
    from mq.kafka_consumer import start_kafka_consumer
    from mq.rabbit_consumer import start_rabbit_consumer
    from health_server import start_health_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - [%(filename)s:%(lineno)d] - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("main.log")
    ])
logger = logging.getLogger("ConsumerMain")


def init_services():
    """Инициализация общих сервисов"""
    vector_service = VectorizationService()
    qdrant_service = QdrantService(QDRANT_HOST, QDRANT_PORT)
    qdrant_service.ensure_collection("drug_collection", vector_service.get_vector_size())
    return vector_service, qdrant_service


def run_kafka_consumer(vector_service: VectorizationService,
                       qdrant_service: QdrantService):
    """Запуск Kafka консьюмера с пробросом исключений супервизору"""
    try:
        logger.info("Starting Kafka consumer...")
        start_kafka_consumer(vector_service, qdrant_service)
    except Exception as e:
        logger.critical(f"Kafka consumer crashed: {e}", exc_info=True)
        raise


def run_rabbitmq_consumer(vector_service: VectorizationService,
                          qdrant_service: QdrantService):
    """Запуск RabbitMQ консьюмера с пробросом исключений супервизору"""
    try:
        logger.info("Starting RabbitMQ consumer...")
        start_rabbit_consumer(vector_service, qdrant_service)
    except Exception as e:
        logger.critical(f"RabbitMQ consumer crashed: {e}", exc_info=True)
        raise


def supervise_consumers(consumers: list):
    """Супервизор фоновых консьюмеров с fail-fast остановкой при падении любого процесса"""
    with ThreadPoolExecutor(max_workers=len(consumers)) as executor:
        future_to_name = {
            executor.submit(func, *args): getattr(func, "__name__", str(func))
            for func, args in consumers
        }

        try:
            logger.info(f"Запущено {len(consumers)} консьюмеров...")
            # Fail-fast: ожидаем завершения первого из консьюмеров
            for future in as_completed(future_to_name):
                name = future_to_name[future]
                try:
                    future.result()
                    logger.warning(f"Консьюмер {name} неожиданно завершил работу.")
                except Exception as ex:
                    logger.critical(f"Консьюмер {name} аварийно завершился с ошибкой: {ex}", exc_info=True)
                    raise
                break
        except KeyboardInterrupt:
            logger.info("Получен сигнал завершения. Остановка консьюмеров...")
        finally:
            executor.shutdown(wait=False, cancel_futures=True)
            logger.info("Все консьюмеры остановлены")


def main():
    vector_service, qdrant_service = init_services()

    # Запуск HTTP сервера для health checks (/health) и Prometheus метрик (/metrics)
    start_health_server(qdrant_service, port=8000)

    consumers = [
        (run_kafka_consumer, (vector_service, qdrant_service)),
        (run_rabbitmq_consumer, (vector_service, qdrant_service))
    ]

    supervise_consumers(consumers)


if __name__ == "__main__":
    main()
