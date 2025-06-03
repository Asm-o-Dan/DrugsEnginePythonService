import logging
from concurrent.futures import ThreadPoolExecutor
from services.vectorization_service import VectorizationService
from services.qdrant_service import QdrantService
from config import QDRANT_HOST,QDRANT_PORT
from mq.kafka_consumer import start_kafka_consumer
from mq.rabbit_consumer import start_rabbit_consumer



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
    qdrant_service = QdrantService(QDRANT_HOST,QDRANT_PORT)
    qdrant_service.ensure_collection("drug_collection", vector_service.get_vector_size())
    return vector_service, qdrant_service 

def run_kafka_consumer(vector_service: VectorizationService,
                       qdrant_service: QdrantService):
    try:
        logger.info("Starting Kafka consumer...")
        start_kafka_consumer(vector_service, qdrant_service)
    except Exception as e:
        logger.error(f"Kafka consumer failed: {e}", exc_info=True)


def run_rabbitmq_consumer(vector_service: VectorizationService,
                          qdrant_service: QdrantService):
    try:
        logger.info("Starting RabbitMQ consumer...")
        start_rabbit_consumer(vector_service, qdrant_service)
    except Exception as e:
        logger.error(f"RabbitMQ consumer failed: {e}", exc_info=True)


def main():
    # Инициализация сервисов один раз
    vector_service, qdrant_service = init_services()

    consumers = [
        (run_kafka_consumer, (vector_service, qdrant_service)),
        (run_rabbitmq_consumer, (vector_service, qdrant_service))
    ]

    # Используем ThreadPoolExecutor вместо ProcessPoolExecutor
    with ThreadPoolExecutor(max_workers=len(consumers)) as executor:
        try:
            logger.info(f"Starting {len(consumers)} consumers...")
            futures = [
                executor.submit(func, *args)
                for func, args in consumers
            ]
            for future in futures:
                future.result()  # Блокируем основной поток

        except KeyboardInterrupt:
            logger.info("Shutting down consumers...")
        except Exception as e:
            logger.critical(f"Main process error: {e}", exc_info=True)
        finally:
            executor.shutdown(wait=True)
            logger.info("All consumers stopped")


if __name__ == "__main__":
    main()
