import time
import json
import logging
from typing import Optional, Dict, Any, Union

try:
    import pika
    from pika.adapters.blocking_connection import BlockingChannel
    from pika.spec import BasicProperties
except ImportError:
    pika = None
    BlockingChannel = Any
    BasicProperties = Any

from app.Classes.classes import SearchQueryMessage
from app.config import RABBITMQ_HOST, RABBITMQ_QUEUE, RABBITMQ_PREFETCH_COUNT

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - [%(filename)s:%(lineno)d] - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("rabbitmq_consumer.log")
    ]
)
logger = logging.getLogger("RabbitMQConsumer")


class RabbitMQConsumer:
    """Класс для обработки сообщений из RabbitMQ с интеграцией векторных сервисов"""

    def __init__(self,vector_service, qdrant_service):
        self.vector_service = vector_service
        self.qdrant_service = qdrant_service
        self.connection = None
        self.channel = None
        self._shutdown_requested = False

    def _setup_connection(self) -> bool:
        """Настройка соединения с RabbitMQ с реконнектом"""
        try:
            params = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                connection_attempts=5,
                retry_delay=5,
                heartbeat=60,
                blocked_connection_timeout=30
            )
            self.connection = pika.BlockingConnection(params)
            self.channel = self.connection.channel()

            # Настройка очереди
            self.channel.queue_declare(
                queue=RABBITMQ_QUEUE,
                durable=True,
                arguments={'x-max-priority': 10}
            )
            self.channel.basic_qos(prefetch_count=RABBITMQ_PREFETCH_COUNT)

            logger.info(f"Успешное подключение к RabbitMQ ({RABBITMQ_HOST})")
            return True

        except Exception as e:
            logger.error(f"Ошибка подключения: {str(e)}", exc_info=True)
            return False

    def _process_message(self, raw_input: Union[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Обработка текстового или JSON сообщения через векторные сервисы"""
        try:
            search_msg = SearchQueryMessage.from_message(raw_input)
            logger.debug(f"Обработка поискового запроса: '{search_msg.query}' (limit: {search_msg.limit})")

            # Векторизация чистого текста запроса (без JSON синтаксиса)
            vector = self.vector_service.vectorize_text(search_msg.query)
            if not vector:
                raise ValueError("Ошибка векторизации")

            # Поиск в Qdrant
            results = self.qdrant_service.search_vector(
                vector=vector,
                limit=search_msg.limit,
                score_threshold=search_msg.score_threshold,
                **search_msg.filters
            )
            if not results:
                logger.warning(f"Не найдено результатов для запроса: '{search_msg.query}'")
                return None

            return {
                "status": "success",
                "query": search_msg.query,
                "results": results,
                "vector_dim": len(vector)
            }

        except Exception as e:
            logger.error(f"Ошибка обработки: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "message": str(e)
            }

    def _on_message_callback(
            self,
            channel: BlockingChannel,
            method: pika.spec.Basic.Deliver,
            properties: BasicProperties,
            body: bytes
    ) -> None:
        """Callback для обработки входящих сообщений"""
        try:
            message_id = properties.message_id or method.delivery_tag
            logger.info(f"Получено сообщение ID: {message_id}")

            try:
                text = body.decode('utf-8')
            except UnicodeDecodeError:
                logger.error("Неверный формат сообщения")
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return

            # Обработка сообщения
            response = self._process_message(text)

            # Отправка ответа (если указана очередь для ответа)
            if properties.reply_to:
                self._send_response(
                    channel,
                    properties.reply_to,
                    properties.correlation_id,
                    response or {"status": "error", "message": "Processing failed"}
                )

            # Подтверждение обработки
            channel.basic_ack(delivery_tag=method.delivery_tag)
            logger.info(f"Сообщение {message_id} обработано")

        except Exception as e:
            logger.critical(f"Критическая ошибка: {str(e)}", exc_info=True)
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def _send_response(
            self,
            channel: BlockingChannel,
            reply_to: str,
            correlation_id: str,
            data: Dict[str, Any]
    ) -> None:
        """Отправка ответа через RabbitMQ"""
        try:
            channel.basic_publish(
                exchange='',
                routing_key=reply_to,
                properties=BasicProperties(
                    correlation_id=correlation_id,
                    content_type='application/json'
                ),
                body=json.dumps(data)
            )
            logger.debug(f"Ответ отправлен в очередь {reply_to}")
        except Exception as e:
            logger.error(f"Ошибка отправки ответа: {str(e)}")

    def start(self) -> None:
        """Запуск consumer в бесконечном цикле с обработкой реконнекта"""
        logger.info("Запуск RabbitMQ consumer...")

        while not self._shutdown_requested:
            try:
                if not self._setup_connection():
                    if self._shutdown_requested:
                        break
                    logger.warning("Повторная попытка подключения через 10 сек...")
                    time.sleep(10)
                    continue

                self.channel.basic_consume(
                    queue=RABBITMQ_QUEUE,
                    on_message_callback=self._on_message_callback,
                    consumer_tag="vectorization_consumer"
                )

                logger.info("Ожидание сообщений...")
                self.channel.start_consuming()

            except pika.exceptions.ConnectionClosedByBroker:
                logger.warning("Соединение закрыто брокером. Переподключение...")
                continue
            except pika.exceptions.AMQPChannelError as e:
                logger.error(f"Ошибка канала: {str(e)}")
                break
            except pika.exceptions.AMQPConnectionError:
                logger.warning("Потеряно соединение. Переподключение...")
                continue
            except KeyboardInterrupt:
                logger.info("Получен сигнал прерывания...")
                self._shutdown_requested = True
            except Exception as e:
                logger.critical(f"Неожиданная ошибка: {str(e)}", exc_info=True)
                break

        self._cleanup()
        logger.info("Consumer остановлен")

    def _cleanup(self):
        """Корректное завершение работы"""
        try:
            if self.channel and self.channel.is_open:
                self.channel.stop_consuming()
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception as e:
            logger.error(f"Ошибка при завершении работы: {str(e)}")


def start_rabbit_consumer(vector_service, qdrant_service):
    """Функция для запуска из main.py"""
    consumer = RabbitMQConsumer(vector_service, qdrant_service)
    consumer.start()

