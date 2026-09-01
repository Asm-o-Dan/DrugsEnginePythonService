import json
import logging
import time
from typing import List, Optional, Tuple

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from app.Classes.classes import Drug
from app.config import KAFKA_BROKER, KAFKA_TOPIC
from app.services.gpt_service import DrugInfoAPI
from app.services.qdrant_service import QdrantService
from app.services.vectorization_service import VectorizationService

try:
    from app.telemetry import extract_traceparent, metrics
except ImportError:
    from telemetry import extract_traceparent, metrics

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - [%(filename)s:%(lineno)d] - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("kafka_consumer.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("KafkaConsumer")


class KafkaDrugConsumer:
    """Micro-batching Kafka Consumer для потоковой валидации,
    AI-обогащения через Gemini и векторного сохранения в Qdrant.
    """

    BATCH_SIZE = 25
    BATCH_MAX_WAIT_SECONDS = 3.0

    def __init__(
        self,
        vector_service: VectorizationService,
        qdrant_service: QdrantService,
        api_service: DrugInfoAPI,
        topic: Optional[str] = None,
        broker: Optional[str] = None,
    ):
        self.vector_service = vector_service
        self.qdrant_service = qdrant_service
        self.api_service = api_service
        chosen_topic = topic.strip() if (isinstance(topic, str) and topic.strip()) else KAFKA_TOPIC
        self.topic = chosen_topic if (isinstance(chosen_topic, str) and chosen_topic.strip()) else "drugs"
        chosen_broker = broker.strip() if (isinstance(broker, str) and broker.strip()) else KAFKA_BROKER
        self.broker = chosen_broker if (isinstance(chosen_broker, str) and chosen_broker.strip()) else "kafka:9092"
        self.consumer = self._configure_consumer()

    def _configure_consumer(self) -> Consumer:
        """Настройка Kafka consumer"""
        conf = {
            "bootstrap.servers": self.broker,
            "group.id": "drug-vectorization-group",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300000,
            "session.timeout.ms": 10000,
        }
        consumer = Consumer(conf)
        consumer.subscribe([self.topic])
        return consumer

    @staticmethod
    def _parse_message(msg: Message) -> Optional[Drug]:
        """Парсинг и валидация сообщения Kafka"""
        try:
            if msg is None or msg.value() is None:
                logger.debug("Получено пустое сообщение Kafka (tombstone)")
                return None

            raw_val = msg.value()
            if isinstance(raw_val, bytes):
                decoded_val = raw_val.decode("utf-8")
            elif isinstance(raw_val, str):
                decoded_val = raw_val
            else:
                logger.error(f"Неожиданный тип значения сообщения Kafka: {type(raw_val)}")
                return None

            data = json.loads(decoded_val)
            drug = Drug.from_json(data)

            if not drug.name or not drug.id:
                raise ValueError("Отсутствуют обязательные поля name или id")

            logger.debug(f"Успешно распарсено лекарство из Kafka: {drug.name}")
            return drug

        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            logger.error(f"Ошибка декодирования сообщения JSON: {e}")
        except ValueError as e:
            logger.error(f"Ошибка валидации данных: {e}")
        except Exception as e:
            logger.error(f"Неожиданная ошибка при парсинге: {e}")
        return None

    def _process_batch(
        self, batch: List[Tuple[Message, Drug]]
    ) -> List[Message]:
        """Пакетная обработка пачки лекарств:
        1. 1 запрос в Gemini на всю пачку (25 шт) с валидацией достоверности.
        2. Фильтрация VERIFIED лекарств.
        3. Пакетная векторизация через LaBSE (vectorize_bulk).
        4. Пакетное сохранение в Qdrant.
        
        Returns:
            Список Kafka сообщений, которые успешно обработаны и готовы к коммиту.
        """
        if not batch:
            return []

        start_time = time.time()
        queries = [drug.name for _, drug in batch]
        logger.info(
            "Запуск пакетной обработки %d сообщений из Kafka...", len(queries)
        )

        try:
            # 1. Запрос в Gemini (батч с авто-балансировкой моделей 3.5 Lite + 3.6 Flash)
            batch_enrichments = self.api_service.get_batch_drug_info(queries)

            # Сопоставляем результаты по исходному названию
            enrichment_map = {}
            for item in batch_enrichments:
                q_key = item.get("query", "").strip().lower()
                if q_key:
                    enrichment_map[q_key] = item

            # 2. Разделяем лекарства на валидированные и нелекарственные
            verified_items: List[Tuple[Message, Drug, str]] = []
            committed_messages: List[Message] = []

            for msg, drug in batch:
                q_key = drug.name.strip().lower()
                enrichment = enrichment_map.get(q_key)

                # Fallback: если не нашли по точному ключу, пробуем по индексу или одиночный
                if not enrichment:
                    logger.warning(
                        "Элемент '%s' отсутствует в ответе батча Gemini, пробуем одиночный fallback",
                        drug.name,
                    )
                    single_desc = self.api_service.get_drug_info(drug.name)
                    if single_desc:
                        verified_items.append((msg, drug, single_desc))
                    else:
                        metrics.inc_counter(
                            "drugsengine_python_drug_errors_total",
                            labels={"reason": "llm_failed"},
                        )
                    continue

                status = enrichment.get("status", "VERIFIED")
                is_drug = enrichment.get("is_drug", True)

                if status == "VERIFIED" or is_drug:
                    desc = enrichment.get("description")
                    if not desc:
                        desc = (
                            f"Применение: {enrichment.get('drug_name', drug.name)}, "
                            f"Диагнозы: {', '.join(enrichment.get('indications', []))}, "
                            f"Действующее вещество: {enrichment.get('active_ingredient', '')}, "
                            f"Аналоги: {', '.join(enrichment.get('analogs', []))}"
                        )
                    verified_items.append((msg, drug, desc))
                else:
                    # NOT_DRUG или INVALID — отсеиваем, но коммитим оффсет
                    logger.info(
                        "Товар '%s' отфильтрован валидатором (%s). Пропуск сохранения.",
                        drug.name,
                        status,
                    )
                    committed_messages.append(msg)

            # 3. Пакетная векторизация валидированных лекарств
            if verified_items:
                descriptions = [desc for _, _, desc in verified_items]
                vectors = self.vector_service.vectorize_bulk(descriptions)

                # 4. Сохранение в Qdrant
                for (msg, drug, desc), vector in zip(verified_items, vectors):
                    drug.description = desc
                    if self.qdrant_service.add_vector(drug, vector):
                        committed_messages.append(msg)
                        metrics.inc_counter(
                            "drugsengine_python_drugs_processed_total"
                        )
                    else:
                        logger.error(
                            "Ошибка сохранения в Qdrant для '%s'", drug.name
                        )
                        metrics.inc_counter(
                            "drugsengine_python_drug_errors_total",
                            labels={"reason": "qdrant_failed"},
                        )

            duration = time.time() - start_time
            metrics.observe_histogram(
                "drugsengine_python_drug_processing_seconds", duration
            )
            logger.info(
                "Пакет из %d сообщений обработан за %.2fs (успешно: %d, отфильтровано: %d)",
                len(batch),
                duration,
                len(verified_items),
                len(committed_messages) - len(verified_items),
            )

            return committed_messages

        except Exception as e:
            metrics.inc_counter(
                "drugsengine_python_drug_errors_total",
                labels={"reason": "batch_processing_exception"},
            )
            logger.error(f"Критическая ошибка при обработке батча: {e}", exc_info=True)
            return []

    def run_consumption_loop(self):
        """Основной цикл потребления сообщений с поддержкой micro-batching"""
        logger.info(
            "Запуск micro-batching consumer (батч: %d, таймаут: %.1fs)...",
            self.BATCH_SIZE,
            self.BATCH_MAX_WAIT_SECONDS,
        )
        metrics.set_gauge("drugsengine_python_kafka_consumer_active", 1.0)

        current_batch: List[Tuple[Message, Drug]] = []
        batch_start_time = time.time()

        try:
            while True:
                msg = self.consumer.poll(0.5)

                if msg is not None:
                    if msg.error():
                        self._handle_kafka_error(msg.error())
                    else:
                        trace_context = extract_traceparent(msg.headers())
                        if trace_context and "trace_id" in trace_context:
                            logger.debug(
                                f"Kafka message trace_id: {trace_context['trace_id']}"
                            )

                        drug = self._parse_message(msg)
                        if drug:
                            if not current_batch:
                                batch_start_time = time.time()
                            current_batch.append((msg, drug))

                # Проверяем условия сброса батча: достигли лимита или истек таймаут ожидания
                has_items = len(current_batch) > 0
                size_reached = len(current_batch) >= self.BATCH_SIZE
                time_expired = (
                    has_items
                    and (time.time() - batch_start_time)
                    >= self.BATCH_MAX_WAIT_SECONDS
                )

                if size_reached or time_expired:
                    processed_msgs = self._process_batch(current_batch)
                    # Коммитим оффсет последнего успешно обработанного сообщения
                    if processed_msgs:
                        self.consumer.commit(processed_msgs[-1])
                    current_batch = []
                    batch_start_time = time.time()

        except KeyboardInterrupt:
            logger.info("Получен сигнал прерывания...")
        except Exception as e:
            logger.critical(f"Критическая ошибка consumer: {e}", exc_info=True)
        finally:
            metrics.set_gauge("drugsengine_python_kafka_consumer_active", 0.0)
            self._shutdown()

    def _handle_kafka_error(self, error):
        """Обработка ошибок Kafka"""
        if error.code() == KafkaError._PARTITION_EOF:
            logger.debug("Достигнут конец партиции")
        else:
            logger.error(f"Ошибка Kafka: {error}")

    def _shutdown(self):
        """Корректное завершение работы"""
        metrics.set_gauge("drugsengine_python_kafka_consumer_active", 0.0)
        logger.info("Завершение работы consumer")
        self.consumer.close()


def start_kafka_consumer(
    vector_service: VectorizationService,
    qdrant_service: QdrantService,
    topic: Optional[str] = None,
    broker: Optional[str] = None,
):
    """Функция для запуска из main.py"""
    api_service = DrugInfoAPI()
    consumer = KafkaDrugConsumer(
        vector_service, qdrant_service, api_service, topic=topic, broker=broker
    )
    consumer.run_consumption_loop()