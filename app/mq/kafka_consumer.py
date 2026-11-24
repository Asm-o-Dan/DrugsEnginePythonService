from confluent_kafka import Consumer, KafkaError, KafkaException, Message
import json
import logging
from typing import Optional

from app.Classes.classes import Drug
from app.config import KAFKA_BROKER, KAFKA_TOPIC
from app.services.qdrant_service import QdrantService
from app.services.vectorization_service import VectorizationService
from app.services.gpt_service import DrugInfoAPI

logger = logging.getLogger("KafkaConsumer")


class KafkaDrugConsumer:
    """Класс для обработки сообщений о лекарствах из Kafka"""

    def __init__(self,
                 vector_service: VectorizationService,
                 qdrant_service: QdrantService,
                 api_service: DrugInfoAPI):
        self.vector_service = vector_service
        self.qdrant_service = qdrant_service
        self.api_service = api_service
        self.consumer = self._configure_consumer()

    def _configure_consumer(self) -> Consumer:
        """Настройка Kafka consumer"""
        conf = {
            'bootstrap.servers': KAFKA_BROKER,
            'group.id': 'drug-vectorization-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'max.poll.interval.ms': 300000,
            'session.timeout.ms': 10000
        }
        consumer = Consumer(conf)
        consumer.subscribe([KAFKA_TOPIC])
        return consumer

    @staticmethod
    def _parse_message(msg: Message) -> Optional[Drug]:
        """Парсинг и валидация сообщения Kafka"""
        try:
            data = json.loads(msg.value().decode('utf-8'))
            drug = Drug.from_json(data)


            if not drug.name or not drug.id:
                raise ValueError("Отсутствуют обязательные поля")

            logger.debug(f"Успешно распарсено лекарство: {drug.name}")
            return drug

        except json.JSONDecodeError as e:
            logger.error(f"Ошибка декодирования JSON: {e}")
        except ValueError as e:
            logger.error(f"Ошибка валидации данных: {e}")
        except Exception as e:
            logger.error(f"Неожиданная ошибка при парсинге: {e}")
        return None

    def _process_single_drug(self, drug: Drug) -> bool:
        """
        Индексация одного препарата. Описание от LLM — необязательное обогащение:
        без ключей или при сбое провайдера препарат всё равно индексируется по названию,
        действующему веществу и форме, иначе поиск по смыслу был бы пуст.
        """
        try:
            if self.api_service.configured:
                drug_info = self.api_service.get_drug_info(drug.name)
                if drug_info:
                    drug.description = drug_info
                    drug.description_source = "generated"
                else:
                    logger.warning(f"Описание для {drug.name} не получено — индексируем без него")

            vector = self.vector_service.vectorize_passage(drug.search_text())
            if not vector:
                logger.error(f"Ошибка векторизации для {drug.name}")
                return False

            if not self.qdrant_service.add_vector(drug, vector):
                logger.error(f"Ошибка сохранения вектора для {drug.name}")
                return False

            logger.debug(f"Проиндексирован: {drug.name}")
            return True

        except Exception as e:
            logger.error(f"Ошибка обработки {drug.name}: {e}", exc_info=True)
            return False

    def run_consumption_loop(self):
        """Основной цикл потребления сообщений"""
        logger.info("Запуск consumer...")

        try:
            while True:
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    self._handle_kafka_error(msg.error())
                    continue

                drug = self._parse_message(msg)
                if not drug:
                    continue

                success = self._process_single_drug(drug)
                if success:
                    self.consumer.commit(msg)

        except KeyboardInterrupt:
            logger.info("Получен сигнал прерывания...")
        except Exception as e:
            logger.critical(f"Критическая ошибка: {e}", exc_info=True)
        finally:
            self._shutdown()

    def _handle_kafka_error(self, error):
        """Обработка ошибок Kafka"""
        if error.code() == KafkaError._PARTITION_EOF:
            logger.debug("Достигнут конец партиции")
        else:
            logger.error(f"Ошибка Kafka: {error}")

    def _shutdown(self):
        """Корректное завершение работы"""
        logger.info("Завершение работы consumer")
        self.consumer.close()


def start_kafka_consumer(
        vector_service: VectorizationService,
        qdrant_service: QdrantService
):
    """Функция для запуска из main.py"""
    api_service = DrugInfoAPI()
    consumer = KafkaDrugConsumer(vector_service, qdrant_service, api_service)
    consumer.run_consumption_loop()