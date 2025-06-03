from typing import List, Optional, Dict, Any, Union
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct, VectorParams, Distance, ScoredPoint, Filter
from qdrant_client.http.exceptions import UnexpectedResponse, ResponseHandlingException
from threading import Lock, RLock
from functools import lru_cache
import logging
import os
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from app.Classes.classes import Drug

logger = logging.getLogger("QdrantService")


class QdrantService:
    """Сервис для работы с Qdrant с поддержкой переподключения и потокобезопасности"""

    _instance = None
    _init_lock = Lock()
    _client_lock = RLock()  # Реентерабельная блокировка для операций с клиентом

    def __new__(cls, host: str = "qdrant", port: int = 6333):
        with cls._init_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialize(
                    host or os.getenv("QDRANT_HOST", "qdrant"),
                    port or int(os.getenv("QDRANT_PORT", 6333))
                )
        return cls._instance

    def _initialize(self, host: str, port: int):
        """Инициализация клиента с поддержкой переподключения"""
        self._host = host
        self._port = port
        self._reconnect()

    def _reconnect(self):
        """Установка нового подключения с обработкой ошибок"""
        with self._client_lock:
            try:
                self.client = QdrantClient(
                    url=f"http://{self._host}:{self._port}"
                )
                logger.info(f"Подключено к Qdrant {self._host}:{self._port}")
            except Exception as e:
                logger.critical(f"Ошибка подключения к Qdrant: {str(e)}", exc_info=True)
                raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((ResponseHandlingException, UnexpectedResponse)))
    def ensure_collection(self, collection_name: str, vector_size: int) -> bool:
        """Создает коллекцию с повторами при ошибках"""
        try:
            with self._client_lock:
                if not self.client.collection_exists(collection_name):
                    self.client.create_collection(
                        collection_name=collection_name,
                        vectors_config=VectorParams(
                            size=vector_size,
                            distance=Distance.COSINE
                        )
                    )
                    logger.info(f"Создана коллекция '{collection_name}' (размерность: {vector_size})")
                return True
        except Exception as e:
            logger.error(f"Ошибка при работе с коллекцией: {str(e)}", exc_info=True)
            self._reconnect()  # Пытаемся переподключиться
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=5))
    def add_vector(self, drug: Drug, vector: List[float], collection_name: str = "drug_collection") -> bool:
        """Добавление вектора с проверкой размерности"""
        if not self._validate_vector(vector, collection_name):
            return False

        try:
            point = PointStruct(
                id=drug.id,
                vector=vector,
                payload={
                    "name": drug.name,
                    "id": drug.id,
                    "description": getattr(drug, "description", ""),
                    "category": getattr(drug, "category", "")
                }
            )

            with self._client_lock:
                operation_info = self.client.upsert(
                    collection_name=collection_name,
                    points=[point],
                    wait=True
                )

            logger.debug(f"Добавлен вектор для {drug.name} (ID: {drug.id})")
            return True

        except Exception as e:
            logger.error(f"Ошибка при добавлении вектора: {str(e)}")
            self._reconnect()
            return False

    def _validate_vector(self, vector: List[float], collection_name: str) -> bool:
        """Проверка размерности вектора"""
        with self._client_lock:
            collection_info = self.client.get_collection(collection_name)
            expected_size = collection_info.config.params.vectors.size

        if len(vector) != expected_size:
            logger.error(
                f"Несоответствие размерности: ожидалось {expected_size}, получено {len(vector)}"
            )
            return False
        return True

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=5))
    def search_vector(
            self,
            vector: List[float],
            collection_name: str = "drug_collection",
            limit: int = 5,
            score_threshold: Optional[float] = None,
            **filters
    ) -> Optional[List[Dict[str, Any]]]:
        """Поиск с автоматическим переподключением"""
        try:
            if not self._validate_vector(vector, collection_name):
                return None

            search_params = {
                "collection_name": collection_name,
                "query_vector": vector,
                "limit": limit,
                "score_threshold": score_threshold,
                "with_payload": True
            }

            if filters:
                search_params["query_filter"] = Filter(**self._build_filter(**filters))

            with self._client_lock:
                results = self.client.search(**search_params)

            return self._format_search_results(results)

        except Exception as e:
            logger.error(f"Ошибка поиска: {str(e)}", exc_info=True)
            self._reconnect()
            return None

    def health_check(self) -> bool:
        """Проверка здоровья с переподключением"""
        try:
            with self._client_lock:
                return self.client._client.up()
        except Exception:
            try:
                self._reconnect()
                return True
            except Exception:
                return False
