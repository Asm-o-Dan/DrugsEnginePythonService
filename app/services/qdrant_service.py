from typing import List, Optional, Dict, Any, Union
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct, VectorParams, Distance, ScoredPoint, Filter
from qdrant_client.http.exceptions import UnexpectedResponse, ResponseHandlingException
from threading import Lock
import logging
import os
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from app.Classes.classes import Drug

logger = logging.getLogger("QdrantService")


class QdrantService:
    """Сервис для работы с Qdrant с кешированием схемы коллекций и поддержкой переподключения"""

    _instance = None
    _init_lock = Lock()

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
        self._reconnect_lock = Lock()
        self._collection_dimensions: Dict[str, int] = {}
        self._reconnect()

    def _reconnect(self):
        """Установка нового подключения с потокобезопасной блокировкой переподключения"""
        with self._reconnect_lock:
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
        """Создает коллекцию и кеширует ее размерность в памяти"""
        try:
            if not self.client.collection_exists(collection_name):
                self.client.create_collection(
                    collection_name=collection_name,
                    vectors_config=VectorParams(
                        size=vector_size,
                        distance=Distance.COSINE
                    )
                )
                logger.info(f"Создана коллекция '{collection_name}' (размерность: {vector_size})")
            self._collection_dimensions[collection_name] = vector_size
            return True
        except Exception as e:
            logger.error(f"Ошибка при работе с коллекцией: {str(e)}", exc_info=True)
            self._reconnect()
            raise

    def _get_expected_dimension(self, collection_name: str) -> Optional[int]:
        """Возвращает размерность вектора для коллекции из кеша или запрашивает у сервера единожды"""
        if collection_name not in self._collection_dimensions:
            try:
                collection_info = self.client.get_collection(collection_name)
                vectors_config = collection_info.config.params.vectors
                if hasattr(vectors_config, "size"):
                    self._collection_dimensions[collection_name] = vectors_config.size
                elif isinstance(vectors_config, dict) and "size" in vectors_config:
                    self._collection_dimensions[collection_name] = vectors_config["size"]
            except Exception as e:
                logger.warning(f"Не удалось получить конфигурацию коллекции {collection_name}: {e}")
                return None
        return self._collection_dimensions.get(collection_name)

    def _validate_vector(self, vector: List[float], collection_name: str) -> bool:
        """Проверка размерности вектора без избыточных сетевых запросов"""
        expected_size = self._get_expected_dimension(collection_name)
        if expected_size is not None and len(vector) != expected_size:
            logger.error(
                f"Несоответствие размерности: ожидалось {expected_size}, получено {len(vector)}"
            )
            return False
        return True

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=5))
    def add_vector(self, drug: Drug, vector: List[float], collection_name: str = "drug_collection") -> bool:
        """Добавление вектора с валидацией по локальному кешу схемы"""
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

            self.client.upsert(
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
        """Поиск векторов без лишних блокировок потоков"""
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

            results = self.client.search(**search_params)
            return self._format_search_results(results)

        except Exception as e:
            logger.error(f"Ошибка поиска: {str(e)}", exc_info=True)
            self._reconnect()
            return None

    def _build_filter(self, **filters) -> Dict[str, Any]:
        """Формирование фильтров для поиска в Qdrant"""
        must_conditions = []
        for key, value in filters.items():
            if value is not None:
                must_conditions.append({"key": key, "match": {"value": value}})
        return {"must": must_conditions}

    def _format_search_results(self, results: List[ScoredPoint]) -> List[Dict[str, Any]]:
        """Форматирование результатов поиска"""
        formatted = []
        for hit in results:
            item = {
                "id": str(hit.id),
                "score": float(hit.score),
            }
            if hit.payload:
                item.update(hit.payload)
            formatted.append(item)
        return formatted

    def health_check(self) -> bool:
        """Проверка здоровья подключения"""
        try:
            self.client.get_collections()
            return True
        except Exception as e:
            logger.warning(f"Health check Qdrant не удался: {e}")
            return False

    def get_vectors_count(self, collection_name: str = "drug_collection") -> int:
        """Возвращает общее количество векторов/точек в коллекции Qdrant"""
        try:
            if not self.client.collection_exists(collection_name):
                return 0
            collection_info = self.client.get_collection(collection_name)
            count = getattr(collection_info, "points_count", None)
            if count is None:
                count = getattr(collection_info, "vectors_count", 0)
            return int(count) if count is not None else 0
        except Exception as e:
            logger.warning(f"Не удалось получить количество векторов из Qdrant ({collection_name}): {e}")
            return 0

