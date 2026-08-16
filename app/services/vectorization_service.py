import json
import logging
from typing import List, Dict, Any, Union, Optional
from sentence_transformers import SentenceTransformer
from functools import lru_cache

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - [%(filename)s:%(lineno)d] - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("vectorization_service.log")
    ]
)
logger = logging.getLogger("VectorizationService")


class VectorizationService:
    """Сервис для векторизации текстовых данных с использованием LaBSE модели"""

    _instance = None
    _model = None
    _embedding_dim = None

    def __new__(cls, model_name: str = 'sentence-transformers/LaBSE'):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize(model_name)
        return cls._instance

    def _initialize(self, model_name: str):
        """Инициализация модели (вызывается только один раз)"""
        try:
            logger.info(f"Загрузка модели {model_name}...")
            self._model = SentenceTransformer(model_name)
            self._embedding_dim = self._model.get_sentence_embedding_dimension()
            logger.info(
                f"Модель успешно загружена. "
                f"Размерность эмбеддингов: {self._embedding_dim}"
            )
        except Exception as e:
            logger.critical(f"Ошибка загрузки модели: {str(e)}", exc_info=True)
            raise

    @property
    def model(self) -> SentenceTransformer:
        """Доступ к модели с проверкой инициализации"""
        if self._model is None:
            raise VectorizationError("Модель не инициализирована")
        return self._model

    @property
    def embedding_dim(self) -> int:
        """Доступ к размерности эмбеддингов с проверкой"""
        if self._embedding_dim is None:
            raise VectorizationError("Размерность эмбеддингов не определена")
        return self._embedding_dim

    @staticmethod
    @lru_cache(maxsize=5000)
    def _encode_text_cached(model: Any, text: str) -> tuple[float, ...]:
        """Неизменяемый кэш эмбеддингов для предотвращения повреждения памяти"""
        raw_vector = model.encode(text, show_progress_bar=False)
        return tuple(float(x) for x in raw_vector)

    def vectorize_text(self, text: str) -> List[float]:
        """
        Векторизация текста с безопасным кэшированием (возвращает независимую копию)

        Args:
            text: Текст для векторизации

        Returns:
            Список float (независимая копия)

        Raises:
            VectorizationError: В случае ошибки векторизации
        """
        try:
            if not text or not isinstance(text, str):
                logger.warning("Получен пустой или некорректный текст")
                raise ValueError("Текст должен быть непустой строкой")

            if len(text) > 10000:
                logger.warning(f"Очень длинный текст ({len(text)} символов), возможны проблемы с памятью")

            logger.debug(f"Векторизация текста (длина: {len(text)} символов)")
            cached_tuple = self._encode_text_cached(self.model, text)
            logger.debug("Векторизация успешно завершена")
            return list(cached_tuple)

        except ValueError:
            raise
        except Exception as e:
            logger.error(
                f"Ошибка векторизации текста: {str(e)}\n"
                f"Текст (первые 100 символов): {text[:100]}...",
                exc_info=True
            )
            raise VectorizationError("Ошибка векторизации текста") from e

    def vectorize_model(self, data: Union[Dict, Any]) -> List[float]:
        """
        Векторизация JSON-сериализуемого объекта

        Args:
            data: Данные для векторизации (словарь или объект)

        Returns:
            Векторное представление данных

        Raises:
            VectorizationError: В случае ошибки векторизации
        """
        try:
            logger.debug("Векторизация модели данных")
            json_str = json.dumps(data, ensure_ascii=False, default=str, sort_keys=True)
            return self.vectorize_text(json_str)

        except (TypeError, ValueError) as e:
            logger.error(
                "Ошибка сериализации данных для векторизации",
                exc_info=True
            )
            raise VectorizationError("Невозможно сериализовать данные") from e
        except Exception as e:
            logger.error("Ошибка векторизации модели", exc_info=True)
            raise VectorizationError("Ошибка векторизации модели") from e

    def vectorize_bulk(self, texts: List[str]) -> List[List[float]]:
        """
        Пакетная векторизация текстов (более эффективная, чем по одному)

        Args:
            texts: Список текстов для векторизации

        Returns:
            Список векторных представлений

        Raises:
            VectorizationError: В случае ошибки векторизации
        """
        try:
            if not texts or not isinstance(texts, list):
                logger.warning("Получен пустой или некорректный список текстов")
                raise ValueError("Тексты должны быть непустым списком")

            logger.info(f"Пакетная векторизация {len(texts)} текстов")

            vectors = self.model.encode(texts, show_progress_bar=True, batch_size=32).tolist()
            logger.info("Пакетная векторизация успешно завершена")
            return vectors

        except Exception as e:
            logger.error(
                "Ошибка пакетной векторизации",
                exc_info=True
            )
            raise VectorizationError("Ошибка пакетной векторизации") from e

    def get_vector_size(self) -> int:
        """Возвращает размерность векторов модели"""
        return self.embedding_dim


class VectorizationError(Exception):
    """Специальное исключение для ошибок векторизации"""
    pass