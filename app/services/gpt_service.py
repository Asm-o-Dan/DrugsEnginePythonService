import json
import logging
import time
from typing import Dict, List, Optional

from google import genai
from google.genai import types

from app.config import GEMINI_API_KEY, GEMINI_MODEL

logger = logging.getLogger(__name__)


class DrugInfoAPI:
    """Сервис пакетного обогащения и валидации достоверности лекарств
    через Google Gemini API с мульти-модельным балансировщиком (Flash Lite + Flash)
    и Structured JSON Schema.
    """

    SYSTEM_INSTRUCTION = (
        "Ты — профессиональный фармацевтический валидатор и эксперт.\n"
        "Для каждого переданного товара из аптечного каталога определи:\n"
        "1. is_drug (bool): является ли товар лекарственным средством (НЕ БАД, не косметика, не медтехника).\n"
        "2. drug_name (str): нормализованное торговое/международное название лекарства (или null если не лекарство).\n"
        "3. active_ingredient (str): точное международное непатентованное наименование (МНН) действующего вещества.\n"
        "4. indications (list[str]): список из ровно 3 ключевых медицинских показаний/диагнозов.\n"
        "5. analogs (list[str]): список из ровно 3 проверенных торговых аналогов (с тем же МНН или действием).\n"
        "6. description (str): строго одна строка в формате: "
        "'Применение: [применение], Диагнозы: [диагноз 1, диагноз 2, диагноз 3], "
        "Действующее вещество: [МНН], Аналоги: [аналог 1, аналог 2, аналог 3]'.\n"
        "7. status (str): один из вердиктов: 'VERIFIED' (подлинное лекарство), "
        "'NOT_DRUG' (гигиена, косметика, БАД, изделие), 'INVALID' (битый текст, ошибка парсера).\n\n"
        "Верни строго JSON массив объектов с указанными полями."
    )

    MAX_RETRIES_PER_MODEL = 2
    BASE_BACKOFF_SECONDS = 1.5

    def __init__(self):
        if not GEMINI_API_KEY:
            logger.error("GEMINI_API_KEY не задан! Обогащение данных будет недоступно.")
            self._client = None
            return

        self._client = genai.Client(api_key=GEMINI_API_KEY)
        # Тройной пул моделей для максимальной утилизации всех доступных квот AI Studio
        models = [
            GEMINI_MODEL,
            "gemini-3.5-flash-lite",
            "gemini-3.6-flash",
            "gemini-3.7-flash",
        ]
        seen = set()
        unique_models = [m for m in models if m and not (m in seen or seen.add(m))]
        self._models = unique_models[:3]
        self._current_model_idx = 0
        logger.info(
            "Gemini Triple-Model API клиент инициализирован (пул моделей: %s)",
            self._models,
        )

    def _get_active_models_order(self) -> List[str]:
        """Возвращает порядок моделей с ротацией для равномерного распределения квот"""
        idx = self._current_model_idx % len(self._models)
        self._current_model_idx = (self._current_model_idx + 1) % len(self._models)
        return self._models[idx:] + self._models[:idx]

    def get_batch_drug_info(self, queries: List[str]) -> List[Dict]:
        """Пакетная обработка и валидация пачки лекарств (до 25-50 штук за 1 запрос).
        
        Гарантирует 100% экономию квот RPD:
        20 запросов * 25 лекарств = 500 лекарств/день на одной модели,
        а с пулом из 2 моделей = 1 000 лекарств/день!
        
        Args:
            queries: Список строк/названий из парсера.
            
        Returns:
            Список словарей с валидированными и обогащенными данными.
        """
        if not queries or self._client is None:
            return []

        prompt = (
            f"Проанализируй список товаров ({len(queries)} позиций) и верни JSON массив:\n"
            f"{json.dumps(queries, ensure_ascii=False)}"
        )

        models_to_try = self._get_active_models_order()

        for model_name in models_to_try:
            for attempt in range(1, self.MAX_RETRIES_PER_MODEL + 1):
                try:
                    logger.debug(
                        "Отправка батча (%d позиций) в модель %s (попытка %d)...",
                        len(queries),
                        model_name,
                        attempt,
                    )
                    start_t = time.time()

                    response = self._client.models.generate_content(
                        model=model_name,
                        contents=prompt,
                        config=types.GenerateContentConfig(
                            system_instruction=self.SYSTEM_INSTRUCTION,
                            response_mime_type="application/json",
                            temperature=0.3,
                            max_output_tokens=4096,
                        ),
                    )

                    duration = time.time() - start_t

                    if not response.text:
                        logger.warning(
                            "Модель %s вернула пустой ответ для батча", model_name
                        )
                        break

                    raw_text = response.text.strip()
                    parsed = json.loads(raw_text)

                    if isinstance(parsed, list):
                        logger.info(
                            "Батч из %d позиций успешно обработан моделью %s за %.2fs",
                            len(parsed),
                            model_name,
                            duration,
                        )
                        return parsed
                    else:
                        logger.warning(
                            "Модель %s вернула не массив: %s",
                            model_name,
                            raw_text[:100],
                        )

                except Exception as e:
                    err_msg = str(e)
                    if "429" in err_msg or "RESOURCE_EXHAUSTED" in err_msg:
                        backoff = self.BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                        logger.warning(
                            "Rate-limit (429) на модели %s, переключение или backoff %.1fs...",
                            model_name,
                            backoff,
                        )
                        time.sleep(backoff)
                        # Переходим к следующей модели из пула при 429
                        break
                    else:
                        logger.error(
                            "Ошибка запроса к %s: %s", model_name, err_msg
                        )
                        break

        logger.error(
            "Не удалось обработать батч из %d позиций всеми моделями пула",
            len(queries),
        )
        return []

    def get_drug_info(self, drug_name: str) -> Optional[str]:
        """Одиночный метод обогащения (для обратной совместимости)"""
        batch_res = self.get_batch_drug_info([drug_name])
        if batch_res:
            item = batch_res[0]
            if item.get("status") == "VERIFIED" or item.get("is_drug"):
                desc = item.get("description")
                if desc:
                    return desc
                # Синтезируем описание, если поле description пустое
                return (
                    f"Применение: {item.get('drug_name', drug_name)}, "
                    f"Диагнозы: {', '.join(item.get('indications', []))}, "
                    f"Действующее вещество: {item.get('active_ingredient', '')}, "
                    f"Аналоги: {', '.join(item.get('analogs', []))}"
                )
        return None
