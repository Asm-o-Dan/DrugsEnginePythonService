import datetime
import logging
from typing import Optional, List, Dict
import requests

try:
    from together import Together
except ImportError:
    Together = None

try:
    from g4f.client import Client
    from g4f.Provider import Phind, Liaobots
except ImportError:
    Client = None
    Phind = None
    Liaobots = None

from app.config import (
    DEEPINFRA_API_KEY,
    DEEPINFRA_URL,
    TOGETHER_API_KEY,
    TOGETHER_MODEL,
    FIREWORKS_API_KEY,
    FIREWORKS_URL,
    FIREWORKS_MODEL,
    G4F_MODEL,
)

logger = logging.getLogger(__name__)


class DrugInfoAPI:
    """Класс для работы с API генерации описаний лекарств с динамической Round Robin стратегией и отказоустойчивым фоллбеком"""

    def __init__(self):
        # Конфигурация API с загрузкой секретов из конфигурации
        self.api_config = {
            "deepinfra": {
                "url": DEEPINFRA_URL,
                "api_key": DEEPINFRA_API_KEY,
                "enabled": bool(DEEPINFRA_API_KEY),
                "enable_time": None,
            },
            "together": {
                "api_key": TOGETHER_API_KEY,
                "model": TOGETHER_MODEL,
                "enabled": bool(TOGETHER_API_KEY),
                "enable_time": None,
            },
            "fireworks": {
                "url": FIREWORKS_URL,
                "api_key": FIREWORKS_API_KEY,
                "model": FIREWORKS_MODEL,
                "enabled": bool(FIREWORKS_API_KEY),
                "enable_time": None,
            },
            "g4f": {
                "model": G4F_MODEL,
                "providers": [Phind, Liaobots],
                "enabled": True,
                "enable_time": None,
            },
        }

        # Шаблоны промптов
        self.prompt_templates = {
            "basic": (
                "Ты — ассистент, который коротко и четко составляет описание лекарства. "
                "ЕСЛИ ТО ЧТО Я ПРИШЛЮ ТЕБЕ БУДЕТ НЕ ЛЕКАРСТВОМ, ПРОСТО СКАЖИ, ЧТО ЭТО НЕ ЛЕКАРСТВО И Я [область применения]. "
                "Формат ответа: Название: [название], Применение: [применение], "
                "Диагнозы: [диагнозы], Вещество: [вещество], Аналоги: [аналоги]. "
                "Ответь в 1 строке."
            ),
            "strict": (
                "Название лекарства: {drug_name}\n"
                "ЕСЛИ ТО ЧТО Я ПРИШЛЮ ТЕБЕ БУДЕТ НЕ ЛЕКАРСТВОМ, ПРОСТО СКАЖИ, ЧТО ЭТО НЕ ЛЕКАРСТВО И Я [область применения]\n"
                "Кратко ответь строго в формате одной строки без переносов:\n"
                "Применять при болях в: [где применять], "
                "Диагнозы: [минимум 3 диагноза], "
                "Действующее вещество: [вещество], "
                "Аналоги: [минимум 3 аналога]"
            ),
        }

        self._current_index = 0
        self.g4f_client = Client() if Client is not None else None

    def _get_enabled_apis(self) -> List[str]:
        """Возвращает актуальный список активных API провайдеров"""
        self._enable_api()
        return [
            name
            for name, config in self.api_config.items()
            if config.get("enabled", True)
        ]

    def _disable_provider(self, provider_name: str, cooldown_minutes: int = 5):
        """Временно отключает провайдер при возникновении ошибок"""
        if provider_name in self.api_config:
            self.api_config[provider_name]["enabled"] = False
            self.api_config[provider_name]["enable_time"] = (
                datetime.datetime.now() + datetime.timedelta(minutes=cooldown_minutes)
            )
            logger.warning(f"Провайдер {provider_name} отключен на {cooldown_minutes} минут из-за ошибки.")

    def _enable_api(self):
        """Восстанавливает ранее отключенные провайдеры по истечении таймаута"""
        now = datetime.datetime.now()
        for api_name, config in self.api_config.items():
            if config.get("enable_time") is not None and config["enable_time"] <= now:
                # Включаем только если есть необходимые ключи или провайдер бесплатный (g4f)
                if api_name == "g4f" or bool(config.get("api_key")):
                    config["enabled"] = True
                    config["enable_time"] = None
                    logger.info(f"Провайдер {api_name} восстановлен после таймаута.")

    def _call_deepinfra(self, drug_name: str) -> Optional[str]:
        """Синхронный запрос к DeepInfra API"""
        logger.info(f"Попытка запроса к DeepInfra для {drug_name}")
        api_key = self.api_config["deepinfra"]["api_key"]
        if not api_key:
            return None

        prompt = self.prompt_templates["basic"] + f"\nНазвание лекарства: {drug_name}"
        payload = {
            "input": prompt,
            "stop": ["<|eot_id|>"],
            "stream": False,
        }
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

        try:
            response = requests.post(
                self.api_config["deepinfra"]["url"],
                json=payload,
                headers=headers,
                timeout=20,
            )
            if response.status_code != 200:
                logger.error(f"DeepInfra API error: {response.status_code}")
                self._disable_provider("deepinfra")
                return None

            result = response.json()
            return result.get("results", [{}])[0].get("generated_text", "").strip()

        except Exception as e:
            logger.error(f"DeepInfra error: {str(e)}")
            self._disable_provider("deepinfra")
            return None

    def _call_together(self, drug_name: str) -> Optional[str]:
        """Запрос к Together API"""
        logger.info(f"Попытка запроса к Together для {drug_name}")
        api_key = self.api_config["together"]["api_key"]
        if not api_key:
            return None

        if Together is None:
            logger.error("Пакет 'together' не установлен")
            self._disable_provider("together")
            return None

        try:
            client = Together(api_key=api_key)
            response = client.chat.completions.create(
                model=self.api_config["together"]["model"],
                messages=[{
                    "role": "user",
                    "content": self.prompt_templates["basic"] + drug_name,
                }],
                timeout=20,
            )
            return response.choices[0].message.content

        except Exception as e:
            logger.error(f"Together error: {str(e)}")
            self._disable_provider("together")
            return None

    def _call_fireworks(self, drug_name: str) -> Optional[str]:
        """Запрос к Fireworks API"""
        logger.info(f"Попытка запроса к Fireworks для {drug_name}")
        api_key = self.api_config["fireworks"]["api_key"]
        if not api_key:
            return None

        payload = {
            "model": self.api_config["fireworks"]["model"],
            "messages": [{
                "role": "user",
                "content": self.prompt_templates["strict"].format(drug_name=drug_name),
            }],
            "max_tokens": 200,
            "temperature": 0.6,
        }

        try:
            response = requests.post(
                self.api_config["fireworks"]["url"],
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=20,
            )
            response.raise_for_status()
            data = response.json()
            return data["choices"][0]["message"]["content"]

        except Exception as e:
            logger.error(f"Fireworks error: {str(e)}")
            self._disable_provider("fireworks")
            return None

    def _call_g4f(self, drug_name: str) -> Optional[str]:
        """Запрос через GPT4Free"""
        logger.info(f"Попытка запроса через GPT4Free для {drug_name}")
        if self.g4f_client is None:
            logger.error("Пакет 'g4f' не установлен")
            self._disable_provider("g4f")
            return None

        try:
            response = self.g4f_client.chat.completions.create(
                model=self.api_config["g4f"]["model"],
                messages=[{
                    "role": "user",
                    "content": self.prompt_templates["basic"] + f"\nНазвание лекарства: {drug_name}",
                }],
                timeout=20,
            )
            return response.choices[0].message.content

        except Exception as e:
            logger.error(f"GPT4Free error: {str(e)}")
            self._disable_provider("g4f")
            return None

    def _dispatch_call(self, api_name: str, drug_name: str) -> Optional[str]:
        """Вызывает конкретный метод API по имени"""
        if api_name == "deepinfra":
            return self._call_deepinfra(drug_name)
        elif api_name == "together":
            return self._call_together(drug_name)
        elif api_name == "fireworks":
            return self._call_fireworks(drug_name)
        elif api_name == "g4f":
            return self._call_g4f(drug_name)
        return None

    def get_drug_info(self, drug_name: str) -> Optional[str]:
        """
        Основной метод для получения информации о лекарстве
        с автоматическим переключением между API при ошибках (динамический Round Robin)
        """
        enabled_apis = self._get_enabled_apis()
        if not enabled_apis:
            logger.error("Все API отключены из-за ошибок или отсутствия конфигурации")
            return None

        # Упорядочиваем активные API начиная с текущего индекса (round robin)
        start_idx = self._current_index % len(enabled_apis)
        ordered_apis = enabled_apis[start_idx:] + enabled_apis[:start_idx]
        self._current_index = (self._current_index + 1) % len(enabled_apis)

        for api_name in ordered_apis:
            if not self.api_config[api_name].get("enabled", True):
                continue

            logger.info(f"Используем API: {api_name}")

            try:
                result = self._dispatch_call(api_name, drug_name)

                if result is not None and result != "" and "Извините" not in result:
                    return result.strip()

                logger.warning(f"API {api_name} вернул пустой результат, пробуем следующий")

            except Exception as e:
                logger.error(f"Неожиданная ошибка в {api_name}: {str(e)}")
                self._disable_provider(api_name)
                continue

        logger.error("Не удалось получить ответ ни от одного API")
        return None
