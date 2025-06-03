import datetime
import logging
from together import Together
import requests
from typing import Optional, List, Dict
import itertools
from g4f.client import Client
from g4f.Provider import RetryProvider, Phind, Liaobots

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DrugInfoAPI:
    """Класс для работы с API генерации описаний лекарств с Round Robin стратегией"""

    def __init__(self):
        # Конфигурация API
        self.api_config = {
            "deepinfra": {
                "url": "https://api.deepinfra.com/v1/inference/deepseek-ai/DeepSeek-V3",
                "headers": {
                    "Authorization": "Bearer 6hxoDsvx7Kh5sMI5XZiV2n4Y11Gl3mt1",
                    "Content-Type": "application/json"
                },
                "enabled": True,
                "enable_time":None
            },
            "together": {
                "api_key": "40e343a9294ac61cf9b957ecbfcc78e728f413230f626e157e43bea3458fb360",
                "model": "deepseek-ai/DeepSeek-V3",
                "enabled": True,
                "enable_time": None
            },
            "fireworks": {
                "url": "https://api.fireworks.ai/inference/v1/chat/completions",
                "api_key": "fw_3ZLGuZa7QTR7LnJKiM2XfJ73",
                "model": "accounts/fireworks/models/llama-v3p1-8b-instruct",
                "enabled": True,
                "enable_time": None
            },
            "g4f": {
                "model": "gpt-4",
                "providers": [Phind, Liaobots],
                "enabled": True,
                "enable_time": None
            }
        }

        # Шаблоны промптов
        self.prompt_templates = {
            "basic": (
                "Ты — ассистент, который коротко и четко составляет описание лекарства. "
                "ЕСЛИ ТО ЧТО Я ПРИШЛЮ ТЕБЕ БУДЕТ НЕ ЛЕКАРСТВОМ, ПРОСТО СКАЖИ, ЧТО ЭТО ННЕ ЛЕКАРСТВО И Я [область применения]."
                "Формат ответа: Название: [название], Применение: [применение], "
                "Диагнозы: [диагнозы], Вещество: [вещество], Аналоги: [аналоги]. "
                "Ответь в 1 строке."
            ),
            "strict": (
                "Название лекарства: {drug_name}\n"
                "ЕСЛИ ТО ЧТО Я ПРИШЛЮ ТЕБЕ БУДЕТ НЕ ЛЕКАРСТВОМ, ПРОСТО СКАЖИ, ЧТО ЭТО ННЕ ЛЕКАРСТВО И Я [область применения]\n"
                "Кратко ответь строго в формате одной строки без переносов:\n"
                "Применять при болях в: [где применять], "
                "Диагнозы: [минимум 3 диагноза], "
                "Действующее вещество: [вещество], "
                "Аналоги: [минимум 3 аналога]"
            )
        }

        # Итератор для Round Robin
        self.api_cycle = itertools.cycle(self._get_enabled_apis())

        # Клиент для g4f
        self.g4f_client = Client()

    def _get_enabled_apis(self) -> List[str]:
        """Возвращает список включенных API"""
        return [name for name, config in self.api_config.items() if config.get('enabled', True)]

    def _call_deepinfra(self, drug_name: str) -> Optional[str]:
        """Синхронный запрос к DeepInfra API"""
        logger.info(f"Попытка запроса к DeepInfra для {drug_name}")

        prompt = self.prompt_templates["basic"] + f"\nНазвание лекарства: {drug_name}"
        payload = {
            "input": prompt,
            "stop": ["<|eot_id|>"],
            "stream": False
        }

        try:
            response = requests.post(
                self.api_config["deepinfra"]["url"],
                json=payload,
                headers=self.api_config["deepinfra"]["headers"],
                timeout=20
            )

            if response.status_code != 200:
                logger.error(f"DeepInfra API error: {response.status_code}")
                return None

            result = response.json()
            return result.get("results", [{}])[0].get("generated_text", "").strip()

        except Exception as e:
            logger.error(f"DeepInfra error: {str(e)}")
            self.api_config["deepinfra"]["enabled"] = False
            self.api_config["deepinfra"]["enable_time"] = datetime.datetime.now() + datetime.timedelta(minutes=5)
            return None

    def _call_together(self, drug_name: str) -> Optional[str]:
        """Запрос к Together API"""
        logger.info(f"Попытка запроса к Together для {drug_name}")

        try:
            client = Together(api_key=self.api_config["together"]["api_key"])
            response = client.chat.completions.create(
                model=self.api_config["together"]["model"],
                messages=[{
                    "role": "user",
                    "content": self.prompt_templates["basic"] + drug_name
                }],
                timeout=20
            )
            return response.choices[0].message.content

        except Exception as e:
            logger.error(f"Together error: {str(e)}")
            self.api_config["together"]["enabled"] = False
            self.api_config["together"]["enable_time"] = datetime.datetime.now() + datetime.timedelta(minutes=5)
            return None

    def _call_fireworks(self, drug_name: str) -> Optional[str]:
        """Запрос к Fireworks API"""
        logger.info(f"Попытка запроса к Fireworks для {drug_name}")

        payload = {
            "model": self.api_config["fireworks"]["model"],
            "messages": [{
                "role": "user",
                "content": self.prompt_templates["strict"].format(drug_name=drug_name)
            }],
            "max_tokens": 200,
            "temperature": 0.6
        }

        try:
            response = requests.post(
                self.api_config["fireworks"]["url"],
                headers={
                    "Authorization": f"Bearer {self.api_config['fireworks']['api_key']}",
                    "Content-Type": "application/json"
                },
                json=payload,
                timeout=20
            )
            response.raise_for_status()
            data = response.json()
            return data['choices'][0]['message']['content']

        except Exception as e:
            logger.error(f"Fireworks error: {str(e)}")
            self.api_config["fireworks"]["enabled"] = False
            self.api_config["fireworks"]["enable_time"] = datetime.datetime.now() + datetime.timedelta(minutes=5)
            return None

    def _call_g4f(self, drug_name: str) -> Optional[str]:
        """Запрос через GPT4Free"""
        logger.info(f"Попытка запроса через GPT4Free для {drug_name}")

        try:
            response = self.g4f_client.chat.completions.create(
                model=self.api_config["g4f"]["model"],
                messages=[{
                    "role": "user",
                    "content": self.prompt_templates["basic"] + f"\nНазвание лекарства: {drug_name}"
                }],
                timeout=20
            )
            return response.choices[0].message.content

        except Exception as e:
            logger.error(f"GPT4Free error: {str(e)}")
            self.api_config["g4f"]["enabled"] = False
            self.api_config["g4f"]["enable_time"] = datetime.datetime.now() + datetime.timedelta(minutes=5)
            return None

    def _enable_api(self):
        api_names = [name for name, config in self.api_config.items()]
        for api_name in api_names:
            if self.api_config[api_name]["enable_time"] is not None:
                if self.api_config[api_name]["enable_time"] <= datetime.datetime.now():
                    self.api_config[api_name]["enabled"] = True
                    self.api_config[api_name]["enable_time"] = None


    def get_drug_info(self, drug_name: str) -> Optional[str]:
        """
        Основной метод для получения информации о лекарстве
        с автоматическим переключением между API при ошибках
        """
        self._enable_api()
        enabled_apis = self._get_enabled_apis()
        if not enabled_apis:
            logger.error("Все API отключены из-за ошибок")
            return None

        # Пробуем каждый доступный API по кругу
        for _ in range(len(enabled_apis)):
            api_name = next(self.api_cycle)

            if not self.api_config[api_name]["enabled"]:
                continue

            logger.info(f"Используем API: {api_name}")

            try:
                if api_name == "deepinfra":
                    result = self._call_deepinfra(drug_name)
                elif api_name == "together":
                    result = self._call_together(drug_name)
                elif api_name == "fireworks":
                    result = self._call_fireworks(drug_name)
                elif api_name == "g4f":
                    result = self._call_g4f(drug_name)
                else:
                    continue

                if result is not None and result != "" and "Извините" not in result :
                    return result

                logger.warning(f"API {api_name} вернул None, пробуем следующий")

            except Exception as e:
                logger.error(f"Неожиданная ошибка в {api_name}: {str(e)}")
                self.api_config[api_name]["enabled"] = False
                continue

        logger.error("Не удалось получить ответ ни от одного API")
        return None
