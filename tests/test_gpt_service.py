import json
import unittest
from unittest.mock import MagicMock, patch

from app.services.gpt_service import DrugInfoAPI


class TestDrugInfoAPI(unittest.TestCase):

    def test_no_api_key_initializes_cleanly(self):
        """Проверяем корректную инициализацию без заданного GEMINI_API_KEY"""
        with patch("app.services.gpt_service.GEMINI_API_KEY", None):
            api = DrugInfoAPI()
            self.assertIsNone(api._client)
            self.assertEqual(api.get_batch_drug_info(["Но-Шпа"]), [])
            self.assertIsNone(api.get_drug_info("Но-Шпа"))

    def test_model_pool_rotation(self):
        """Проверяем корректную ротацию пула моделей для распределения квот"""
        with patch("app.services.gpt_service.GEMINI_API_KEY", "test-key"), \
             patch("google.genai.Client"):
            api = DrugInfoAPI()
            self.assertEqual(len(api._models), 3)
            order1 = api._get_active_models_order()
            order2 = api._get_active_models_order()
            self.assertNotEqual(order1[0], order2[0])

    def test_get_batch_drug_info_success(self):
        """Проверяем успешную обработку пакета лекарств через Gemini API"""
        mock_response_data = [
            {
                "is_drug": True,
                "drug_name": "Но-Шпа",
                "active_ingredient": "Дротаверин",
                "indications": ["Спазм гладкой мускулатуры", "Почечная колика", "Спастический колит"],
                "analogs": ["Дротаверин", "Спазмол", "Спазмонет"],
                "description": "Применение: Но-Шпа, Диагнозы: Спазм гладкой мускулатуры, Действующее вещество: Дротаверин, Аналоги: Дротаверин, Спазмол, Спазмонет",
                "status": "VERIFIED"
            }
        ]
        mock_resp = MagicMock()
        mock_resp.text = json.dumps(mock_response_data)

        with patch("app.services.gpt_service.GEMINI_API_KEY", "test-key"), \
             patch("google.genai.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client.models.generate_content.return_value = mock_resp
            mock_client_cls.return_value = mock_client

            api = DrugInfoAPI()
            result = api.get_batch_drug_info(["Но-Шпа"])
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]["drug_name"], "Но-Шпа")
            self.assertEqual(result[0]["status"], "VERIFIED")

    def test_get_drug_info_single_item(self):
        """Проверяем метод get_drug_info для одного лекарства"""
        mock_response_data = [
            {
                "is_drug": True,
                "drug_name": "Аспирин",
                "active_ingredient": "Ацетилсалициловая кислота",
                "indications": ["Лихорадка", "Головная боль", "Воспаление"],
                "analogs": ["Ацекардол", "Тромбо АСС", "Кардиомагнил"],
                "description": "Применение: Аспирин, Диагнозы: Лихорадка, Действующее вещество: Ацетилсалициловая кислота, Аналоги: Ацекардол",
                "status": "VERIFIED"
            }
        ]
        with patch.object(DrugInfoAPI, "get_batch_drug_info", return_value=mock_response_data):
            with patch("app.services.gpt_service.GEMINI_API_KEY", "test-key"), \
                 patch("google.genai.Client"):
                api = DrugInfoAPI()
                info = api.get_drug_info("Аспирин")
                self.assertIsNotNone(info)
                self.assertIn("Аспирин", info)

    def test_fallback_on_model_error(self):
        """Проверяем переключение на следующую модель в пуле при ошибке первой модели"""
        mock_resp = MagicMock()
        mock_resp.text = json.dumps([{"is_drug": True, "drug_name": "Парацетамол", "status": "VERIFIED"}])

        with patch("app.services.gpt_service.GEMINI_API_KEY", "test-key"), \
             patch("google.genai.Client") as mock_client_cls:
            mock_client = MagicMock()
            
            call_count = 0
            def side_effect(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count <= 2:
                    raise Exception("429 RESOURCE_EXHAUSTED: Rate limit exceeded")
                return mock_resp

            mock_client.models.generate_content.side_effect = side_effect
            mock_client_cls.return_value = mock_client

            api = DrugInfoAPI()
            with patch("time.sleep"):  # Не замедляем тесты
                res = api.get_batch_drug_info(["Парацетамол"])
                self.assertEqual(len(res), 1)
                self.assertEqual(res[0]["drug_name"], "Парацетамол")


if __name__ == "__main__":
    unittest.main()
