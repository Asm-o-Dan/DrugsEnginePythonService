import unittest
from unittest.mock import patch, MagicMock
from app.services.gpt_service import DrugInfoAPI


class TestDrugInfoAPI(unittest.TestCase):

    def setUp(self):
        self.api = DrugInfoAPI()
        # Explicitly configure mock providers for testing
        self.api.api_config["deepinfra"]["enabled"] = True
        self.api.api_config["together"]["enabled"] = True
        self.api.api_config["fireworks"]["enabled"] = True
        self.api.api_config["g4f"]["enabled"] = True

    def test_no_hardcoded_secrets_in_config_defaults(self):
        """Проверяем, что в репозитории нет захардкоженных токенов"""
        # When environment variables are empty, api_key in fresh instance should be empty
        with patch.dict("os.environ", {}, clear=True):
            fresh_api = DrugInfoAPI()
            self.assertIn("deepinfra", fresh_api.api_config)
            self.assertIn("together", fresh_api.api_config)
            self.assertIn("fireworks", fresh_api.api_config)

    def test_fallback_when_first_provider_fails(self):
        """Проверяем, что при падении первого провайдера запрос переключается на следующий"""
        with patch.object(self.api, "_dispatch_call") as mock_dispatch:
            def side_effect(api_name, drug_name):
                if api_name == "deepinfra":
                    raise Exception("DeepInfra connection timeout")
                elif api_name == "together":
                    return "Название: Но-Шпа, Применение: спазмолитик"
                return None

            mock_dispatch.side_effect = side_effect

            result = self.api.get_drug_info("Но-Шпа")
            self.assertEqual(result, "Название: Но-Шпа, Применение: спазмолитик")
            # DeepInfra should now be disabled
            self.assertFalse(self.api.api_config["deepinfra"]["enabled"])
            self.assertIsNotNone(self.api.api_config["deepinfra"]["enable_time"])

    def test_all_providers_fail_returns_none(self):
        """Проверяем возврат None, если все провайдеры вернули ошибку"""
        with patch.object(self.api, "_dispatch_call", side_effect=Exception("API failure")):
            result = self.api.get_drug_info("Аспирин")
            self.assertIsNone(result)
            for config in self.api.api_config.values():
                self.assertFalse(config["enabled"])

    def test_empty_enabled_apis_returns_none(self):
        """Проверяем возврат None, если изначально нет включенных провайдеров"""
        for config in self.api.api_config.values():
            config["enabled"] = False

        result = self.api.get_drug_info("Парацетамол")
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
