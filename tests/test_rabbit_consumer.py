import unittest
from unittest.mock import MagicMock
import sys
import types
import json

# Ensure all 3rd party dependencies are mocked if not present in test runner
mock_mods = ["confluent_kafka", "qdrant_client", "sentence_transformers", "together", "pika", "pika.adapters.blocking_connection", "pika.spec", "g4f", "tenacity"]
for mod_name in mock_mods:
    if mod_name not in sys.modules:
        m = types.ModuleType(mod_name)
        m.__path__ = []
        sys.modules[mod_name] = m

sys.modules["pika"].ConnectionParameters = MagicMock
sys.modules["pika"].BlockingConnection = MagicMock
sys.modules["pika.adapters.blocking_connection"].BlockingChannel = MagicMock
sys.modules["pika.spec"].BasicProperties = MagicMock

from app.Classes.classes import Drug, SearchQueryMessage
from app.mq.rabbit_consumer import RabbitMQConsumer


class TestSearchQueryAndRabbitConsumer(unittest.TestCase):
    def setUp(self):
        self.mock_vector_service = MagicMock()
        self.mock_vector_service.vectorize_text.return_value = [0.1, 0.2, 0.3]
        self.mock_qdrant_service = MagicMock()
        self.mock_qdrant_service.search_vector.return_value = [{"id": "123", "name": "Парацетамол"}]
        self.consumer = RabbitMQConsumer(self.mock_vector_service, self.mock_qdrant_service)

    def test_search_query_message_from_plain_text(self):
        msg = SearchQueryMessage.from_message("Аспирин")
        self.assertEqual(msg.query, "Аспирин")
        self.assertEqual(msg.limit, 5)

    def test_search_query_message_from_json_string(self):
        json_str = json.dumps({"query": "Ибупрофен 400мг", "limit": 10, "filters": {"country": "RU"}})
        msg = SearchQueryMessage.from_message(json_str)
        self.assertEqual(msg.query, "Ибупрофен 400мг")
        self.assertEqual(msg.limit, 10)
        self.assertEqual(msg.filters, {"country": "RU"})

    def test_process_message_passes_clean_query_to_vectorizer(self):
        raw_payload = json.dumps({"query": "Парацетамол", "limit": 3})
        result = self.consumer._process_message(raw_payload)

        # Ensure vectorize_text receives ONLY the query string, NOT the JSON
        self.mock_vector_service.vectorize_text.assert_called_with("Парацетамол")
        self.mock_qdrant_service.search_vector.assert_called_with(
            vector=[0.1, 0.2, 0.3],
            limit=3,
            score_threshold=None
        )
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["query"], "Парацетамол")

    def test_drug_from_json_preserves_canonical_id(self):
        test_id = "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d"
        data = {
            "Id": test_id,
            "Name": "Аспирин",
            "Manufacturer": "Байер",
            "CountryCodeId": "DE"
        }
        drug = Drug.from_json(data)
        self.assertEqual(drug.id, test_id)
        self.assertEqual(drug.name, "Аспирин")

    def test_on_message_callback_success_acks_message(self):
        channel = MagicMock()
        method = MagicMock(delivery_tag=42)
        properties = MagicMock(message_id="msg-1", reply_to="reply_queue", correlation_id="corr-1")
        body = json.dumps({"query": "Анальгин"}).encode("utf-8")

        self.consumer._on_message_callback(channel, method, properties, body)

        channel.basic_ack.assert_called_once_with(delivery_tag=42)
        channel.basic_publish.assert_called_once()

    def test_on_message_callback_error_nacks_with_requeue_false(self):
        channel = MagicMock()
        method = MagicMock(delivery_tag=99)
        properties = MagicMock(message_id="msg-fail", reply_to="reply_queue", correlation_id="corr-2")
        # Simulate processing error
        self.consumer._process_message = MagicMock(side_effect=RuntimeError("Qdrant unavailable"))
        body = b"poison pill"

        self.consumer._on_message_callback(channel, method, properties, body)

        # MUST be requeue=False to prevent poison pill loop
        channel.basic_nack.assert_called_once_with(delivery_tag=99, requeue=False)
        channel.basic_publish.assert_called_once()
        sent_body = json.loads(channel.basic_publish.call_args[1]["body"])
        self.assertEqual(sent_body["status"], "error")


if __name__ == '__main__':
    unittest.main()
