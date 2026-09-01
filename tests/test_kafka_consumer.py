import unittest
from unittest.mock import MagicMock, patch
import sys
import types

# Ensure all 3rd party dependencies are mocked if not present in test runner
mock_mods = ["confluent_kafka", "qdrant_client", "sentence_transformers", "together", "pika", "g4f", "tenacity"]
for mod_name in mock_mods:
    if mod_name not in sys.modules:
        m = types.ModuleType(mod_name)
        m.__path__ = []
        sys.modules[mod_name] = m

for sub_name in ["qdrant_client.models", "qdrant_client.http", "qdrant_client.http.exceptions", "g4f.client", "g4f.Provider"]:
    if sub_name not in sys.modules:
        sub_m = types.ModuleType(sub_name)
        sub_m.__path__ = []
        sys.modules[sub_name] = sub_m

def noop_decorator(*args, **kwargs):
    def decorator(fn):
        return fn
    return decorator

tenacity_mod = sys.modules["tenacity"]
tenacity_mod.retry = noop_decorator
tenacity_mod.Retrying = MagicMock
tenacity_mod.stop_after_attempt = MagicMock
tenacity_mod.wait_exponential = MagicMock
tenacity_mod.retry_if_exception_type = MagicMock

sys.modules["sentence_transformers"].SentenceTransformer = MagicMock
sys.modules["together"].Together = MagicMock
sys.modules["pika"].BlockingConnection = MagicMock
sys.modules["pika"].ConnectionParameters = MagicMock
sys.modules["pika"].PlainCredentials = MagicMock
sys.modules["g4f.client"].Client = MagicMock
sys.modules["g4f"].client = sys.modules["g4f.client"]
sys.modules["g4f.Provider"].RetryProvider = MagicMock
sys.modules["g4f.Provider"].Phind = MagicMock
sys.modules["g4f.Provider"].Liaobots = MagicMock
sys.modules["g4f"].Provider = sys.modules["g4f.Provider"]

models_mod = sys.modules["qdrant_client.models"]
models_mod.PointStruct = MagicMock
models_mod.VectorParams = MagicMock
models_mod.Distance = MagicMock
models_mod.ScoredPoint = MagicMock
models_mod.Filter = MagicMock

exceptions_mod = sys.modules["qdrant_client.http.exceptions"]
exceptions_mod.UnexpectedResponse = Exception
exceptions_mod.ResponseHandlingException = Exception

http_mod = sys.modules["qdrant_client.http"]
http_mod.exceptions = exceptions_mod

sys.modules["qdrant_client"].models = models_mod
sys.modules["qdrant_client"].http = http_mod
sys.modules["qdrant_client"].QdrantClient = MagicMock

class FakeKafkaError:
    _PARTITION_EOF = -191
    UNKNOWN_TOPIC_OR_PART = 3
    _UNKNOWN_TOPIC = -168
    _UNKNOWN_PARTITION = -190
    def __init__(self, code=-191):
        self._code = code
    def code(self):
        return self._code

sys.modules["confluent_kafka"].KafkaError = FakeKafkaError
sys.modules["confluent_kafka"].Consumer = MagicMock
sys.modules["confluent_kafka"].KafkaException = Exception
sys.modules["confluent_kafka"].Message = MagicMock

from app.mq.kafka_consumer import KafkaDrugConsumer, KafkaError


class TestKafkaConsumerErrorHandler(unittest.TestCase):
    def setUp(self):
        self.mock_vector = MagicMock()
        self.mock_qdrant = MagicMock()
        self.mock_api = MagicMock()
        with patch.object(KafkaDrugConsumer, '_configure_consumer', return_value=MagicMock()):
            self.consumer = KafkaDrugConsumer(
                vector_service=self.mock_vector,
                qdrant_service=self.mock_qdrant,
                api_service=self.mock_api
            )

    def test_handle_kafka_error_partition_eof_does_not_raise(self):
        """Verify that partition EOF error is handled gracefully without NameError."""
        mock_error = MagicMock()
        mock_error.code.return_value = FakeKafkaError._PARTITION_EOF

        try:
            self.consumer._handle_kafka_error(mock_error)
        except NameError as e:
            self.fail(f"_handle_kafka_error raised NameError unexpectedly: {e}")

    def test_handle_kafka_error_unknown_topic_warns_cleanly(self):
        """Verify that unknown topic error (code 3) is handled gracefully with throttled warning."""
        mock_error = MagicMock()
        mock_error.code.return_value = 3

        try:
            self.consumer._handle_kafka_error(mock_error)
        except Exception as e:
            self.fail(f"_handle_kafka_error failed on unknown topic error: {e}")

    def test_handle_kafka_error_generic_error_does_not_raise(self):
        """Verify that generic broker errors are logged without unhandled exceptions."""
        mock_error = MagicMock()
        mock_error.code.return_value = -1

        try:
            self.consumer._handle_kafka_error(mock_error)
        except Exception as e:
            self.fail(f"_handle_kafka_error failed on generic error: {e}")


class TestKafkaConsumerConfiguration(unittest.TestCase):
    def setUp(self):
        self.mock_vector = MagicMock()
        self.mock_qdrant = MagicMock()
        self.mock_api = MagicMock()

    def test_default_topic_and_broker(self):
        """Verify that default topic is 'drugs' and broker is 'kafka:9092' when no override is given."""
        with patch("app.mq.kafka_consumer.Consumer") as mock_consumer_cls:
            mock_consumer_instance = MagicMock()
            mock_consumer_cls.return_value = mock_consumer_instance

            consumer = KafkaDrugConsumer(
                vector_service=self.mock_vector,
                qdrant_service=self.mock_qdrant,
                api_service=self.mock_api
            )

            self.assertEqual(consumer.topic, "drugs")
            self.assertEqual(consumer.broker, "kafka:9092")
            mock_consumer_instance.subscribe.assert_called_once_with(["drugs"])

    def test_custom_topic_and_broker_passed(self):
        """Verify that custom topic and broker passed to constructor are applied to Kafka consumer."""
        with patch("app.mq.kafka_consumer.Consumer") as mock_consumer_cls:
            mock_consumer_instance = MagicMock()
            mock_consumer_cls.return_value = mock_consumer_instance

            consumer = KafkaDrugConsumer(
                vector_service=self.mock_vector,
                qdrant_service=self.mock_qdrant,
                api_service=self.mock_api,
                topic="custom_topic",
                broker="localhost:9093"
            )

            self.assertEqual(consumer.topic, "custom_topic")
            self.assertEqual(consumer.broker, "localhost:9093")
            mock_consumer_cls.assert_called_once()
            call_conf = mock_consumer_cls.call_args[0][0]
            self.assertEqual(call_conf["bootstrap.servers"], "localhost:9093")
            mock_consumer_instance.subscribe.assert_called_once_with(["custom_topic"])

    def test_whitespace_and_empty_topic_and_broker_fallback(self):
        """Verify that whitespace or empty strings passed as topic/broker fallback to defaults."""
        with patch("app.mq.kafka_consumer.Consumer") as mock_consumer_cls:
            mock_consumer_instance = MagicMock()
            mock_consumer_cls.return_value = mock_consumer_instance

            consumer = KafkaDrugConsumer(
                vector_service=self.mock_vector,
                qdrant_service=self.mock_qdrant,
                api_service=self.mock_api,
                topic="   ",
                broker="   "
            )

            self.assertEqual(consumer.topic, "drugs")
            self.assertEqual(consumer.broker, "kafka:9092")
            mock_consumer_instance.subscribe.assert_called_once_with(["drugs"])

    def test_start_kafka_consumer_passes_topic_and_broker(self):
        """Verify that start_kafka_consumer properly passes custom topic and broker to KafkaDrugConsumer."""
        with patch("app.mq.kafka_consumer.KafkaDrugConsumer") as mock_consumer_cls, \
             patch("app.mq.kafka_consumer.DrugInfoAPI") as mock_api_cls:
            mock_inst = MagicMock()
            mock_consumer_cls.return_value = mock_inst
            from app.mq.kafka_consumer import start_kafka_consumer

            start_kafka_consumer(
                self.mock_vector,
                self.mock_qdrant,
                topic="special_topic",
                broker="kafka:9094"
            )

            mock_consumer_cls.assert_called_once()
            _, kwargs = mock_consumer_cls.call_args
            self.assertEqual(kwargs.get("topic"), "special_topic")
            self.assertEqual(kwargs.get("broker"), "kafka:9094")
            mock_inst.run_consumption_loop.assert_called_once()


class TestKafkaConfigAliases(unittest.TestCase):
    def test_config_env_alias_resolution(self):
        """Verify that config.py resolves Kafka__BootstrapServers and Kafka__Topic environment variables."""
        import importlib
        import os
        import app.config as cfg

        with patch.dict(os.environ, {
            "KAFKA_BROKER": "",
            "KAFKA_BOOTSTRAP_SERVERS": "",
            "Kafka__BootstrapServers": "broker-override:9092",
            "KAFKA_TOPIC": "",
            "Kafka__Topic": "topic-override"
        }):
            importlib.reload(cfg)
            self.assertEqual(cfg.KAFKA_BROKER, "broker-override:9092")
            self.assertEqual(cfg.KAFKA_TOPIC, "topic-override")

        # Reload back to clean state
        with patch.dict(os.environ, {}, clear=True):
            importlib.reload(cfg)

    def test_config_whitespace_fallback_to_next_alias(self):
        """Verify that whitespace in primary alias falls back to secondary alias instead of default."""
        import importlib
        import os
        import app.config as cfg

        with patch.dict(os.environ, {
            "KAFKA_BROKER": "   ",
            "KAFKA_BOOTSTRAP_SERVERS": "secondary-broker:9092",
            "KAFKA_TOPIC": "   ",
            "Kafka__Topic": "secondary-topic"
        }):
            importlib.reload(cfg)
            self.assertEqual(cfg.KAFKA_BROKER, "secondary-broker:9092")
            self.assertEqual(cfg.KAFKA_TOPIC, "secondary-topic")

        with patch.dict(os.environ, {}, clear=True):
            importlib.reload(cfg)

    def test_config_colon_alias_resolution(self):
        """Verify that config.py resolves Kafka:BootstrapServers and Kafka:Topic environment variables."""
        import importlib
        import os
        import app.config as cfg

        with patch.dict(os.environ, {
            "KAFKA_BROKER": "",
            "KAFKA_BOOTSTRAP_SERVERS": "",
            "Kafka__BootstrapServers": "",
            "Kafka:BootstrapServers": "colon-broker:9092",
            "KAFKA_TOPIC": "",
            "Kafka__Topic": "",
            "Kafka:Topic": "colon-topic"
        }):
            importlib.reload(cfg)
            self.assertEqual(cfg.KAFKA_BROKER, "colon-broker:9092")
            self.assertEqual(cfg.KAFKA_TOPIC, "colon-topic")

        with patch.dict(os.environ, {}, clear=True):
            importlib.reload(cfg)


class TestKafkaMessageProcessing(unittest.TestCase):
    def test_parse_message_tombstone_returns_none(self):
        """Verify that tombstone (null value) message returns None gracefully."""
        mock_msg = MagicMock()
        mock_msg.value.return_value = None

        res = KafkaDrugConsumer._parse_message(mock_msg)
        self.assertIsNone(res)

    def test_parse_message_invalid_json_returns_none(self):
        """Verify that invalid JSON string returns None without raising."""
        mock_msg = MagicMock()
        mock_msg.value.return_value = b"{invalid-json"

        res = KafkaDrugConsumer._parse_message(mock_msg)
        self.assertIsNone(res)

    def test_parse_message_invalid_utf8_returns_none(self):
        """Verify that invalid UTF-8 bytes return None without crashing."""
        mock_msg = MagicMock()
        mock_msg.value.return_value = b"\xff\xfe\xfd"

        res = KafkaDrugConsumer._parse_message(mock_msg)
        self.assertIsNone(res)

    def test_parse_message_str_value_parsed_correctly(self):
        """Verify that string (non-bytes) value is parsed correctly."""
        mock_msg = MagicMock()
        mock_msg.value.return_value = '{"id": "test-id-1", "name": "Aspirin 100mg"}'

        drug = KafkaDrugConsumer._parse_message(mock_msg)
        self.assertNotNull = self.assertIsNotNone(drug)
        self.assertEqual(drug.name, "Aspirin 100mg")
        self.assertEqual(drug.id, "test-id-1")

    def test_parse_message_non_str_non_bytes_returns_none(self):
        """Verify that non-string and non-bytes value returns None without crashing."""
        mock_msg = MagicMock()
        mock_msg.value.return_value = 12345

        res = KafkaDrugConsumer._parse_message(mock_msg)
        self.assertIsNone(res)

    def test_process_batch_empty_returns_empty_list(self):
        """Verify that processing an empty batch returns an empty list immediately."""
        mock_vector = MagicMock()
        mock_qdrant = MagicMock()
        mock_api = MagicMock()
        with patch.object(KafkaDrugConsumer, '_configure_consumer', return_value=MagicMock()):
            consumer = KafkaDrugConsumer(
                vector_service=mock_vector,
                qdrant_service=mock_qdrant,
                api_service=mock_api
            )
            result = consumer._process_batch([])
            self.assertEqual(result, [])


if __name__ == '__main__':
    unittest.main()
