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

    def test_handle_kafka_error_generic_error_does_not_raise(self):
        """Verify that generic broker errors are logged without unhandled exceptions."""
        mock_error = MagicMock()
        mock_error.code.return_value = -1

        try:
            self.consumer._handle_kafka_error(mock_error)
        except Exception as e:
            self.fail(f"_handle_kafka_error failed on generic error: {e}")


if __name__ == '__main__':
    unittest.main()
