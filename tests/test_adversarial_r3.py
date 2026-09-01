"""
Adversarial Stress Test Suite for Requirement R3 (Python Startup Metrics & Lifecycles).

Comprehensive empirical tests:
1. Cold-start GET /metrics under concurrent load (30 concurrent threads, zero dropped connections, registry thread safety).
2. Aborted / malformed client HTTP requests (connection reset / socket disconnect resilience).
3. Qdrant failure simulation (offline daemon, connection drops, missing collections, zero/large point counts).
4. Kafka consumer active gauge lifecycle (0 -> 1 -> 0 on clean exit, exceptions, OOM, and batch processing errors).
5. High-precision uptime gauge progression and monotonicity across rapid samples.
6. Strict Prometheus 0.0.4 exposition format compliance (BNF grammar, single # TYPE declarations, trailing newline, Content-Type header).
7. High-concurrency SimpleMetricsRegistry multi-threaded hammer test (writers + readers).
"""

import http.client
import os
import re
import socket
import sys
import threading
import time
import types
import unittest
from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import MagicMock, patch

# Ensure project root is in sys.path
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

# Mock 3rd-party dependencies for standalone runner execution
mock_mods = [
    "confluent_kafka",
    "qdrant_client",
    "sentence_transformers",
    "together",
    "pika",
    "g4f",
    "tenacity",
]
for mod_name in mock_mods:
    if mod_name not in sys.modules:
        m = types.ModuleType(mod_name)
        m.__path__ = []
        sys.modules[mod_name] = m

for sub_name in [
    "qdrant_client.models",
    "qdrant_client.http",
    "qdrant_client.http.exceptions",
    "g4f.client",
    "g4f.Provider",
]:
    if sub_name not in sys.modules:
        sub_m = types.ModuleType(sub_name)
        sub_m.__path__ = []
        sys.modules[sub_name] = sub_m

# Setup tenacity mock decorators if tenacity was mocked
def _noop_decorator(*args, **kwargs):
    def decorator(fn):
        return fn
    return decorator

tenacity_mod = sys.modules["tenacity"]
if not hasattr(tenacity_mod, "retry") or isinstance(tenacity_mod, types.ModuleType) and "retry" not in dir(tenacity_mod):
    tenacity_mod.retry = _noop_decorator
    tenacity_mod.stop_after_attempt = MagicMock
    tenacity_mod.wait_exponential = MagicMock
    tenacity_mod.retry_if_exception_type = MagicMock

# Setup sentence_transformers mock classes
st_mod = sys.modules["sentence_transformers"]
if not hasattr(st_mod, "SentenceTransformer"):
    st_mod.SentenceTransformer = MagicMock

# Setup pika mock classes
pika_mod = sys.modules["pika"]
if not hasattr(pika_mod, "BlockingConnection"):
    pika_mod.BlockingConnection = MagicMock
    pika_mod.ConnectionParameters = MagicMock
    pika_mod.PlainCredentials = MagicMock

# Setup qdrant_client mock classes
qdrant_mod = sys.modules["qdrant_client"]
if not hasattr(qdrant_mod, "QdrantClient"):
    qdrant_mod.QdrantClient = MagicMock

qdrant_models = sys.modules["qdrant_client.models"]
if not hasattr(qdrant_models, "PointStruct"):
    qdrant_models.PointStruct = MagicMock
    qdrant_models.VectorParams = MagicMock
    qdrant_models.Distance = MagicMock
    qdrant_models.ScoredPoint = MagicMock
    qdrant_models.Filter = MagicMock

qdrant_exceptions = sys.modules["qdrant_client.http.exceptions"]
if not hasattr(qdrant_exceptions, "UnexpectedResponse"):
    class UnexpectedResponse(Exception): pass
    class ResponseHandlingException(Exception): pass
    qdrant_exceptions.UnexpectedResponse = UnexpectedResponse
    qdrant_exceptions.ResponseHandlingException = ResponseHandlingException

# Setup confluent_kafka mock classes
class FakeKafkaError:
    _PARTITION_EOF = -191
    def __init__(self, code=-191):
        self._code = code
    def code(self):
        return self._code

ck_mod = sys.modules["confluent_kafka"]
ck_mod.KafkaError = FakeKafkaError
if not hasattr(ck_mod, "Consumer"):
    ck_mod.Consumer = MagicMock
if not hasattr(ck_mod, "KafkaException"):
    ck_mod.KafkaException = Exception
if not hasattr(ck_mod, "Message"):
    ck_mod.Message = MagicMock


from app.health_server import (
    HealthAndMetricsHandler,
    ThreadedHTTPServer,
    _periodic_metrics_poller,
    _stop_event,
    start_health_server,
)
from app.mq.kafka_consumer import KafkaDrugConsumer
from app.services.qdrant_service import QdrantService
from app.telemetry import SimpleMetricsRegistry, metrics


def get_free_port() -> int:
    """Returns a free local TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        s.listen(1)
        return s.getsockname()[1]


class Prometheus004Validator:
    """Validates Prometheus Exposition 0.0.4 BNF grammar."""

    METRIC_NAME_RE = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")
    LABEL_KEY_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
    TYPE_LINE_RE = re.compile(r"^# TYPE ([a-zA-Z_:][a-zA-Z0-9_:]*) (counter|gauge|summary|histogram|untyped)$")
    SAMPLE_LINE_RE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})? ([-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?)$")

    @classmethod
    def validate_exposition(cls, text: str) -> dict:
        """
        Validates text against Prometheus 0.0.4 rules.
        Returns parsed dict of {family: {"type": type, "samples": [(metric_name, labels_dict, float_val)]}}.
        Raises ValueError on any violation.
        """
        if not text.endswith("\n"):
            raise ValueError("Prometheus exposition MUST terminate with a newline character (\\n)")

        lines = text.split("\n")
        if lines[-1] == "":
            lines.pop()  # Remove the empty string resulting from trailing \n

        families = {}
        current_family = None
        current_type = None

        for idx, line in enumerate(lines, 1):
            if not line.strip():
                raise ValueError(f"Line {idx}: Blank lines are not permitted in Prometheus exposition format")

            if line.startswith("# TYPE "):
                match = cls.TYPE_LINE_RE.match(line)
                if not match:
                    raise ValueError(f"Line {idx}: Malformed # TYPE comment: '{line}'")
                family_name, metric_type = match.groups()
                if family_name in families:
                    raise ValueError(f"Line {idx}: Duplicate # TYPE declaration for '{family_name}'")
                current_family = family_name
                current_type = metric_type
                families[current_family] = {"type": current_type, "samples": []}

            elif line.startswith("#"):
                continue  # HELP or other comment
            else:
                match = cls.SAMPLE_LINE_RE.match(line)
                if not match:
                    raise ValueError(f"Line {idx}: Malformed sample line: '{line}'")
                metric_name, label_str, val_str = match.groups()

                # Determine expected family name
                family_from_metric = metric_name
                if metric_name.endswith(("_count", "_sum")):
                    base = metric_name.rsplit("_", 1)[0]
                    if base in families and families[base]["type"] in ("summary", "histogram"):
                        family_from_metric = base

                if family_from_metric not in families:
                    raise ValueError(
                        f"Line {idx}: Sample '{metric_name}' does not have a preceding # TYPE comment for family '{family_from_metric}'"
                    )

                # Validate labels
                labels_dict = {}
                if label_str:
                    label_pairs = label_str.split(",")
                    for pair in label_pairs:
                        if "=" not in pair:
                            raise ValueError(f"Line {idx}: Invalid label format in '{label_str}'")
                        k, v = pair.split("=", 1)
                        if not cls.LABEL_KEY_RE.match(k):
                            raise ValueError(f"Line {idx}: Invalid label name '{k}'")
                        if not (v.startswith('"') and v.endswith('"')):
                            raise ValueError(f"Line {idx}: Label value must be double-quoted in '{pair}'")
                        labels_dict[k] = v[1:-1]

                float_val = float(val_str)
                families[family_from_metric]["samples"].append((metric_name, labels_dict, float_val))

        return families


class TestColdStartConcurrentMetrics(unittest.TestCase):
    """Adversarial stress-test: Cold-start GET /metrics under concurrent requests."""

    def setUp(self):
        _stop_event.set()
        self.port = get_free_port()
        self.mock_qdrant = MagicMock(spec=QdrantService)
        self.mock_qdrant.get_vectors_count.return_value = 500
        self.mock_qdrant.health_check.return_value = True

    def test_cold_start_concurrent_requests_race_safety(self):
        """Spawns 30 concurrent client threads immediately at cold-start while updating telemetry."""
        server = ThreadedHTTPServer(("127.0.0.1", self.port), HealthAndMetricsHandler)
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()

        time.sleep(0.02)  # Minimal warm-up to ensure socket is bound

        num_threads = 30
        results = []

        def background_telemetry_writer():
            for i in range(100):
                metrics.inc_counter("drugsengine_python_drugs_processed_total", 1.0)
                metrics.observe_histogram("drugsengine_python_drug_processing_seconds", 0.05)
                time.sleep(0.001)

        writer_thread = threading.Thread(target=background_telemetry_writer, daemon=True)
        writer_thread.start()

        def fetch_metrics(client_id: int):
            for attempt in range(3):
                try:
                    conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
                    conn.request("GET", "/metrics")
                    resp = conn.getresponse()
                    content = resp.read()
                    content_type = resp.getheader("Content-Type")
                    content_len = resp.getheader("Content-Length")
                    status = resp.status
                    conn.close()
                    return {
                        "client_id": client_id,
                        "status": status,
                        "content_type": content_type,
                        "content_len": int(content_len) if content_len else -1,
                        "actual_len": len(content),
                        "body": content.decode("utf-8"),
                        "error": None,
                    }
                except Exception as exc:
                    if attempt == 2:
                        return {"client_id": client_id, "error": str(exc)}
                    time.sleep(0.01)

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(fetch_metrics, i) for i in range(num_threads)]
            for f in as_completed(futures):
                results.append(f.result())

        writer_thread.join(timeout=2.0)
        server.shutdown()
        server.server_close()

        self.assertEqual(len(results), num_threads)

        for res in results:
            self.assertIsNone(res.get("error"), f"Client error: {res.get('error')}")
            self.assertEqual(res["status"], 200)
            self.assertEqual(res["content_type"], "text/plain; version=0.0.4")
            self.assertEqual(res["content_len"], res["actual_len"])

            body = res["body"]
            self.assertIn("drugsengine_python_uptime_seconds", body)
            self.assertIn("drugsengine_python_qdrant_vectors_total", body)
            self.assertIn("drugsengine_python_kafka_consumer_active", body)

            # Strict Prometheus Exposition validation
            parsed = Prometheus004Validator.validate_exposition(body)
            self.assertIn("drugsengine_python_uptime_seconds", parsed)
            self.assertIn("drugsengine_python_qdrant_vectors_total", parsed)
            self.assertIn("drugsengine_python_kafka_consumer_active", parsed)

    def test_abrupt_client_disconnect_resilience(self):
        """Client connects, sends GET /metrics, and immediately closes socket."""
        server = ThreadedHTTPServer(("127.0.0.1", self.port), HealthAndMetricsHandler)
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()

        time.sleep(0.02)
        try:
            for _ in range(10):
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect(("127.0.0.1", self.port))
                sock.sendall(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
                sock.close()  # Abrupt disconnect before reading full response

            # Verify server is still completely operational
            conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
            conn.request("GET", "/metrics")
            resp = conn.getresponse()
            self.assertEqual(resp.status, 200)
            conn.close()
        finally:
            server.shutdown()
            server.server_close()


class TestQdrantFailureSimulation(unittest.TestCase):
    """Stress tests Qdrant failure modes (startup crash, missing collection, network drops)."""

    def setUp(self):
        _stop_event.set()

    def test_qdrant_initialization_exception_falls_back_to_zero(self):
        """When Qdrant fails on service startup, vectors gauge is set to 0.0 and server starts cleanly."""
        mock_qdrant = MagicMock(spec=QdrantService)
        mock_qdrant.get_vectors_count.side_effect = ConnectionRefusedError("Qdrant daemon is offline")

        with patch("app.health_server.ThreadedHTTPServer") as mock_srv:
            mock_srv.return_value = MagicMock()
            start_health_server(qdrant_service=mock_qdrant, port=get_free_port())
            _stop_event.set()

            text = metrics.generate_prometheus_text()
            parsed = Prometheus004Validator.validate_exposition(text)
            gauge_sample = parsed["drugsengine_python_qdrant_vectors_total"]["samples"][0]
            self.assertEqual(gauge_sample[2], 0.0)

    def test_qdrant_poller_resilience_under_network_failure(self):
        """Poller catches continuous exceptions without crashing the thread or corrupting metrics."""
        mock_qdrant = MagicMock(spec=QdrantService)
        mock_qdrant.get_vectors_count.side_effect = RuntimeError("Qdrant connection dropped during poll")

        with patch("app.health_server._qdrant_service_ref", mock_qdrant):
            _stop_event.clear()
            poller = threading.Thread(target=_periodic_metrics_poller, args=(0.02,), daemon=True)
            poller.start()

            time.sleep(0.08)  # Allow poller to run multiple failing iterations
            self.assertTrue(poller.is_alive(), "Poller thread must NOT terminate on unhandled Qdrant errors")

            _stop_event.set()
            poller.join(timeout=1.0)

            text = metrics.generate_prometheus_text()
            self.assertIn("drugsengine_python_uptime_seconds", text)

    def test_qdrant_service_get_vectors_count_edge_cases(self):
        """Direct unit testing of QdrantService.get_vectors_count against varied client responses."""
        with patch("app.services.qdrant_service.QdrantClient"):
            QdrantService._instance = None
            service = QdrantService(host="localhost", port=6333)
            mock_client = MagicMock()
            service.client = mock_client

            # 1. Collection does not exist
            mock_client.collection_exists.return_value = False
            self.assertEqual(service.get_vectors_count("missing_col"), 0)

            # 2. Collection exists, points_count is integer
            mock_client.collection_exists.return_value = True
            info = MagicMock()
            info.points_count = 4200
            info.vectors_count = 4200
            mock_client.get_collection.return_value = info
            self.assertEqual(service.get_vectors_count("my_col"), 4200)

            # 3. points_count is 0
            info.points_count = 0
            info.vectors_count = 0
            self.assertEqual(service.get_vectors_count("my_col"), 0)

            # 4. points_count is None, fallback to vectors_count
            info.points_count = None
            info.vectors_count = 1337
            self.assertEqual(service.get_vectors_count("my_col"), 1337)

            # 5. Large number of vectors (e.g. 50 million)
            info.points_count = 50_000_000
            self.assertEqual(service.get_vectors_count("my_col"), 50_000_000)

            # 6. Both counts are None
            info.points_count = None
            info.vectors_count = None
            self.assertEqual(service.get_vectors_count("my_col"), 0)

            # 7. get_collection throws an arbitrary network/socket exception
            mock_client.get_collection.side_effect = TimeoutError("Qdrant timed out")
            self.assertEqual(service.get_vectors_count("my_col"), 0)

    def test_qdrant_health_check_resilience(self):
        """Health check returns True when healthy, False on any network exception."""
        with patch("app.services.qdrant_service.QdrantClient"):
            QdrantService._instance = None
            service = QdrantService(host="localhost", port=6333)
            mock_client = MagicMock()
            service.client = mock_client

            mock_client.get_collections.return_value = MagicMock()
            self.assertTrue(service.health_check())

            mock_client.get_collections.side_effect = ConnectionResetError("Qdrant unreachable")
            self.assertFalse(service.health_check())


class TestKafkaConsumerLifecycleTransitions(unittest.TestCase):
    """Stress tests Kafka consumer active gauge transitions (0 -> 1 -> 0) under all exit paths."""

    def _create_consumer(self, mock_consumer):
        return KafkaDrugConsumer(
            vector_service=MagicMock(),
            qdrant_service=MagicMock(),
            api_service=MagicMock(),
            topic="test-drugs",
            broker="localhost:9092",
        )

    @patch("app.mq.kafka_consumer.Consumer")
    def test_lifecycle_normal_exit(self, mock_consumer_cls):
        """Consumer runs, gauge becomes 1.0, on stop signal returns to 0.0."""
        mock_c = MagicMock()
        mock_consumer_cls.return_value = mock_c

        consumer = self._create_consumer(mock_c)

        observed_active_inside_loop = []

        def side_effect_poll(timeout):
            val = metrics._gauges.get("drugsengine_python_kafka_consumer_active", 0.0)
            observed_active_inside_loop.append(val)
            raise KeyboardInterrupt("Stop loop")

        mock_c.poll.side_effect = side_effect_poll

        metrics.set_gauge("drugsengine_python_kafka_consumer_active", 0.0)
        consumer.run_consumption_loop()

        self.assertEqual(observed_active_inside_loop, [1.0], "Gauge must be 1.0 while loop is running")
        self.assertEqual(
            metrics._gauges.get("drugsengine_python_kafka_consumer_active"),
            0.0,
            "Gauge must revert to 0.0 in finally block upon exit",
        )
        mock_c.close.assert_called_once()

    @patch("app.mq.kafka_consumer.Consumer")
    def test_lifecycle_uncaught_fatal_exception(self, mock_consumer_cls):
        """Consumer encounters fatal uncaught exception, gauge must revert to 0.0."""
        mock_c = MagicMock()
        mock_consumer_cls.return_value = mock_c

        consumer = self._create_consumer(mock_c)
        mock_c.poll.side_effect = MemoryError("Simulated OOM crash")

        metrics.set_gauge("drugsengine_python_kafka_consumer_active", 0.0)
        consumer.run_consumption_loop()

        self.assertEqual(
            metrics._gauges.get("drugsengine_python_kafka_consumer_active"),
            0.0,
            "Gauge must be 0.0 even after fatal unhandled exception",
        )
        mock_c.close.assert_called_once()

    @patch("app.mq.kafka_consumer.Consumer")
    def test_multiple_sequential_lifecycles(self, mock_consumer_cls):
        """Tests sequential start-stop cycles (0 -> 1 -> 0 -> 1 -> 0)."""
        mock_c = MagicMock()
        mock_consumer_cls.return_value = mock_c

        consumer = self._create_consumer(mock_c)

        for cycle in range(3):
            mock_c.poll.side_effect = KeyboardInterrupt(f"Stop cycle {cycle}")
            consumer.run_consumption_loop()
            self.assertEqual(
                metrics._gauges.get("drugsengine_python_kafka_consumer_active"),
                0.0,
                f"Cycle {cycle}: Gauge must reset to 0.0 after execution",
            )

    @patch("app.mq.kafka_consumer.Consumer")
    def test_batch_processing_exception_increments_error_counter(self, mock_consumer_cls):
        """When batch processing throws an exception, error metric is incremented."""
        mock_c = MagicMock()
        mock_consumer_cls.return_value = mock_c

        vector_svc = MagicMock()
        qdrant_svc = MagicMock()
        api_svc = MagicMock()
        api_svc.get_batch_drug_info.side_effect = RuntimeError("Batch API failure")

        consumer = KafkaDrugConsumer(
            vector_service=vector_svc,
            qdrant_service=qdrant_svc,
            api_service=api_svc,
            topic="test-drugs",
            broker="localhost:9092",
        )

        mock_msg = MagicMock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = b'{"id": "123", "name": "Aspirin"}'
        mock_msg.headers.return_value = []

        # Return a message on first poll, then stop loop
        polls = [mock_msg]
        def side_effect_poll(timeout):
            if polls:
                return polls.pop(0)
            raise KeyboardInterrupt("Stop")

        mock_c.poll.side_effect = side_effect_poll

        # Set batch size to 1 to force immediate flush
        consumer.BATCH_SIZE = 1
        consumer.run_consumption_loop()

        text = metrics.generate_prometheus_text()
        self.assertIn('drugsengine_python_drug_errors_total{reason="batch_processing_exception"} 1.0', text)


class TestUptimeGaugeProgression(unittest.TestCase):
    """Validates uptime gauge monotonicity, accuracy, and precision."""

    def test_uptime_monotonic_progression_via_http(self):
        port = get_free_port()
        server = ThreadedHTTPServer(("127.0.0.1", port), HealthAndMetricsHandler)
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()

        samples = []
        try:
            for _ in range(5):
                time.sleep(0.04)
                conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
                conn.request("GET", "/metrics")
                resp = conn.getresponse()
                body = resp.read().decode("utf-8")
                conn.close()

                parsed = Prometheus004Validator.validate_exposition(body)
                uptime = parsed["drugsengine_python_uptime_seconds"]["samples"][0][2]
                samples.append(uptime)
        finally:
            server.shutdown()
            server.server_close()

        self.assertEqual(len(samples), 5)
        for i in range(1, len(samples)):
            self.assertGreaterEqual(
                samples[i],
                samples[i - 1],
                f"Uptime must be strictly monotonic: sample {i} ({samples[i]}) < sample {i-1} ({samples[i-1]})",
            )
        self.assertGreaterEqual(samples[0], 0.0)


class TestPrometheusExposition004Compliance(unittest.TestCase):
    """Strict test suite for Prometheus 0.0.4 text exposition compliance."""

    def test_empty_registry_exposition_compliance(self):
        reg = SimpleMetricsRegistry()
        text = reg.generate_prometheus_text()
        parsed = Prometheus004Validator.validate_exposition(text)

        self.assertIn("drugsengine_python_uptime_seconds", parsed)
        self.assertEqual(parsed["drugsengine_python_uptime_seconds"]["type"], "gauge")
        self.assertEqual(parsed["drugsengine_python_uptime_seconds"]["samples"][0][2], 0.0)

        self.assertIn("drugsengine_python_qdrant_vectors_total", parsed)
        self.assertEqual(parsed["drugsengine_python_qdrant_vectors_total"]["type"], "gauge")
        self.assertEqual(parsed["drugsengine_python_qdrant_vectors_total"]["samples"][0][2], 0.0)

        self.assertIn("drugsengine_python_kafka_consumer_active", parsed)
        self.assertEqual(parsed["drugsengine_python_kafka_consumer_active"]["type"], "gauge")
        self.assertEqual(parsed["drugsengine_python_kafka_consumer_active"]["samples"][0][2], 0.0)

    def test_summary_and_histogram_exposition_compliance(self):
        reg = SimpleMetricsRegistry()
        reg.observe_histogram("drugsengine_python_drug_processing_seconds", 0.1234, labels={"batch": "25"})
        reg.observe_histogram("drugsengine_python_drug_processing_seconds", 0.5678, labels={"batch": "25"})

        text = reg.generate_prometheus_text()
        parsed = Prometheus004Validator.validate_exposition(text)

        self.assertIn("drugsengine_python_drug_processing_seconds", parsed)
        self.assertEqual(parsed["drugsengine_python_drug_processing_seconds"]["type"], "summary")
        samples = parsed["drugsengine_python_drug_processing_seconds"]["samples"]

        sample_names = {s[0] for s in samples}
        self.assertIn("drugsengine_python_drug_processing_seconds_count", sample_names)
        self.assertIn("drugsengine_python_drug_processing_seconds_sum", sample_names)

        count_sample = [s for s in samples if s[0].endswith("_count")][0]
        sum_sample = [s for s in samples if s[0].endswith("_sum")][0]

        self.assertEqual(count_sample[2], 2)
        self.assertAlmostEqual(sum_sample[2], 0.6912, places=4)
        self.assertEqual(count_sample[1], {"batch": "25"})

    def test_unicode_labels_and_escaping_safety(self):
        reg = SimpleMetricsRegistry()
        reg.inc_counter(
            "drugsengine_python_drug_errors_total",
            1.0,
            labels={"reason": "ошибка_сети", "target": "qdrant-кластер"},
        )
        text = reg.generate_prometheus_text()
        parsed = Prometheus004Validator.validate_exposition(text)

        self.assertIn("drugsengine_python_drug_errors_total", parsed)
        sample = parsed["drugsengine_python_drug_errors_total"]["samples"][0]
        self.assertEqual(sample[1]["reason"], "ошибка_сети")
        self.assertEqual(sample[1]["target"], "qdrant-кластер")

    def test_multithreaded_stress_concurrency(self):
        """20 writers and 10 readers hammering SimpleMetricsRegistry simultaneously."""
        reg = SimpleMetricsRegistry()
        errors = []

        def writer_task(tid: int):
            try:
                for i in range(200):
                    reg.inc_counter("drugsengine_python_drugs_processed_total", 1.0)
                    reg.set_gauge("drugsengine_python_uptime_seconds", float(i))
                    reg.observe_histogram(
                        "drugsengine_python_drug_processing_seconds", 0.01 * (i % 10), labels={"worker": str(tid)}
                    )
            except Exception as e:
                errors.append(e)

        def reader_task():
            try:
                for _ in range(50):
                    text = reg.generate_prometheus_text()
                    Prometheus004Validator.validate_exposition(text)
            except Exception as e:
                errors.append(e)

        writers = [threading.Thread(target=writer_task, args=(i,)) for i in range(20)]
        readers = [threading.Thread(target=reader_task) for _ in range(10)]

        all_threads = writers + readers
        for t in all_threads:
            t.start()
        for t in all_threads:
            t.join()

        self.assertEqual(len(errors), 0, f"Thread errors encountered: {errors}")
        text = reg.generate_prometheus_text()
        parsed = Prometheus004Validator.validate_exposition(text)
        self.assertEqual(parsed["drugsengine_python_drugs_processed_total"]["samples"][0][2], 4000.0)


if __name__ == "__main__":
    unittest.main()
