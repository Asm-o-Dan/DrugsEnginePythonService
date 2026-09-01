import http.client
import threading
import time
import unittest
from unittest.mock import MagicMock, patch

from app.services.qdrant_service import QdrantService
from app.telemetry import SimpleMetricsRegistry, metrics
from app.health_server import (
    HealthAndMetricsHandler,
    ThreadedHTTPServer,
    start_health_server,
    _stop_event,
    _periodic_metrics_poller,
)
from app.mq.kafka_consumer import KafkaDrugConsumer


class TestStartupMetricsRegistry(unittest.TestCase):
    """Тесты инициализации и формата Prometheus Exposition реестра метрик"""

    def test_startup_gauges_exist_immediately_on_init(self):
        reg = SimpleMetricsRegistry()
        text = reg.generate_prometheus_text()

        self.assertIn("# TYPE drugsengine_python_uptime_seconds gauge", text)
        self.assertIn("drugsengine_python_uptime_seconds 0.0", text)

        self.assertIn("# TYPE drugsengine_python_qdrant_vectors_total gauge", text)
        self.assertIn("drugsengine_python_qdrant_vectors_total 0.0", text)

        self.assertIn("# TYPE drugsengine_python_kafka_consumer_active gauge", text)
        self.assertIn("drugsengine_python_kafka_consumer_active 0.0", text)

    def test_global_metrics_has_startup_gauges(self):
        text = metrics.generate_prometheus_text()
        self.assertIn("drugsengine_python_uptime_seconds", text)
        self.assertIn("drugsengine_python_qdrant_vectors_total", text)
        self.assertIn("drugsengine_python_kafka_consumer_active", text)

    def test_gauge_update_and_counter_interaction(self):
        reg = SimpleMetricsRegistry()
        reg.set_gauge("drugsengine_python_uptime_seconds", 42.5)
        reg.set_gauge("drugsengine_python_qdrant_vectors_total", 1500.0)
        reg.set_gauge("drugsengine_python_kafka_consumer_active", 1.0)
        reg.inc_counter("drugsengine_python_drugs_processed_total", 10.0)

        text = reg.generate_prometheus_text()
        self.assertIn("drugsengine_python_uptime_seconds 42.5", text)
        self.assertIn("drugsengine_python_qdrant_vectors_total 1500.0", text)
        self.assertIn("drugsengine_python_kafka_consumer_active 1.0", text)
        self.assertIn("# TYPE drugsengine_python_drugs_processed_total counter", text)
        self.assertIn("drugsengine_python_drugs_processed_total 10.0", text)

    def test_concurrent_registry_access(self):
        reg = SimpleMetricsRegistry()
        errors = []

        def worker(w_id):
            try:
                for i in range(100):
                    reg.set_gauge("drugsengine_python_uptime_seconds", float(i))
                    reg.inc_counter("drugsengine_python_drugs_processed_total", 1.0)
                    reg.observe_histogram("drugsengine_python_drug_processing_seconds", 0.05)
                    _ = reg.generate_prometheus_text()
            except Exception as ex:
                errors.append(ex)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0)
        self.assertIn("drugsengine_python_drugs_processed_total 500.0", reg.generate_prometheus_text())


class TestQdrantServiceVectorsCount(unittest.TestCase):
    """Тесты метода get_vectors_count в QdrantService"""

    def setUp(self):
        with patch("app.services.qdrant_service.QdrantClient"):
            self.service = QdrantService(host="localhost", port=6333)
            self.mock_client = MagicMock()
            self.service.client = self.mock_client

    def test_get_vectors_count_with_points_count(self):
        self.mock_client.collection_exists.return_value = True
        mock_info = MagicMock()
        mock_info.points_count = 1250
        mock_info.vectors_count = None
        self.mock_client.get_collection.return_value = mock_info

        count = self.service.get_vectors_count("drug_collection")
        self.assertEqual(count, 1250)
        self.mock_client.get_collection.assert_called_once_with("drug_collection")

    def test_get_vectors_count_fallback_to_vectors_count(self):
        self.mock_client.collection_exists.return_value = True
        mock_info = MagicMock()
        mock_info.points_count = None
        mock_info.vectors_count = 850
        self.mock_client.get_collection.return_value = mock_info

        count = self.service.get_vectors_count("drug_collection")
        self.assertEqual(count, 850)

    def test_get_vectors_count_with_no_count_attributes(self):
        self.mock_client.collection_exists.return_value = True
        mock_info = MagicMock(spec=[])
        self.mock_client.get_collection.return_value = mock_info

        count = self.service.get_vectors_count("drug_collection")
        self.assertEqual(count, 0)

    def test_get_vectors_count_collection_does_not_exist(self):
        self.mock_client.collection_exists.return_value = False

        count = self.service.get_vectors_count("nonexistent_collection")
        self.assertEqual(count, 0)
        self.mock_client.get_collection.assert_not_called()

    def test_get_vectors_count_collection_exists_exception(self):
        self.mock_client.collection_exists.side_effect = RuntimeError("Connection dropped")

        count = self.service.get_vectors_count("drug_collection")
        self.assertEqual(count, 0)

    def test_get_vectors_count_get_collection_exception(self):
        self.mock_client.collection_exists.return_value = True
        self.mock_client.get_collection.side_effect = RuntimeError("Qdrant unavailable")

        count = self.service.get_vectors_count("drug_collection")
        self.assertEqual(count, 0)


class TestKafkaConsumerActiveGauge(unittest.TestCase):
    """Тесты изменения метрики drugsengine_python_kafka_consumer_active при жизненном цикле consumer"""

    @patch("app.mq.kafka_consumer.Consumer")
    def test_kafka_consumer_active_gauge_lifecycle(self, mock_consumer_cls):
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer

        vector_svc = MagicMock()
        qdrant_svc = MagicMock()
        api_svc = MagicMock()

        consumer = KafkaDrugConsumer(
            vector_service=vector_svc,
            qdrant_service=qdrant_svc,
            api_service=api_svc,
            topic="test-topic",
            broker="localhost:9092",
        )

        # Вызываем исключение на первом poll, чтобы прервать бесконечный цикл
        mock_consumer.poll.side_effect = KeyboardInterrupt("Stop loop")

        metrics.set_gauge("drugsengine_python_kafka_consumer_active", 0.0)
        consumer.run_consumption_loop()

        # После выхода из цикла значение должно быть 0.0
        text = metrics.generate_prometheus_text()
        self.assertIn("drugsengine_python_kafka_consumer_active 0.0", text)
        mock_consumer.close.assert_called_once()

    @patch("app.mq.kafka_consumer.Consumer")
    def test_kafka_consumer_active_gauge_on_exception(self, mock_consumer_cls):
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer

        vector_svc = MagicMock()
        qdrant_svc = MagicMock()
        api_svc = MagicMock()

        consumer = KafkaDrugConsumer(
            vector_service=vector_svc,
            qdrant_service=qdrant_svc,
            api_service=api_svc,
            topic="test-topic",
            broker="localhost:9092",
        )

        mock_consumer.poll.side_effect = RuntimeError("Fatal poll failure")
        consumer.run_consumption_loop()

        text = metrics.generate_prometheus_text()
        self.assertIn("drugsengine_python_kafka_consumer_active 0.0", text)
        mock_consumer.close.assert_called_once()


class TestHealthServerMetricsEndpoint(unittest.TestCase):
    """Интеграционные тесты HTTP эндпоинта /metrics и работы фонового поллера"""

    def setUp(self):
        _stop_event.set()

    def test_metrics_http_exposition_live(self):
        mock_qdrant = MagicMock()
        mock_qdrant.get_vectors_count.return_value = 345
        mock_qdrant.health_check.return_value = True

        server = ThreadedHTTPServer(("127.0.0.1", 8011), HealthAndMetricsHandler)
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()

        time.sleep(0.05)
        conn = http.client.HTTPConnection("127.0.0.1", 8011, timeout=5)
        try:
            conn.request("GET", "/metrics")
            response = conn.getresponse()
            body = response.read().decode("utf-8")

            self.assertEqual(response.status, 200)
            self.assertEqual(response.getheader("Content-Type"), "text/plain; version=0.0.4")
            self.assertIn("# TYPE drugsengine_python_uptime_seconds gauge", body)
            self.assertIn("# TYPE drugsengine_python_qdrant_vectors_total gauge", body)
            self.assertIn("# TYPE drugsengine_python_kafka_consumer_active gauge", body)
        finally:
            conn.close()
            server.shutdown()
            server.server_close()

    def test_health_endpoint_healthy(self):
        mock_qdrant = MagicMock()
        mock_qdrant.health_check.return_value = True

        with patch("app.health_server._qdrant_service_ref", mock_qdrant):
            server = ThreadedHTTPServer(("127.0.0.1", 8012), HealthAndMetricsHandler)
            server_thread = threading.Thread(target=server.serve_forever, daemon=True)
            server_thread.start()

            time.sleep(0.05)
            conn = http.client.HTTPConnection("127.0.0.1", 8012, timeout=5)
            try:
                conn.request("GET", "/health")
                response = conn.getresponse()
                self.assertEqual(response.status, 200)
            finally:
                conn.close()
                server.shutdown()
                server.server_close()

    def test_health_endpoint_unhealthy_when_qdrant_fails(self):
        mock_qdrant = MagicMock()
        mock_qdrant.health_check.return_value = False

        with patch("app.health_server._qdrant_service_ref", mock_qdrant):
            server = ThreadedHTTPServer(("127.0.0.1", 8013), HealthAndMetricsHandler)
            server_thread = threading.Thread(target=server.serve_forever, daemon=True)
            server_thread.start()

            time.sleep(0.05)
            conn = http.client.HTTPConnection("127.0.0.1", 8013, timeout=5)
            try:
                conn.request("GET", "/health")
                response = conn.getresponse()
                self.assertEqual(response.status, 503)
            finally:
                conn.close()
                server.shutdown()
                server.server_close()

    def test_periodic_metrics_poller_updates_gauges(self):
        mock_qdrant = MagicMock()
        mock_qdrant.get_vectors_count.return_value = 999

        with patch("app.health_server._qdrant_service_ref", mock_qdrant):
            _stop_event.clear()
            poller_thread = threading.Thread(
                target=_periodic_metrics_poller,
                args=(0.05,),
                daemon=True
            )
            poller_thread.start()
            time.sleep(0.15)
            _stop_event.set()
            poller_thread.join(timeout=1.0)

            text = metrics.generate_prometheus_text()
            self.assertIn("drugsengine_python_qdrant_vectors_total 999.0", text)

    def test_start_health_server_with_none_qdrant(self):
        with patch("app.health_server.ThreadedHTTPServer") as mock_server_cls:
            mock_server_instance = MagicMock()
            mock_server_cls.return_value = mock_server_instance

            thread = start_health_server(qdrant_service=None, port=8014)
            _stop_event.set()

            text = metrics.generate_prometheus_text()
            self.assertIn("drugsengine_python_qdrant_vectors_total 0.0", text)

    def test_start_health_server_with_qdrant_exception_on_init(self):
        mock_qdrant = MagicMock()
        mock_qdrant.get_vectors_count.side_effect = RuntimeError("Failed initial poll")

        with patch("app.health_server.ThreadedHTTPServer") as mock_server_cls:
            mock_server_instance = MagicMock()
            mock_server_cls.return_value = mock_server_instance

            thread = start_health_server(qdrant_service=mock_qdrant, port=8015)
            _stop_event.set()

            text = metrics.generate_prometheus_text()
            self.assertIn("drugsengine_python_qdrant_vectors_total 0.0", text)


if __name__ == "__main__":
    unittest.main()
