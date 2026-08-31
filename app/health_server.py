import http.server
import socketserver
import json
import logging
import threading
import time
from typing import Optional

try:
    from app.telemetry import metrics
    from app.services.qdrant_service import QdrantService
except ImportError:
    from telemetry import metrics
    from services.qdrant_service import QdrantService

logger = logging.getLogger("HealthServer")

_qdrant_service_ref: Optional[QdrantService] = None

class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    """Многопоточный HTTP сервер для исключения блокировок между health check и scraper метрик"""
    daemon_threads = True

class HealthAndMetricsHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # Отключаем дефолтный шумный лог http.server
        logger.debug("%s - - [%s] %s" % (self.address_string(), self.log_date_time_string(), format % args))

    def do_GET(self):
        if self.path == "/health" or self.path == "/health/ready" or self.path == "/health/live":
            self._handle_health()
        elif self.path == "/metrics":
            self._handle_metrics()
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not Found")

    def _handle_health(self):
        global _qdrant_service_ref
        qdrant_healthy = True
        if _qdrant_service_ref is not None:
            try:
                qdrant_healthy = _qdrant_service_ref.health_check()
            except Exception as e:
                logger.warning(f"Health check exception: {e}")
                qdrant_healthy = False

        status_code = 200 if qdrant_healthy else 503
        response_body = {
            "status": "Healthy" if qdrant_healthy else "Unhealthy",
            "timestamp": time.time(),
            "services": {
                "qdrant": "UP" if qdrant_healthy else "DOWN",
                "python_embedding_service": "UP"
            }
        }
        data = json.dumps(response_body).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _handle_metrics(self):
        content = metrics.generate_prometheus_text().encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)


def start_health_server(qdrant_service: Optional[QdrantService] = None, port: int = 8000):
    """Запускает многопоточный HTTP health & metrics сервер в фоновом потоке-демоне"""
    global _qdrant_service_ref
    _qdrant_service_ref = qdrant_service

    def _serve():
        try:
            server = ThreadedHTTPServer(("0.0.0.0", port), HealthAndMetricsHandler)
            logger.info(f"Threaded Health & Metrics HTTP сервер запущен на порту {port} (/health, /metrics)")
            server.serve_forever()
        except Exception as e:
            logger.error(f"Ошибка запуска health сервера на порту {port}: {e}", exc_info=True)

    thread = threading.Thread(target=_serve, daemon=True, name="HealthServerThread")
    thread.start()
    return thread
