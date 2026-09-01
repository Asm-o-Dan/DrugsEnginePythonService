import threading
import logging
from typing import Dict, Optional, Any, List

logger = logging.getLogger("Telemetry")

class SimpleMetricsRegistry:
    """Потокобезопасный реестр метрик в строгом соответствии со спецификацией Prometheus Exposition"""
    def __init__(self):
        self._lock = threading.Lock()
        self._counters: Dict[str, float] = {}
        self._gauges: Dict[str, float] = {}
        self._summary_sums: Dict[str, float] = {}
        self._summary_counts: Dict[str, int] = {}
        self.init_startup_metrics()

    def init_startup_metrics(self):
        """Инициализация обязательных стартовых gauge-метрик для предотвращения пустых ответов /metrics"""
        self.set_gauge("drugsengine_python_uptime_seconds", 0.0)
        self.set_gauge("drugsengine_python_qdrant_vectors_total", 0.0)
        self.set_gauge("drugsengine_python_kafka_consumer_active", 0.0)

    def inc_counter(self, name: str, value: float = 1.0, labels: Optional[Dict[str, str]] = None):
        key = self._format_key(name, labels)
        with self._lock:
            self._counters[key] = self._counters.get(key, 0.0) + value

    def set_gauge(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        key = self._format_key(name, labels)
        with self._lock:
            self._gauges[key] = value

    def observe_histogram(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        key = self._format_key(name, labels)
        with self._lock:
            self._summary_sums[key] = self._summary_sums.get(key, 0.0) + value
            self._summary_counts[key] = self._summary_counts.get(key, 0) + 1

    def _format_key(self, name: str, labels: Optional[Dict[str, str]]) -> str:
        if not labels:
            return name
        label_str = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    def generate_prometheus_text(self) -> str:
        lines: List[str] = []
        with self._lock:
            # 1. Counters
            counters_by_family: Dict[str, List[tuple]] = {}
            for key, val in self._counters.items():
                family = key.split("{")[0]
                counters_by_family.setdefault(family, []).append((key, val))

            for family, items in sorted(counters_by_family.items()):
                lines.append(f"# TYPE {family} counter")
                for key, val in sorted(items):
                    lines.append(f"{key} {val}")

            # 2. Gauges
            gauges_by_family: Dict[str, List[tuple]] = {}
            for key, val in self._gauges.items():
                family = key.split("{")[0]
                gauges_by_family.setdefault(family, []).append((key, val))

            for family, items in sorted(gauges_by_family.items()):
                lines.append(f"# TYPE {family} gauge")
                for key, val in sorted(items):
                    lines.append(f"{key} {val}")

            # 3. Summaries / Histograms (cumulative sum and count)
            summaries_by_family: Dict[str, List[str]] = {}
            for key in set(list(self._summary_sums.keys()) + list(self._summary_counts.keys())):
                family = key.split("{")[0]
                summaries_by_family.setdefault(family, []).append(key)

            for family, keys in sorted(summaries_by_family.items()):
                lines.append(f"# TYPE {family} summary")
                for key in sorted(keys):
                    count = self._summary_counts.get(key, 0)
                    total_sum = self._summary_sums.get(key, 0.0)
                    if "{" in key:
                        labels = key[key.find("{"):]
                        lines.append(f"{family}_count{labels} {count}")
                        lines.append(f"{family}_sum{labels} {total_sum:.4f}")
                    else:
                        lines.append(f"{family}_count {count}")
                        lines.append(f"{family}_sum {total_sum:.4f}")

        return "\n".join(lines) + "\n"

metrics = SimpleMetricsRegistry()

def extract_traceparent(headers) -> Optional[Dict[str, str]]:
    """Извлекает W3C traceparent из Kafka заголовков (байты или строки)"""
    if not headers:
        return None

    header_dict = {}
    if isinstance(headers, list):
        for k, v in headers:
            key_str = k.decode("utf-8", errors="ignore") if isinstance(k, bytes) else str(k)
            val_str = v.decode("utf-8", errors="ignore") if isinstance(v, bytes) else str(v)
            header_dict[key_str] = val_str
    elif isinstance(headers, dict):
        for k, v in headers.items():
            key_str = k.decode("utf-8", errors="ignore") if isinstance(k, bytes) else str(k)
            val_str = v.decode("utf-8", errors="ignore") if isinstance(v, bytes) else str(v)
            header_dict[key_str] = val_str

    traceparent = header_dict.get("traceparent")
    if not traceparent:
        return None

    parts = traceparent.split("-")
    if len(parts) >= 4:
        return {
            "version": parts[0],
            "trace_id": parts[1],
            "parent_id": parts[2],
            "flags": parts[3],
            "raw": traceparent
        }
    return {"raw": traceparent}
