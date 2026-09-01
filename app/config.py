import os

# RabbitMQ
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "rpc_queue")
RABBITMQ_PREFETCH_COUNT = os.getenv("RABBITMQ_PREFETCH_COUNT", 1)

# Kafka
def _get_env_str(*keys: str, default: str) -> str:
    for key in keys:
        val = os.getenv(key)
        if val is not None and isinstance(val, str) and val.strip():
            return val.strip()
    return default

KAFKA_BROKER = _get_env_str(
    "KAFKA_BROKER",
    "KAFKA_BOOTSTRAP_SERVERS",
    "Kafka__BootstrapServers",
    "Kafka__Broker",
    "Kafka:BootstrapServers",
    "Kafka:Broker",
    default="kafka:9092",
)

KAFKA_TOPIC = _get_env_str(
    "KAFKA_TOPIC",
    "Kafka__Topic",
    "Kafka:Topic",
    default="drugs",
)

# Qdrant
QDRANT_HOST = os.getenv("QDRANT_HOST", "qdrant")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", 6333))

# Gemini API Configuration
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.5-flash-lite")

# Model Cache Configuration (Docker Volume)
MODEL_PATH = os.getenv("MODEL_PATH", "/app/model")
MODEL_NAME = os.getenv("MODEL_NAME", "sentence-transformers/LaBSE")
