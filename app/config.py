import os

# RabbitMQ
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "rpc_queue")
RABBITMQ_PREFETCH_COUNT = os.getenv("RABBITMQ_PREFETCH_COUNT", 1)

# Kafka
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "drugs")

# Qdrant
QDRANT_HOST = os.getenv("QDRANT_HOST", "qdrant")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", 6333))

# Gemini API Configuration
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.5-flash-lite")

# Model Cache Configuration (Docker Volume)
MODEL_PATH = os.getenv("MODEL_PATH", "/app/model")
MODEL_NAME = os.getenv("MODEL_NAME", "sentence-transformers/LaBSE")
