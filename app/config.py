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
