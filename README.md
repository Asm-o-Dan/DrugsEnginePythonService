# 🧠 DrugsEnginePythonService (Vector Search & Embedding Microservice)

[![Python 3.10](https://img.shields.io/badge/Python-3.10-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![Qdrant](https://img.shields.io/badge/Vector%20DB-Qdrant-DC2626?style=flat&logo=qdrant&logoColor=white)](https://qdrant.tech/)
[![Sentence Transformers](https://img.shields.io/badge/Model-LaBSE%20(HuggingFace)-FFA800)](https://huggingface.co/sentence-transformers/LaBSE)
[![Kafka](https://img.shields.io/badge/Consumer-Kafka-231F20?style=flat&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![RabbitMQ](https://img.shields.io/badge/Consumer-RabbitMQ-FF6600?style=flat&logo=rabbitmq&logoColor=white)](https://www.rabbitmq.com/)
[![Docker](https://img.shields.io/badge/Container-Docker-2496ED?style=flat&logo=docker&logoColor=white)](https://www.docker.com/)

Интеллектуальный микросервис векторизации и семантического поиска для экосистемы **DrugsEngine**. Обеспечивает нечеткий и смысловой поиск лекарственных препаратов с использованием векторной базы данных **Qdrant** и многоязычной модели **LaBSE** (`Language-Agnostic BERT Sentence Embedding`).

---

## 🔬 Архитектура и функционал

```
pythonProject2/
├── app/
│   ├── Classes/           # DTOs и Pydantic/dataclass структуры
│   ├── mq/                # Консьюмеры очередей (Kafka Consumer, RabbitMQ Consumer)
│   ├── services/          # Сервисы векторизации, работы с Qdrant и LLM
│   │   ├── vectorization_service.py # Генерация эмбеддингов через LaBSE
│   │   ├── qdrant_service.py        # Индексация и поиск в векторной БД
│   │   └── gpt_service.py           # Интеграция с языковыми моделями
│   ├── config.py          # Конфигурация окружения
│   └── main.py            # Точка входа приложения
├── Dockerfile             # Multi-stage сборка с предзагрузкой весов модели
├── docker-compose.yaml    # Локальное развертывание сервиса и Qdrant
├── requirements.txt       # Зависимости
└── entrypoint.sh          # Скрипт инициализации контейнера
```

---

## ⚡ Ключевые возможности

1. **Мультиязычные эмбеддинги (LaBSE)**:
   - Векторизация названий лекарств, активных веществ и описаний на русском и латинице для точного семантического мэтчинга.
   - Предзагрузка весов модели (`/app/model`) на этапе сборки Docker для моментального старта контейнера.
2. **Семантический векторный поиск (Qdrant)**:
   - Индексация коллекций векторов и поиск похожих препаратов по косинусному расстоянию (Cosine Distance).
3. **Асинхронная обработка очередей (Kafka / RabbitMQ)**:
   - Потребление событий о новых спарсенных лекарствах от основного .NET бэкенда и фоновая генерация векторов.
4. **Resilient Docker Setup**:
   - Multi-stage Dockerfile с кэшированием pip-пакетов и проверкой работоспособности Qdrant (`healthcheck /readyz`).

---

## 🛠️ Стек технологий

- **Язык**: Python 3.10
- **AI / ML**: `sentence-transformers`, `torch`, `huggingface-hub` (LaBSE)
- **Векторное хранилище**: `qdrant-client`
- **Очереди сообщений**: `confluent-kafka`, `pika` (RabbitMQ)
- **Контейнеризация**: Docker, Docker Compose

---

## 🚦 Быстрый старт

### 1. Запуск через Docker Compose (Рекомендуется)
```bash
docker-compose up --build -d
```
Стек запустит:
- Микросервис векторизации на порту `8000`
- Векторную базу данных Qdrant на портах `6333` (REST) и `6334` (gRPC)

Дашборд Qdrant будет доступен по адресу: `http://localhost:6333/dashboard`.

### 2. Локальный запуск для разработки
```bash
python -m venv .venv
# Активация окружения (Windows):
.venv\Scripts\activate
# Активация окружения (Linux/macOS):
source .venv/bin/activate

pip install -r requirements.txt
python main.py
```

---

## 📄 Лицензия
Распространяется под лицензией MIT. Автор: **Даниил Гандапас** ([@Asm-o-Dan](https://github.com/Asm-o-Dan)).
