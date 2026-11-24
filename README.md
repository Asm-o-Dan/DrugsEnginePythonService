# DrugsEnginePythonService

Векторный поиск DrugsEngine: эмбеддинги multilingual-e5-small, хранилище Qdrant, HTTP API для .NET-бэкенда
и потребитель Kafka, который индексирует новые препараты.

## Как это работает

```
.NET бэкенд ──Kafka "drugs"──▶ kafka consumer ──LLM-описание──▶ multilingual-e5-small ──▶ Qdrant
.NET бэкенд ──HTTP──────────▶ /v1/search, /v1/drugs/{id}/similar, /v1/drugs/{id}
```

Id точки в Qdrant совпадает с Id препарата в PostgreSQL — по нему бэкенд связывает результаты с каталогом.

## HTTP API

| Метод | Путь | Ответ |
|---|---|---|
| GET | `/health` | `{status: ok \| degraded, qdrant}` |
| POST | `/v1/search` `{query, limit}` | `{items: [{id, score}]}` |
| GET | `/v1/drugs/{id}/similar?limit=N` | `{items: [{id, score}]}` или 404, если препарат не проиндексирован |
| GET | `/v1/drugs/{id}` | `{id, description, descriptionSource}` или 404 |

## Запуск

```bash
cp .env.example .env        # ключи LLM (необязательно — без них описания не генерируются)
docker compose up -d --build
```

Модель по умолчанию — `intfloat/multilingual-e5-small` (384 измерения, префиксы `query:`/`passage:`):
на реальном каталоге аптеки она заметно точнее LaBSE («от изжоги» → средства от изжоги, а не пакеты для мусора).
При смене модели коллекция Qdrant пересоздаётся автоматически — каталог нужно переопубликовать
(в .NET: `Kafka__PublishAllOnStartup=true`).

Без ключей LLM препараты индексируются по названию, действующему веществу и форме выпуска. Поиск по симптомам,
которых нет в названии («головная боль», «снизить давление»), работает только с описаниями — для него нужны ключи LLM
или справочник показаний.

Переменные окружения: `MODEL_NAME` / `MODEL_PATH`, `EMBEDDING_QUERY_PREFIX`, `EMBEDDING_PASSAGE_PREFIX`,
`QDRANT_HOST`, `QDRANT_PORT`, `QDRANT_PATH` (встроенный Qdrant для локальной разработки), `KAFKA_BROKER`, `KAFKA_TOPIC`, `KAFKA_ENABLED` (по умолчанию `true`),
`RABBITMQ_ENABLED` (RPC-поиск через RabbitMQ, по умолчанию `false`), `MODEL_PATH`, `PORT`, `LOG_LEVEL`,
`DEEPINFRA_API_KEY`, `TOGETHER_API_KEY`, `FIREWORKS_API_KEY`.

Ключи LLM никогда не хранятся в репозитории.

## Тесты

```bash
pip install -r requirements-dev.txt
pytest
```

Тесты не требуют модели и сети: HTTP API проверяется на фейках, `QdrantService` — на Qdrant в режиме `:memory:`.

## Лицензия
GNU General Public License v3.0 (GPL-3.0) — см. LICENSE.
