"""
HTTP API для .NET-бэкенда DrugsEngine (клиент — Infrastructure/SemanticSearch/SemanticSearchClient.cs).

  GET  /health                          → {"status": "ok" | "degraded", "qdrant": bool}
  POST /v1/search {query, limit}        → {"items": [{"id", "score"}]}
  GET  /v1/drugs/{id}/similar?limit=N   → {"items": [{"id", "score"}]} | 404
  GET  /v1/drugs/{id}                   → {"id", "description", "descriptionSource"} | 404
"""
import logging
import uuid
from typing import Protocol

from fastapi import FastAPI, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

logger = logging.getLogger("Api")

COLLECTION = "drug_collection"


class Vectorizer(Protocol):
    def vectorize_query(self, text: str) -> list[float]: ...


class VectorStore(Protocol):
    def search_vector(self, vector: list[float], collection_name: str = ..., limit: int = ..., **filters): ...
    def similar(self, point_id: str, limit: int = ..., collection_name: str = ...): ...
    def get_point(self, point_id: str, with_vector: bool = ..., collection_name: str = ...): ...
    def health_check(self) -> bool: ...


class SearchRequest(BaseModel):
    query: str = Field(min_length=1, max_length=500)
    limit: int = Field(default=20, ge=1, le=200)


class Hit(BaseModel):
    id: str
    score: float


class Hits(BaseModel):
    items: list[Hit]


class Document(BaseModel):
    id: str
    description: str | None
    descriptionSource: str | None


def _parse_id(drug_id: str) -> str:
    try:
        return str(uuid.UUID(drug_id))
    except ValueError:
        raise HTTPException(status_code=404, detail="Некорректный id препарата")


def create_app(vectorizer: Vectorizer, store: VectorStore) -> FastAPI:
    app = FastAPI(title="DrugsEngine Vector Search", version="1.0.0")

    @app.get("/health")
    async def health():
        qdrant_ok = await run_in_threadpool(store.health_check)
        return {"status": "ok" if qdrant_ok else "degraded", "qdrant": qdrant_ok}

    @app.post("/v1/search", response_model=Hits)
    async def search(request: SearchRequest):
        vector = await run_in_threadpool(vectorizer.vectorize_query, request.query.strip())
        results = await run_in_threadpool(store.search_vector, vector, COLLECTION, request.limit)
        if results is None:
            raise HTTPException(status_code=503, detail="Векторное хранилище недоступно")
        return Hits(items=[Hit(id=r["id"], score=r["score"]) for r in results])

    @app.get("/v1/drugs/{drug_id}/similar", response_model=Hits)
    async def similar(drug_id: str, limit: int = Query(default=6, ge=1, le=50)):
        results = await run_in_threadpool(store.similar, _parse_id(drug_id), limit, COLLECTION)
        if results is None:
            raise HTTPException(status_code=404, detail="Препарат не проиндексирован")
        return Hits(items=[Hit(id=r["id"], score=r["score"]) for r in results])

    @app.get("/v1/drugs/{drug_id}", response_model=Document)
    async def document(drug_id: str):
        point = await run_in_threadpool(store.get_point, _parse_id(drug_id), False, COLLECTION)
        if point is None:
            raise HTTPException(status_code=404, detail="Препарат не проиндексирован")
        payload = point["payload"]
        description = (payload.get("description") or "").strip() or None
        return Document(
            id=point["id"],
            description=description,
            descriptionSource=(payload.get("description_source") or "generated") if description else None,
        )

    return app
