import uuid

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams

from app.services.qdrant_service import QdrantService

IDS = [str(uuid.UUID(int=i)) for i in range(1, 5)]


def make_service():
    # Обходим синглтон и сетевое подключение: настоящий Qdrant в памяти процесса
    service = object.__new__(QdrantService)
    service.client = QdrantClient(":memory:")
    service.client.create_collection("drug_collection", vectors_config=VectorParams(size=2, distance=Distance.COSINE))
    vectors = [[1.0, 0.0], [0.9, 0.1], [0.0, 1.0], [0.7, 0.7]]
    service.client.upsert("drug_collection", points=[
        PointStruct(id=i, vector=v, payload={"name": f"drug-{n}", "description": f"d{n}"})
        for n, (i, v) in enumerate(zip(IDS, vectors))
    ])
    return service


def test_similar_excludes_point_itself_and_orders_by_score():
    result = make_service().similar(IDS[0], limit=2)
    assert [r["id"] for r in result] == [IDS[1], IDS[3]]
    assert result[0]["score"] > result[1]["score"]


def test_similar_returns_none_for_unknown_point():
    assert make_service().similar(str(uuid.UUID(int=99))) is None


def test_get_point_returns_payload():
    point = make_service().get_point(IDS[2])
    assert point["payload"]["description"] == "d2"


def test_search_vector_formats_results():
    service = make_service()
    service._validate_vector = lambda vector, collection: True
    results = service.search_vector([0.0, 1.0], limit=1)
    assert results == [{"id": IDS[2], "score": results[0]["score"], "payload": {"name": "drug-2", "description": "d2"}}]
