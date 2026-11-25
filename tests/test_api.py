import uuid

from fastapi.testclient import TestClient

from app.api import create_app

A, B, C = (str(uuid.UUID(int=i)) for i in (1, 2, 3))


class FakeVectorizer:
    def __init__(self):
        self.calls = []

    def vectorize_query(self, text):
        self.calls.append(text)
        return [0.1, 0.2]


class FakeStore:
    def __init__(self, available=True):
        self.available = available
        self.points = {A: {"description": "Обезболивающее", "description_source": "generated"}, B: {}}

    def health_check(self):
        return self.available

    def search_vector(self, vector, collection_name="drug_collection", limit=5, **filters):
        if not self.available:
            return None
        return [{"id": A, "score": 0.9, "payload": {}}, {"id": B, "score": 0.7, "payload": {}}][:limit]

    def similar(self, point_id, limit=6, collection_name="drug_collection"):
        if point_id not in self.points:
            return None
        return [{"id": C, "score": 0.8, "payload": {}}]

    def get_point(self, point_id, with_vector=False, collection_name="drug_collection"):
        if point_id not in self.points:
            return None
        return {"id": point_id, "payload": self.points[point_id], "vector": None}


def client(store=None, vectorizer=None):
    return TestClient(create_app(vectorizer or FakeVectorizer(), store or FakeStore()))


def test_search_returns_ids_and_scores():
    vectorizer = FakeVectorizer()
    response = client(vectorizer=vectorizer).post("/v1/search", json={"query": "  изжога ", "limit": 1})
    assert response.status_code == 200
    assert response.json() == {"items": [{"id": A, "score": 0.9}]}
    assert vectorizer.calls == ["изжога"]


def test_search_validates_input():
    assert client().post("/v1/search", json={"query": "", "limit": 5}).status_code == 422
    assert client().post("/v1/search", json={"query": "x", "limit": 1000}).status_code == 422


def test_search_returns_503_when_store_unavailable():
    response = client(FakeStore(available=False)).post("/v1/search", json={"query": "боль"})
    assert response.status_code == 503


def test_similar_and_404_for_unknown_or_malformed_id():
    c = client()
    assert c.get(f"/v1/drugs/{A}/similar?limit=3").json() == {"items": [{"id": C, "score": 0.8}]}
    assert c.get(f"/v1/drugs/{C}/similar").status_code == 404
    assert c.get("/v1/drugs/not-a-guid/similar").status_code == 404


def test_document_marks_generated_descriptions():
    c = client()
    assert c.get(f"/v1/drugs/{A.upper()}").json() == {
        "id": A, "description": "Обезболивающее", "descriptionSource": "generated"}
    assert c.get(f"/v1/drugs/{B}").json() == {"id": B, "description": None, "descriptionSource": None}
    assert c.get(f"/v1/drugs/{C}").status_code == 404


def test_health_reports_degraded_qdrant():
    assert client().get("/health").json() == {"status": "ok", "qdrant": True, "consumer": None}
    assert client(FakeStore(available=False)).get("/health").json()["status"] == "degraded"


def test_health_reports_dead_consumer():
    app = create_app(FakeVectorizer(), FakeStore(), consumer_alive=lambda: False)
    body = TestClient(app).get("/health").json()
    assert body == {"status": "degraded", "qdrant": True, "consumer": False}
