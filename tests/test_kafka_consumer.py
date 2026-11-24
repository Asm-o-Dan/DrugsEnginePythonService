from app.Classes.classes import Drug
from app.mq.kafka_consumer import KafkaDrugConsumer


class FakeLlm:
    def __init__(self, configured, answer=None):
        self.configured = configured
        self.answer = answer
        self.calls = 0

    def get_drug_info(self, name):
        self.calls += 1
        return self.answer


class FakeVectorizer:
    def __init__(self):
        self.texts = []

    def vectorize_passage(self, text):
        self.texts.append(text)
        return [0.1, 0.2]


class FakeStore:
    def __init__(self):
        self.saved = []

    def add_vector(self, drug, vector):
        self.saved.append(drug)
        return True


def make(llm):
    consumer = object.__new__(KafkaDrugConsumer)  # без подключения к Kafka
    consumer.vector_service, consumer.qdrant_service, consumer.api_service = FakeVectorizer(), FakeStore(), llm
    return consumer


def drug():
    return Drug.from_json({"Id": "d0a1b2c3-0000-4000-8000-000000000001", "Name": "Омез", "ActiveIngredient": "Омепразол"})


def test_indexes_without_llm_keys():
    consumer = make(FakeLlm(configured=False))
    assert consumer._process_single_drug(drug()) is True
    assert consumer.api_service.calls == 0
    assert consumer.qdrant_service.saved[0].description is None
    assert "Омепразол" in consumer.vector_service.texts[0]


def test_uses_llm_description_when_available():
    consumer = make(FakeLlm(configured=True, answer="Снижает кислотность"))
    assert consumer._process_single_drug(drug()) is True
    saved = consumer.qdrant_service.saved[0]
    assert (saved.description, saved.description_source) == ("Снижает кислотность", "generated")
    assert "Снижает кислотность" in consumer.vector_service.texts[0]


def test_llm_failure_does_not_block_indexing():
    consumer = make(FakeLlm(configured=True, answer=None))
    assert consumer._process_single_drug(drug()) is True
    assert consumer.qdrant_service.saved[0].description is None
