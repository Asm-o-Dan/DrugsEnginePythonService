import unittest
from unittest.mock import MagicMock, patch
from app.services.qdrant_service import QdrantService
from app.Classes.classes import Drug


class TestQdrantService(unittest.TestCase):

    def setUp(self):
        # Reset singleton instance for tests
        QdrantService._instance = None
        with patch("app.services.qdrant_service.QdrantClient"):
            self.service = QdrantService("localhost", 6333)

    def test_ensure_collection_caches_dimension(self):
        """ensure_collection должен сохранять размерность в _collection_dimensions"""
        self.service.client.collection_exists.return_value = True
        self.service.ensure_collection("test_coll", 768)

        self.assertEqual(self.service._collection_dimensions.get("test_coll"), 768)

    def test_validate_vector_uses_cached_dimension_without_extra_network_call(self):
        """_validate_vector не должен повторно вызывать get_collection, если размерность в кеше"""
        self.service._collection_dimensions["cached_coll"] = 3

        # Should be valid
        self.assertTrue(self.service._validate_vector([0.1, 0.2, 0.3], "cached_coll"))
        # get_collection should NOT have been called
        self.service.client.get_collection.assert_not_called()

        # Should be invalid
        self.assertFalse(self.service._validate_vector([0.1, 0.2], "cached_coll"))

    def test_add_vector_successful_upsert(self):
        """add_vector корректно вызывает upsert без блокировок"""
        self.service._collection_dimensions["drug_collection"] = 2
        drug = Drug(name="Аспирин", manufacturer="Bayer", country_code_id="DE", country=None, id="12345")

        result = self.service.add_vector(drug, [0.5, 0.5])
        self.assertTrue(result)
        self.service.client.upsert.assert_called_once()

    def test_search_vector_successful_search(self):
        """search_vector возвращает отформатированные результаты"""
        self.service._collection_dimensions["drug_collection"] = 2

        mock_scored_point = MagicMock()
        mock_scored_point.id = "12345"
        mock_scored_point.score = 0.95
        mock_scored_point.payload = {"name": "Аспирин"}

        self.service.client.search.return_value = [mock_scored_point]

        results = self.service.search_vector([0.1, 0.2], limit=1)
        self.assertIsNotNone(results)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["id"], "12345")
        self.assertEqual(results[0]["name"], "Аспирин")
        self.assertEqual(results[0]["score"], 0.95)


if __name__ == "__main__":
    unittest.main()
