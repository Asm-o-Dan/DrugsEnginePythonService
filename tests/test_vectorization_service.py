import unittest
from unittest.mock import MagicMock, patch
from app.services.vectorization_service import VectorizationService


class TestVectorizationService(unittest.TestCase):

    def setUp(self):
        VectorizationService._instance = None
        with patch("app.services.vectorization_service.SentenceTransformer") as mock_st_cls:
            self.mock_model = MagicMock()
            self.mock_model.get_sentence_embedding_dimension.return_value = 4
            self.mock_model.encode.return_value = [0.1, 0.2, 0.3, 0.4]
            mock_st_cls.return_value = self.mock_model

            self.service = VectorizationService("test-model")

    def test_immutable_cache_prevents_in_place_corruption(self):
        """Проверяем, что мутация возвращенного вектора не портит кэш"""
        self.mock_model.encode.return_value = [1.0, 2.0, 3.0, 4.0]

        vec1 = self.service.vectorize_text("Парацетамол")
        self.assertEqual(vec1, [1.0, 2.0, 3.0, 4.0])

        # Mutate returned list in-place
        vec1[0] = 999.0
        self.assertEqual(vec1[0], 999.0)

        # Second retrieval for same text must NOT be corrupted
        vec2 = self.service.vectorize_text("Парацетамол")
        self.assertEqual(vec2, [1.0, 2.0, 3.0, 4.0])
        self.assertNotEqual(vec2[0], 999.0)

    def test_invalid_empty_input_raises_value_error(self):
        """Проверяем валидацию пустых строк"""
        with self.assertRaises(ValueError):
            self.service.vectorize_text("")

        with self.assertRaises(ValueError):
            self.service.vectorize_text(None)

    def test_vectorize_bulk_success(self):
        """Проверяем пакетную векторизацию"""
        self.mock_model.encode.return_value = MagicMock(tolist=lambda: [[1.0, 2.0], [3.0, 4.0]])

        vectors = self.service.vectorize_bulk(["Текст 1", "Текст 2"])
        self.assertEqual(len(vectors), 2)
        self.assertEqual(vectors[0], [1.0, 2.0])


if __name__ == "__main__":
    unittest.main()
