import unittest
import time
import threading
from app.main import supervise_consumers


class TestSupervisor(unittest.TestCase):

    def test_supervise_consumers_fails_fast_on_consumer_error(self):
        """Проверяем, что падение второго консьюмера немедленно прерывает супервизор, не зависая на первом"""
        stop_event = threading.Event()

        def long_running_consumer():
            stop_event.wait(timeout=2.0)

        def failing_consumer():
            time.sleep(0.01)
            stop_event.set()
            raise RuntimeError("RabbitMQ connection dropped")

        consumers = [
            (long_running_consumer, ()),
            (failing_consumer, ())
        ]

        start_time = time.time()
        with self.assertRaises(RuntimeError) as ctx:
            supervise_consumers(consumers)

        elapsed = time.time() - start_time
        self.assertIn("RabbitMQ connection dropped", str(ctx.exception))
        self.assertLess(elapsed, 1.0)

    def test_supervise_consumers_clean_exit(self):
        """Проверяем нормальное завершение"""
        def fast_consumer():
            return "done"

        consumers = [
            (fast_consumer, ())
        ]

        supervise_consumers(consumers)


if __name__ == "__main__":
    unittest.main()
