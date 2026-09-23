import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from taskq import TaskQueue  # noqa: E402


class TaskQueueTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "q.db")
        self.q = TaskQueue(self.path)

    def tearDown(self):
        self.q.close()
        self.tmp.cleanup()

    def test_enqueue_returns_increasing_ids(self):
        a = self.q.enqueue("mail", {"n": 1})
        b = self.q.enqueue("mail", {"n": 2})
        self.assertEqual((a, b), (1, 2))

    def test_dequeue_is_fifo(self):
        self.q.enqueue("mail", {"n": 1})
        self.q.enqueue("mail", {"n": 2})
        self.assertEqual(self.q.dequeue("mail")["payload"], {"n": 1})
        self.assertEqual(self.q.dequeue("mail")["payload"], {"n": 2})

    def test_dequeue_empty_returns_none(self):
        self.assertIsNone(self.q.dequeue("mail"))

    def test_dequeue_skips_other_queues(self):
        self.q.enqueue("sms", {"n": 1})
        self.assertIsNone(self.q.dequeue("mail"))

    def test_dequeue_marks_running_and_counts_attempt(self):
        tid = self.q.enqueue("mail", {"n": 1})
        task = self.q.dequeue("mail")
        self.assertEqual(task["id"], tid)
        self.assertEqual(task["attempts"], 1)
        self.assertEqual(self.q.get(tid)["state"], "running")

    def test_complete_and_fail(self):
        a = self.q.enqueue("mail", {})
        b = self.q.enqueue("mail", {})
        self.assertTrue(self.q.complete(a))
        self.assertTrue(self.q.fail(b, "boom"))
        self.assertEqual(self.q.get(a)["state"], "done")
        self.assertEqual(self.q.get(b)["last_error"], "boom")
        self.assertFalse(self.q.complete(999))

    def test_stats_counts_by_state(self):
        a = self.q.enqueue("mail", {})
        self.q.enqueue("mail", {})
        self.q.enqueue("sms", {})
        self.q.dequeue("mail")
        self.q.complete(a)
        self.assertEqual(self.q.stats("mail"), {"pending": 1, "running": 0, "done": 1, "failed": 0, "total": 2})
        self.assertEqual(self.q.stats()["total"], 3)

    def test_persists_across_reopen(self):
        self.q.enqueue("mail", {"n": 7})
        self.q.close()
        again = TaskQueue(self.path)
        try:
            self.assertEqual(again.dequeue("mail")["payload"], {"n": 7})
        finally:
            again.close()

    def test_unicode_payload_roundtrip(self):
        payload = {"收件人": "张三", "主题": "测试"}
        self.q.enqueue("mail", payload)
        self.assertEqual(self.q.dequeue("mail")["payload"], payload)


if __name__ == "__main__":
    unittest.main()
