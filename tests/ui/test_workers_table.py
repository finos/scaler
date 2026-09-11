"""The worker row the Live tab shows, built from one worker's heartbeat."""

import unittest

from tests.ui.test_table_columns import worker_row


class TestWorkerRow(unittest.TestCase):
    def test_a_busy_worker_reports_its_task_and_how_long_it_has_held_it(self) -> None:
        row = worker_row()
        self.assertEqual(row["host"], "box-1")
        self.assertEqual(row["task"], "abcdabcdabcd")
        self.assertEqual((row["task_age"], row["task_age_s"]), ("7s", 7))


if __name__ == "__main__":
    unittest.main()
