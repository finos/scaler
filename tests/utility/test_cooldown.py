import unittest
from unittest.mock import patch

from scaler.utility.cooldown import Cooldown


class TestCooldown(unittest.TestCase):
    def test_not_running_initially(self) -> None:
        cooldown = Cooldown(duration_seconds=10)
        self.assertFalse(cooldown.is_running)
        self.assertIsNone(cooldown.remaining_seconds())

    @patch("scaler.utility.cooldown.time.monotonic")
    def test_remaining_seconds_counts_down(self, mock_time) -> None:
        mock_time.return_value = 100.0
        cooldown = Cooldown(duration_seconds=10)
        cooldown.start_if_not_running()

        mock_time.return_value = 104.0
        self.assertEqual(cooldown.remaining_seconds(), 6.0)

    @patch("scaler.utility.cooldown.time.monotonic")
    def test_remaining_seconds_none_once_elapsed(self, mock_time) -> None:
        mock_time.return_value = 100.0
        cooldown = Cooldown(duration_seconds=10)
        cooldown.start_if_not_running()

        mock_time.return_value = 110.0
        self.assertIsNone(cooldown.remaining_seconds())

    @patch("scaler.utility.cooldown.time.monotonic")
    def test_start_if_not_running_does_not_restart_when_already_running(self, mock_time) -> None:
        mock_time.return_value = 100.0
        cooldown = Cooldown(duration_seconds=10)
        cooldown.start_if_not_running()

        mock_time.return_value = 105.0
        cooldown.start_if_not_running()  # should be a no-op, not push the deadline out

        mock_time.return_value = 109.0
        self.assertIsNotNone(cooldown.remaining_seconds())

        mock_time.return_value = 111.0
        self.assertIsNone(cooldown.remaining_seconds())

    @patch("scaler.utility.cooldown.time.monotonic")
    def test_reset_stops_the_timer(self, mock_time) -> None:
        mock_time.return_value = 100.0
        cooldown = Cooldown(duration_seconds=10)
        cooldown.start_if_not_running()
        self.assertTrue(cooldown.is_running)

        cooldown.reset()
        self.assertFalse(cooldown.is_running)
        self.assertIsNone(cooldown.remaining_seconds())

    @patch("scaler.utility.cooldown.time.monotonic")
    def test_can_be_restarted_after_reset(self, mock_time) -> None:
        mock_time.return_value = 100.0
        cooldown = Cooldown(duration_seconds=10)
        cooldown.start_if_not_running()
        cooldown.reset()

        mock_time.return_value = 200.0
        cooldown.start_if_not_running()
        self.assertEqual(cooldown.remaining_seconds(), 10.0)

    def test_zero_duration_disables_the_timer(self) -> None:
        cooldown = Cooldown(duration_seconds=0)
        cooldown.start_if_not_running()
        self.assertFalse(cooldown.is_running)
        self.assertIsNone(cooldown.remaining_seconds())

    def test_negative_duration_raises(self) -> None:
        with self.assertRaises(ValueError):
            Cooldown(duration_seconds=-1)


if __name__ == "__main__":
    unittest.main()
