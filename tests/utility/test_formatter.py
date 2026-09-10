import unittest

from scaler.utility.formatter import format_microseconds


class TestFormatMicroseconds(unittest.TestCase):
    """Seconds is the terminal unit, so it has to render whatever reaches it.

    Dividing again there walks off the end of the loop and returns None, which
    the callers concatenate (``top.py``) or serialise (``ui/app.py``).
    """

    def test_sub_millisecond(self):
        self.assertEqual(format_microseconds(999), "1.0ms")

    def test_milliseconds(self):
        self.assertEqual(format_microseconds(1_500), "1ms")

    def test_just_under_one_thousand_seconds(self):
        self.assertEqual(format_microseconds(999_999_999), "999s")

    def test_one_thousand_seconds_and_above_render_as_seconds(self):
        self.assertEqual(format_microseconds(1_000_000_000), "1000s")
        self.assertEqual(format_microseconds(3_600_000_000), "3600s")

    def test_always_returns_a_string(self):
        # Deliberately format-agnostic, so it still holds if the rendering
        # above is ever changed.
        for microseconds in (1_000_000_000, 3_600_000_000, 10**13, 10**18):
            with self.subTest(microseconds=microseconds):
                self.assertIsInstance(format_microseconds(microseconds), str)


if __name__ == "__main__":
    unittest.main()
