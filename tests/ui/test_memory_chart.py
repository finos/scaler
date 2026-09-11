"""The memory and CPU chart's CPU axis: round percentages that scale to what the fleet uses."""

import unittest

from scaler.ui.app import CPU_CHART_MINIMUM_PERCENT, MEMORY_CHART_TICKS, MemoryChartState, _cpu_axis_ticks


class TestCPUAxis(unittest.TestCase):
    def test_the_axis_rounds_up_to_a_round_step(self) -> None:
        for peak, maximum in ((37.0, 40.0), (130.0, 200.0), (400.0, 400.0), (401.0, 800.0), (6400.0, 8000.0)):
            with self.subTest(peak=peak):
                ticks = _cpu_axis_ticks(peak)
                self.assertEqual(ticks[-1], maximum)
                self.assertEqual(len(ticks), MEMORY_CHART_TICKS, "one tick per memory gridline")

    def test_ticks_are_evenly_spaced_from_zero(self) -> None:
        self.assertEqual(_cpu_axis_ticks(130.0), [0, 50, 100, 150, 200])

    def test_an_idle_fleet_reads_against_the_floor(self) -> None:
        """The few percent an idle fleet's agents use must not fill the plot."""
        self.assertEqual(_cpu_axis_ticks(0.0)[-1], CPU_CHART_MINIMUM_PERCENT)
        self.assertEqual(_cpu_axis_ticks(3.0), [0, 2.5, 5, 7.5, 10])

    def test_the_chart_labels_its_cpu_axis_from_the_samples_in_its_window(self) -> None:
        chart = MemoryChartState()
        for cpu_percent in (20.0, 130.0, 60.0):
            chart.record_fleet_sample(rss_bytes=1_000_000, cpu_percent=cpu_percent)

        ticks = chart.get_render_data(window_seconds=300, scale="log")["cpu_ticks"]
        self.assertEqual([tick["label"] for tick in ticks], ["0%", "50%", "100%", "150%", "200%"])


if __name__ == "__main__":
    unittest.main()
