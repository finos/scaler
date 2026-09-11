"""Per-browser view state: paging, sorting and settings are served per socket.

The web GUI sends each browser one page of workers rather than the whole fleet.
The page index, the sort column and the chart settings all live on the connection, not in shared state.
"""

import unittest

from scaler.config.types.address import AddressConfig
from scaler.ui.app import (
    WORKER_DETAILS_PAGE_SIZE,
    WORKERS_PAGE_SIZE,
    BrowserView,
    WebGUIConfig,
    WebUIApp,
    _RenderCache,
    paginate,
)


def make_app(worker_count: int) -> WebUIApp:
    app = WebUIApp(WebGUIConfig(monitor_address=AddressConfig.from_string("tcp://127.0.0.1:6380")))
    app._workers_data = {
        f"worker-{index}": {
            "name": f"worker-{index}",
            "manager_id": "pod-1",
            "sent": index,
            "lag": f"{worker_count - index}us",
            "lag_us": worker_count - index,
            "last_s": index,
            "last_seen": f"{index}s",
        }
        for index in range(worker_count)
    }
    app._total_workers = worker_count
    return app


class TestPaginate(unittest.TestCase):
    def test_pages_are_clamped_to_what_exists(self) -> None:
        items = list(range(120))
        rows, page, total_pages = paginate(items, page=99, size=50)
        self.assertEqual((page, total_pages), (2, 3))
        self.assertEqual(rows, list(range(100, 120)))

    def test_negative_page_clamps_to_first(self) -> None:
        rows, page, total_pages = paginate(list(range(10)), page=-5, size=50)
        self.assertEqual((page, total_pages), (0, 1))
        self.assertEqual(rows, list(range(10)))

    def test_empty_still_reports_one_page(self) -> None:
        rows, page, total_pages = paginate([], page=0, size=50)
        self.assertEqual((rows, page, total_pages), ([], 0, 1))


class TestBrowserView(unittest.TestCase):
    def test_unknown_sort_column_is_ignored(self) -> None:
        view = BrowserView()
        view.apply_view({"workers_sort": "not_a_column"})
        self.assertIsNone(view.workers_sort)

    def test_known_sort_column_and_direction_are_kept(self) -> None:
        view = BrowserView()
        view.apply_view({"workers_sort": "sent", "workers_sort_ascending": False})
        self.assertEqual(view.workers_sort, "sent")
        self.assertFalse(view.workers_sort_ascending)

    def test_negative_page_is_rejected(self) -> None:
        view = BrowserView()
        view.apply_view({"workers_page": -3})
        self.assertEqual(view.workers_page, 0)

    def test_invalid_settings_are_ignored(self) -> None:
        view = BrowserView()
        view.apply_settings({"stream_window": 7, "memory_scale": "sideways"})
        self.assertEqual(view.settings(), {"stream_window": 5, "memory_scale": "linear"})

    def test_valid_settings_are_applied(self) -> None:
        view = BrowserView()
        view.apply_settings({"stream_window": 30, "memory_scale": "log"})
        self.assertEqual(view.settings(), {"stream_window": 30, "memory_scale": "log"})


class TestWorkersSection(unittest.TestCase):
    def test_browser_receives_one_page_but_the_whole_fleet_count(self) -> None:
        app = make_app(WORKERS_PAGE_SIZE * 2 + 3)
        section = app._workers_section(BrowserView(), _RenderCache())

        self.assertEqual(len(section["workers"]), WORKERS_PAGE_SIZE)
        self.assertEqual(section["workers_total"], WORKERS_PAGE_SIZE * 2 + 3)
        self.assertEqual(section["workers_pages"], 3)
        self.assertEqual(section["workers_page"], 0)

    def test_out_of_range_page_is_clamped_and_reported_back(self) -> None:
        app = make_app(60)
        view = BrowserView(workers_page=99)
        section = app._workers_section(view, _RenderCache())

        self.assertEqual(section["workers_page"], 1)
        self.assertEqual(view.workers_page, 1)  # the clamp sticks, so the next tick agrees
        self.assertEqual(len(section["workers"]), 10)

    def test_sorting_runs_over_the_whole_fleet_not_the_page(self) -> None:
        app = make_app(120)
        view = BrowserView(workers_sort="sent", workers_sort_ascending=False)
        section = app._workers_section(view, _RenderCache())

        # highest "sent" in the fleet leads, even though it is not in the unsorted first page
        self.assertEqual([worker["sent"] for worker in section["workers"]][:3], [119, 118, 117])

    def test_preformatted_column_sorts_by_its_raw_value(self) -> None:
        app = make_app(12)
        view = BrowserView(workers_sort="lag", workers_sort_ascending=True)
        section = app._workers_section(view, _RenderCache())

        # lag renders as "1us".."12us"; sorting the display strings would put "10us" before "2us"
        self.assertEqual([worker["lag_us"] for worker in section["workers"]][:3], [1, 2, 3])

    def test_two_browsers_get_their_own_page_and_order(self) -> None:
        app = make_app(120)
        cache = _RenderCache()
        first = app._workers_section(BrowserView(workers_page=1), cache)
        second = app._workers_section(BrowserView(workers_sort="sent", workers_sort_ascending=False), cache)

        self.assertEqual(first["workers_page"], 1)
        self.assertEqual(second["workers_page"], 0)
        self.assertNotEqual(first["workers"][0]["name"], second["workers"][0]["name"])


class TestWorkerDetailsSection(unittest.TestCase):
    def test_detail_is_paged_while_summaries_cover_the_fleet(self) -> None:
        app = make_app(0)
        worker_count = WORKER_DETAILS_PAGE_SIZE * 2
        app._worker_processors = {
            f"worker-{index}": {
                "name": f"worker-{index}",
                "full_name": f"worker-{index}",
                "manager_id": "pod-1",
                "rss_free": 0,
                "processors": [{"rss": 10, "cpu": 1.0, "has_task": True, "task_id": ""}],
            }
            for index in range(worker_count)
        }
        app._worker_managers_data = {"pod-1": {"manager_id": "pod-1"}}

        section = app._worker_details_section(BrowserView(), _RenderCache())
        group = section["worker_details"][0]

        self.assertEqual(len(group["workers"]), WORKER_DETAILS_PAGE_SIZE)  # one page of detail
        self.assertEqual(group["worker_count"], worker_count)  # summary still covers every worker
        self.assertEqual(group["total_processors"], worker_count)
        self.assertEqual(section["worker_details_total"], worker_count)
        self.assertEqual(section["worker_details_pages"], 2)


if __name__ == "__main__":
    unittest.main()
