import unittest

from scaler.ui.app import _MIN_HUE_DISTANCE, _capabilities_color, _extract_hue, _find_best_hue, _hue_distance


class TestHueDistance(unittest.TestCase):
    def test_same_hue(self) -> None:
        self.assertAlmostEqual(_hue_distance(0, 0), 0)
        self.assertAlmostEqual(_hue_distance(180, 180), 0)

    def test_opposite_hues(self) -> None:
        self.assertAlmostEqual(_hue_distance(0, 180), 180)
        self.assertAlmostEqual(_hue_distance(90, 270), 180)

    def test_wraparound(self) -> None:
        self.assertAlmostEqual(_hue_distance(10, 350), 20)
        self.assertAlmostEqual(_hue_distance(350, 10), 20)

    def test_small_distance(self) -> None:
        self.assertAlmostEqual(_hue_distance(100, 110), 10)


class TestExtractHue(unittest.TestCase):
    def test_valid_hsl(self) -> None:
        self.assertAlmostEqual(_extract_hue("hsl(120,65%,50%)"), 120.0)
        self.assertAlmostEqual(_extract_hue("hsl(0,55%,45%)"), 0.0)
        self.assertAlmostEqual(_extract_hue("hsl(359,70%,55%)"), 359.0)

    def test_hex_color_returns_none(self) -> None:
        self.assertIsNone(_extract_hue("#ffffff"))
        self.assertIsNone(_extract_hue("#000000"))

    def test_empty_string_returns_none(self) -> None:
        self.assertIsNone(_extract_hue(""))

    def test_malformed_hsl_returns_none(self) -> None:
        self.assertIsNone(_extract_hue("hsl()"))


class TestFindBestHue(unittest.TestCase):
    def test_no_existing_returns_preferred(self) -> None:
        self.assertAlmostEqual(_find_best_hue(120, []), 120)

    def test_far_enough_returns_preferred(self) -> None:
        # 120 deg is far from 0 deg and 240 deg
        self.assertAlmostEqual(_find_best_hue(120, [0, 240]), 120)

    def test_too_close_uses_largest_gap(self) -> None:
        # existing: [0, 10], preferred 5 is too close to both
        # largest gap is 10->0 (wrapping) = 350 deg, midpoint = (10 + 175) % 360 = 185
        result = _find_best_hue(5, [0, 10])
        self.assertGreaterEqual(_hue_distance(result, 0), _MIN_HUE_DISTANCE)
        self.assertGreaterEqual(_hue_distance(result, 10), _MIN_HUE_DISTANCE)


class TestCapabilitiesColor(unittest.TestCase):
    def test_deterministic(self) -> None:
        """Same string always returns same color."""
        map1: dict[str, str] = {}
        map2: dict[str, str] = {}
        c1 = _capabilities_color("cap_a", map1)
        c2 = _capabilities_color("cap_a", map2)
        self.assertEqual(c1, c2)

    def test_cached(self) -> None:
        """Once assigned, the color never changes even when more items are added."""
        color_map: dict[str, str] = {}
        first = _capabilities_color("alpha", color_map)
        _capabilities_color("beta", color_map)
        _capabilities_color("gamma", color_map)
        self.assertEqual(_capabilities_color("alpha", color_map), first)

    def test_stability_on_new_additions(self) -> None:
        """All previously assigned colors remain unchanged when new items are added."""
        color_map: dict[str, str] = {}
        names = [f"worker_{i}" for i in range(8)]
        assigned = []
        for name in names:
            _capabilities_color(name, color_map)
            assigned.append(color_map[name])

        # Add more items
        for extra in ["extra_a", "extra_b", "extra_c"]:
            _capabilities_color(extra, color_map)

        # Verify originals haven't changed
        for name, original_color in zip(names, assigned):
            self.assertEqual(color_map[name], original_color)

    def test_distinct_hues(self) -> None:
        """All assigned colors have pairwise hue distance >= MIN_HUE_DISTANCE (up to capacity)."""
        color_map: dict[str, str] = {}
        names = [f"capability_{i}" for i in range(10)]
        for name in names:
            _capabilities_color(name, color_map)

        hues = [_extract_hue(v) for v in color_map.values() if _extract_hue(v) is not None]
        for i in range(len(hues)):
            for j in range(i + 1, len(hues)):
                self.assertGreaterEqual(
                    _hue_distance(hues[i], hues[j]),
                    _MIN_HUE_DISTANCE,
                    f"Hues {hues[i]:.0f} deg and {hues[j]:.0f} deg are too close "
                    f"(distance {_hue_distance(hues[i], hues[j]):.1f} deg)",
                )

    def test_manager_and_capability_maps_independent(self) -> None:
        """Separate color maps don't interfere with each other."""
        cap_map: dict[str, str] = {}
        mgr_map: dict[str, str] = {}
        _capabilities_color("shared_name", cap_map)
        _capabilities_color("shared_name", mgr_map)
        # Same input to separate maps produces same result (both start empty)
        self.assertEqual(cap_map["shared_name"], mgr_map["shared_name"])

    def test_no_capabilities_passthrough(self) -> None:
        """Pre-seeded #ffffff for '<no capabilities>' is not overwritten."""
        color_map: dict[str, str] = {"<no capabilities>": "#ffffff"}
        result = _capabilities_color("<no capabilities>", color_map)
        self.assertEqual(result, "#ffffff")


if __name__ == "__main__":
    unittest.main()
