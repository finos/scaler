import os
import tempfile
import unittest

from scaler.worker_manager.nested.child_command import load_requirements_content


class TestLoadRequirementsContent(unittest.TestCase):
    def test_returns_literal_string(self):
        content = load_requirements_content("opengris-scaler>=1.0\nboto3\n")
        self.assertEqual(content, "opengris-scaler>=1.0\nboto3\n")

    def test_reads_valid_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("opengris-scaler\n")
            path = f.name
        try:
            content = load_requirements_content(path)
            self.assertEqual(content, "opengris-scaler\n")
        finally:
            os.unlink(path)

    def test_returns_literal_string_when_path_does_not_exist(self):
        content = load_requirements_content("/nonexistent/requirements.txt")
        self.assertEqual(content, "/nonexistent/requirements.txt")
