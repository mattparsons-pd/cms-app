import unittest
from unittest.mock import patch, mock_open, MagicMock
import os
import tempfile
import json

from utils.process import Process

class TestProcess(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.output_dir = self.temp_dir.name
        self.process = Process(
            url="http://mock-url.com",
            theme="Health",
            output_dir=self.output_dir,
            max_workers=1
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    @patch("builtins.open", new_callable=mock_open, read_data='{"files_meta_data": [{"id": "abc", "modified": "2025-05-30"}]}')
    def test_get_processed_files_existing_path(self, mock_file):
        os.makedirs(os.path.join(self.output_dir, self.process.LAST_RUN_DATE, "runs"), exist_ok=True)
        metadata_path = os.path.join(self.output_dir, self.process.LAST_RUN_DATE, "runs", self.process.ATTR_META_FILENAME)
        with open(metadata_path, "w") as f:
            json.dump({"files_meta_data": [{"id": "abc", "modified": "2025-05-30"}]}, f)

        result = self.process.get_processed_files(self.process.LAST_RUN_DATE)
        self.assertEqual(result[0]["id"], "abc")

    def test_save_run_time_creates_file(self):
        processed_files = [{"id": "abc", "modified": "2025-05-30"}]
        self.process.save_run_time(processed_files)

        metadata_path = os.path.join(self.process.METADATA_PATH, "metadata.json")
        self.assertTrue(os.path.exists(metadata_path))

        with open(metadata_path) as f:
            data = json.load(f)
            self.assertIn("last_run", data)
            self.assertEqual(data["files_meta_data"], processed_files)

    @patch("requests.get")
    def test_get_metadata_theme_filters_by_theme(self, mock_get):
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = [
            {"identifier": "abc", "modified": "2025-05-30", "distribution": [], "theme": ["Health"]},
            {"identifier": "def", "modified": "2025-05-29", "distribution": [], "theme": ["Other"]}
        ]
        mock_get.return_value = mock_response

        result = self.process.get_metadata_theme()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], "abc")

    @patch("requests.get")
    def test_download_and_process_csv(self, mock_get):
        csv_data = "Name,Age\nAlice,30\nBob,25"
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.content = csv_data.encode("utf-8")
        mock_get.return_value = mock_response

        url = "http://example.com/test.csv"
        self.process.download_and_process_csv(url)

        expected_output_path = os.path.join(self.process.DATA_PATH, "test.csv")
        self.assertTrue(os.path.exists(expected_output_path))

        with open(expected_output_path) as f:
            lines = f.read().splitlines()
            self.assertEqual(lines[0], "name,age")  # snake_case headers