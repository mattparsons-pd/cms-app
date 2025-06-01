import os
import json
import shutil
import tempfile
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

from utils.process import Process


@pytest.fixture
def test_env():
    # Create a temp dir for output
    tmp_output = tempfile.mkdtemp()
    yield tmp_output
    shutil.rmtree(tmp_output)


@patch("utils.process.requests.get")
def test_fetch_metadata_theme(mock_get, test_env):
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = [
        {"theme": "Hospitals", "modified": datetime.utcnow().isoformat(), "distribution": []},
        {"theme": "Other", "modified": datetime.utcnow().isoformat(), "distribution": []}
    ]
    mock_get.return_value = mock_response

    process = Process(
        url="https://example.com/data.json",
        theme="Hospitals",
        output_dir=test_env,
        max_workers=2
    )
    process.ATTR_META_FILENAME = os.path.join(test_env, "last_run_metadata.json")

    result = process.fetch_metadata_theme()
    assert isinstance(result, list)
    assert len(result) == 1
    assert "modified" in result[0]
    assert "file_location" in result[0] 

def test_save_and_load_last_run_time(test_env):
    meta_file = os.path.join(test_env, "last_run_metadata.json")

    process = Process(
        url="https://example.com",
        theme="Hospitals",
        output_dir=test_env,
        max_workers=1
    )
    process.ATTR_META_FILENAME = meta_file

    files_meta_data = [
        {
            "modified": "2025-04-08",
            "file_location": [
                {
                    "@type": "dcat:Distribution",
                    "downloadURL": "https://data.cms.gov/provider-data/sites/default/files/resources/8392b7d74209bb3dc54ff1b09635e733_1744668307/ASCQR_OAS_CAHPS_BY_ASC.csv",
                    "mediaType": "text/csv"
                }
            ]
        }
    ]

    process.save_last_run_time(files_meta_data)

    assert os.path.exists(meta_file)

    # Load and validate
    with open(meta_file) as f:
        metadata = json.load(f)
        assert "last_run" in metadata
        assert "files_meta_data" in metadata
        assert metadata["files_meta_data"] == files_meta_data

    loaded = process.last_run_time
    assert isinstance(loaded, datetime)
    assert loaded.tzinfo is not None


def test_is_modified_since_logic(test_env):
    meta_file = os.path.join(test_env, "last_run_metadata.json")
    past_time = datetime(2000, 1, 1, tzinfo=timezone.utc)
    with open(meta_file, "w") as f:
        json.dump({"last_run": past_time.isoformat()}, f)

    process = Process(
        url="https://example.com",
        theme="Hospitals",
        output_dir=test_env,
        max_workers=1
    )
    process.ATTR_META_FILENAME = meta_file

    item = {
        "modified": datetime.now(timezone.utc).isoformat()
    }

    assert process.is_modified_since(item) is True