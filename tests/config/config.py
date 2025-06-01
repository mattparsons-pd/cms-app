import os
import tempfile
from pathlib import Path
from config import Configuration

def test_configuration_reads_env_values():
    # Prepare a temporary .env file
    temp_dir = tempfile.TemporaryDirectory()
    temp_env_path = Path(temp_dir.name) / ".env"
    
    env_content = """
    CMS_DATASTORE_URL=https://example.com/data.json
    OUTPUT_DIR=test_output
    METADATA_FILE=test_metadata.json
    MAX_WORKERS=10
    """
    temp_env_path.write_text(env_content.strip())

    # Test
    config = Configuration(dotenv_path=str(temp_env_path))

    assert config.cms_datastore_url == "https://example.com/data.json"
    assert config.output_dir == "test_output"
    assert config.metadata_file == "test_metadata.json"
    assert config.max_workers == 10

    temp_dir.cleanup()