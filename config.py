# config.py
import os
from dotenv import load_dotenv
from pathlib import Path

class Configuration:
    def __init__(self, dotenv_path: str = None):
        if dotenv_path is None:
            dotenv_path = Path(__file__).resolve().parent / ".env"
        load_dotenv(dotenv_path)

    @property
    def CMS_DATASTORE(self) -> str:
        return os.getenv("CMS_DATASTORE_URL")

    @property
    def OUTPUT_DIR(self) -> str:
        return os.getenv("OUTPUT_DIR", "hospital_datasets")

    @property
    def METADATA_FILE(self) -> str:
        return os.getenv("METADATA_FILE", "last_run_metadata.json")

    @property
    def MAX_WORKERS(self) -> int:
        return int(os.getenv("MAX_WORKERS", 5))