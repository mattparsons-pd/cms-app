import os
import csv
import json
import logging
import requests
from datetime import datetime,timezone
from concurrent.futures import ThreadPoolExecutor

from .transform import snake_case


# --- Logging Setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Process:
    ATTR_META_FILENAME = "last_run_metadata.json"

    def __init__(self, url: str, theme: str, output_dir: str, max_workers: int):
        self.url = url
        self.theme = theme
        self.output_dir = output_dir
        self.max_workers = max_workers

    @property
    def last_run_time(self):
        if not os.path.exists(self.ATTR_META_FILENAME):
            return None
        with open(self.ATTR_META_FILENAME, "r") as f:
            return datetime.fromisoformat(json.load(f).get("last_run"))
    
    @property   
    def processed_files(self) -> list[str]:
        if not os.path.exists(self.ATTR_META_FILENAME):
            return None
        with open(self.ATTR_META_FILENAME, "r") as f:
            return json.load(f).get("files_processed")

    def save_last_run_time(self, processed_files: list[dict]):
        data = {
            "last_run": datetime.now(timezone.utc).isoformat(),
            "files_meta_data": processed_files
        }
        with open(self.ATTR_META_FILENAME, "w") as f:
            json.dump(data, f, indent=2)

    def fetch_metadata_theme(self) -> list[dict]:
        logger.info("Fetching CMS metadata...")
        response = requests.get(self.url)
        response.raise_for_status()
        
        filtered = [
            {"modified": item["modified"],"file_location":item["distribution"]}
            for item in response.json()
            if self.theme in item.get("theme", [])
        ]

        if not filtered:
            logger.warning(f"No datasets found with theme '{self.theme}'.")

        return filtered

    def is_modified_since(self, item: dict) -> bool:
        modified = item.get("modified")
        if not modified:
            return False

        # Parse as offset-aware UTC datetime
        mod_time = datetime.fromisoformat(modified)
        if mod_time.tzinfo is None:
            mod_time = mod_time.replace(tzinfo=timezone.utc)

        last_run = self.last_run_time
        return not last_run or mod_time > last_run

    def get_csv_distributions(self, item: dict) -> list[str]:
        return [
            d["downloadURL"]
            for d in item.get("file_location", [])
            if d.get("mediaType") == "text/csv"
        ]
        
    def download_and_process_csv(self, url:str) -> None:
        logger.info(f"Downloading: {url}")
        try:
            response = requests.get(url)
            response.raise_for_status()
            content = response.content.decode("utf-8").splitlines()

            reader = csv.DictReader(content)
            snake_headers = [snake_case(col) for col in reader.fieldnames]
            output_rows = [dict(zip(snake_headers, row.values())) for row in reader]

            file_name = os.path.basename(url.split("?")[0])
            output_path = os.path.join(self.output_dir, file_name)

            os.makedirs(self.output_dir, exist_ok=True)
            with open(output_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=snake_headers)
                writer.writeheader()
                writer.writerows(output_rows)

            logger.info(f"Processed and saved: {output_path}")
        except Exception as e:
            logger.error(f"Failed to process {url}: {e}")

    def run(self):
        logger.info("Starting processing run.")
        metadata = self.fetch_metadata_theme()
        last_run = self.last_run_time

        if last_run is None:
            logger.info("First-time run: processing all datasets matching theme.")
            modified_datasets = metadata
        else:
            modified_datasets = [
                item for item in metadata if self.is_modified_since(item)
            ]

        urls = []
        for dataset in modified_datasets:
            urls.extend(self.get_csv_distributions(dataset))

        if not urls:
            logger.info("No new or updated datasets to download.")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            executor.map(self.download_and_process_csv, urls)

        self.save_last_run_time(processed_files=metadata)
        logger.info("Job completed.")