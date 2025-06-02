import os
import csv
import json
import logging
import requests
from datetime import datetime,timezone,timedelta
from concurrent.futures import ThreadPoolExecutor

from .transform import snake_case


# --- Logging Setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Process:
    ATTR_META_FILENAME = "metadata.json"
    def __init__(self, url: str, theme: str, output_dir: str, max_workers: int):
        self.url = url
        self.theme = theme
        self.output_dir = output_dir
        self.max_workers = max_workers

    @property
    def TODAY(self) -> str:
        return datetime.now().strftime("%Y-%m-%d")

    @property
    def METADATA_PATH(self) -> str:
        return os.path.join(self.output_dir, self.TODAY, "runs")
    
    @property
    def DATA_PATH(self) -> str:
        return os.path.join(self.output_dir, self.TODAY , "data")

    @property
    def LAST_RUN_DATE(self):
        # Returns yesterday's date
        return str((datetime.now() - timedelta(days=1)).date())


    def get_processed_files(self,date_str:str) -> list[dict] | None:
        input_path = os.path.join(self.output_dir,date_str,"runs",self.ATTR_META_FILENAME)
        
        if not os.path.exists(input_path):
            logger.warning(f"path for {date_str} does not exist. Using {self.METADATA_PATH} instead.")
            input_path = os.path.join(self.METADATA_PATH,self.ATTR_META_FILENAME)
            
        metadata_dict = self.read_path(input_path)
        return metadata_dict.get("files_meta_data")
    
    def read_path(self,input_path:str) -> dict:
        with open(input_path, "r") as f:
            return json.load(f)
        
    def save_run_time(self, processed_files: list[dict]):
        data = {
            "last_run": datetime.now(timezone.utc).isoformat(),
            "files_meta_data": processed_files
        }
        output_path = os.path.join(self.METADATA_PATH, self.ATTR_META_FILENAME)
        os.makedirs(self.METADATA_PATH, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(data, f, indent=2)

    def fetch_metadata_theme(self) -> list[dict]:
        logger.info("Fetching CMS metadata...")
        response = requests.get(self.url)
        response.raise_for_status()
        
        filtered = [
            {"id": item["identifier"],
             "modified": item["modified"],
             "file_location":item["distribution"]}
            for item in response.json()
            if self.theme in item.get("theme", [])
        ]

        if not filtered:
            logger.warning(f"No datasets found with theme '{self.theme}'.")

        return filtered
    
    def last_processed(self, id: str) -> str | None:
        processed_files = self.get_processed_files(self.LAST_RUN_DATE)
        for file in processed_files:
            if id in file["id"]:
                last_processed_date = file["modified"]
        return last_processed_date or None
        
    def is_modified_since(self, item: dict) -> bool:
        modified = item.get("modified")
        id = item.get("id")
        #account for first time processing runs
        if not modified:
            return False

        last_run = self.last_processed(id)
        return modified > last_run

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
            output_path = os.path.join(self.DATA_PATH, file_name)

            os.makedirs(self.DATA_PATH, exist_ok=True)
            with open(output_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=snake_headers)
                writer.writeheader()
                writer.writerows(output_rows)

            logger.info(f"Processed and saved: {output_path}")
        except Exception as e:
            logger.error(f"Failed to process {url}: {e}")

    def run(self) -> None:
        logger.info("Starting processing run.")
        metadata = self.fetch_metadata_theme()
        last_run = os.path.exists(self.METADATA_PATH)

        if not last_run:
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

        self.save_run_time(processed_files=metadata)
        logger.info("Job completed.")