# CMS Hospital Data Downloader

This repository includes a `Process` class that automates the daily download and processing of hospital-related datasets from the [CMS provider data metastore](https://data.cms.gov/data.json).

The job supports parallel downloads, automatic snake_case conversion of column headers, and tracks the last successful run using a metadata file that is created locally within the apps environment.

Check out the examples directory for the metadata json output file and previously processed CSV files!

---

## How It Works

1. Downloads CMS metadata from `CMS_DATASTORE_URL`
2. Filters datasets based on the configured theme (e.g., `"Hospitals"`)
3. Identifies all CSV distributions in those datasets
4. Skips any that haven't changed since the last recorded `last_run`
5. Downloads and processes each CSV:
   - Converts headers to `snake_case`
   - Saves the cleaned CSV to `OUTPUT_DIR`
6. Writes a new `last_run_metadata.json` file including:
   - Timestamp of last run
   - Metadata for all files processed

---

## Installation

1. Clone the repo:
    ```bash
    git clone https://github.com/mattparsons-pd/cms-app.git
    cd cms-app
    ```

2. Create a virtual environment (optional but recommended):
    ```bash
    python -m venv venv
    source venv/bin/activate  # or venv\Scripts\activate on Windows
    ```

3. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

4. Create `.env` config file:
    For Linux disto use:
    ```bash
    echo -e "CMS_DATASTORE_URL=https://data.cms.gov/data.json\nOUTPUT_DIR=hospital_datasets\nMAX_WORKERS=5" > .env
    ```
    For command prompt use:
    ```cmd
    (
    echo CMS_DATASTORE_URL=https://data.cms.gov/data.json
    echo OUTPUT_DIR=hospital_datasets
    echo MAX_WORKERS=5
    ) > .env
    ```
    For PowerShell use:
    ```powershell
    @"
    CMS_DATASTORE_URL=https://data.cms.gov/data.json
    OUTPUT_DIR=hospital_datasets
    MAX_WORKERS=5
    "@ | Out-File -Encoding utf8 .env
    ```

5. If you're contributing to the repo, you can install the CI dependencies:
    ```bash
    pip install -r requirements-dev.txt
    ```

6. To run the daily process, you can invoke the app entry point:
    ```bash
    python app.py
    ```
