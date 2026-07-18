"""Module: Ingestion & Extraction Layer.

Handles data retrieval from local files (CSV shutil ingestion) and external 
REST API sources (Typicode JSON payloads), mapping them to raw project directories.
"""

from datetime import datetime
import json
import logging
import os
from pathlib import Path
import pandas as pd
import requests
import shutil

# Configure basic logging fallback if run standalone
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# =============================================================================
# TRACK 1: CUSTOMER CSV FILE INGESTION LOGIC
# =============================================================================


def run_csv_ingestion():
    """Locates the local PC raw customer CSV datasets and copies them into project framework storage.

    Acts as the extraction checkpoint for the file-based dataset pipeline.
    """
    logging.info("📥 Starting Customer CSV Ingestion and File Copy Task...")

    # 1. Define your folders using robust Path configurations
    source_dir = Path(
        r"C:\Users\Kidima\Desktop\Medy_doc_Github_Projects\snowflake_sql_customer_analytics_elt_pipeline\Data\raw"
    )
    raw_data_dir = Path("data/raw")

    # Ensure target data/raw directories exist
    raw_data_dir.mkdir(parents=True, exist_ok=True)

    # 2. List the specific files you want to copy
    file_names = [
        "country_raw.csv",
        "customer_raw.csv",
        "product_raw.csv",
        "sales_raw.csv",
    ]

    # 3. Automatically copy each file to the destination folder
    try:
        for file_name in file_names:
            source_file = source_dir / file_name
            dest_file = raw_data_dir / file_name

            if not source_file.exists():
                logging.warning(
                    f"⚠️ Source file missing on host system: {source_file}"
                )
                continue

            # Copy the file (overwrites existing files automatically)
            shutil.copy(source_file, dest_file)
            logging.info(f"💾 File successfully localized: {file_name}")

        logging.info("🎉 CSV file dataset localization check complete.")

    except Exception as e:
        logging.error(f"❌ Critical CSV file copy ingestion failed: {e}")
        raise e


# =============================================================================
# TRACK 2: TYPICODE REST API EXTRACTION LOGIC
# =============================================================================


def extract_users_data():
    """Extracts raw user profile data from a stable public REST API endpoint."""
    base_url = "https://jsonplaceholder.typicode.com/users"
    logging.info(f"🌐 Sending HTTP GET request to: {base_url}...")

    try:
        response = requests.get(base_url, timeout=10)

        # Raise an exception automatically if the server returns an error code
        response.raise_for_status()
        raw_json_data = response.json()

        # Verify the API actually returned a list of data
        if not isinstance(raw_json_data, list):
            logging.warning(
                "⚠️ API did not return a valid list structure. Verification failed."
            )
            return None

        logging.info(
            f"✅ API request successful! Retrieved {len(raw_json_data)} records."
        )
        return raw_json_data

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ API Extraction Failed: {e}")
        return None


def save_raw_json(data):
    """Saves the raw JSON payload to the project data directory with a timestamp."""
    if data is None:
        logging.warning("⚠️ No valid data to save. Skipping file write operation.")
        return None

    # Resolve project root dynamically relative to the src/ directory position
    project_root = Path(__file__).resolve().parent.parent
    raw_data_dir = project_root / "data" / "raw"
    raw_data_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"{timestamp}_users_raw.json"
    file_path = raw_data_dir / file_name

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    logging.info(f"💾 Raw JSON successfully archived to local path: {file_path}")
    return file_path


def parse_json_to_dataframe(file_path):
    """Loads the saved JSON file and maps flat keys directly into a Pandas DataFrame."""
    logging.info(f"📊 Parsing raw JSON from {file_path.name} into Pandas DataFrame...")

    with open(file_path, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    if not isinstance(raw_data, list):
        logging.error("❌ Parsing aborted: Saved JSON data is not a structured list.")
        return None

    parsed_records = []
    for item in raw_data:
        if not isinstance(item, dict):
            continue

        # Clean, flat mapping of user variables
        record = {
            "USER_ID": item.get("id"),
            "FULL_NAME": item.get("name"),
            "USERNAME": item.get("username"),
            "EMAIL_ADDRESS": item.get("email"),
            "PHONE_NUMBER": item.get("phone"),
            "WEBSITE": item.get("website"),
        }
        parsed_records.append(record)

    df = pd.DataFrame(parsed_records)
    logging.info(f"📋 Extracted API DataFrame complete. Formed structure: {df.shape}")
    return df


def run_api_extraction():
    """Orchestration entry point block specifically for your API Pipeline tracking logic.

    REPLACES old main() to prevent module call overlaps inside the framework.
    """
    logging.info("🚀 Triggering API Extraction Operational Stage...")
    raw_data = extract_users_data()
    saved_file_path = save_raw_json(raw_data)

    if saved_file_path:
        parse_json_to_dataframe(saved_file_path)

    logging.info("🎉 API Extraction checkpoint reached successfully!")


# =============================================================================
# STANDALONE EXECUTION CAPABILITY (For Local Independent Tests Only)
# =============================================================================

if __name__ == "__main__":
    logging.info("=== Running Standalone Extraction Test Run ===")
    # Run track 1 test
    run_csv_ingestion()
    # Run track 2 test
    run_api_extraction()
