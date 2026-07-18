"""Module: Load (L) Layer.

Handles data validation, watermark checking, and writing cleaned Pandas 
DataFrames directly into Snowflake staging tables for both tracks.
"""

from datetime import datetime
import json
import logging
import os
import re
from pathlib import Path
import pandas as pd
from sqlalchemy import text
from src.db import get_snowflake_engine

# --- PIPELINE TRANSFORM IMPORT ---
from src.transform import (
    clean_country,
    clean_customers,
    clean_products,
    clean_sales
)

# Configure standard visual log format matching your system rules
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# =============================================================================
# DATA CLEANUP HELPERS (SHARED BY WORKFLOWS)
# =============================================================================

def fix_all_dates(df):
    """Finds all pandas datetime columns and forces a UTC timezone format."""
    date_cols = df.select_dtypes(include=['datetime64[ns]', 'datetimetz']).columns
    for col in date_cols:
        if df[col].dt.tz is None:
            df[col] = pd.to_datetime(df[col]).dt.tz_localize('UTC')
        else:
            df[col] = pd.to_datetime(df[col]).dt.tz_convert('UTC')
    return df


def table_has_data(engine, table_name, schema="staging"):
    """Queries Snowflake's information catalog to safely check if data records exist."""
    # Using row_count from metadata avoids transactional locking issues entirely
    query = """
        SELECT ROW_COUNT 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = :schema 
          AND TABLE_NAME = :table_name;
    """
    try:
        with engine.connect() as conn:
            # Pass arguments cleanly as bind parameters to maintain lowercase/uppercase mapping safety
            result = conn.execute(
                text(query), 
                {"schema": schema.upper(), "table_name": table_name.upper()}
            ).scalar()
            
            # If the table exists and row_count is greater than 0, return True
            return result is not None and result > 0
    except Exception as e:
        logging.warning(f"Metadata catalog check failed for {table_name}: {e}. Defaulting to false.")
        return False


# =============================================================================
# TRACK 1 CUSTOMER CSV LOADING LOGIC 
# =============================================================================

def load_dataframe_to_snowflake(df, table_name, schema="staging"):
    """Pushes a pandas DataFrame natively into a Snowflake table and forces an explicit commit."""
    logging.info(f"Preparing to upload DataFrame rows to database: {schema}.{table_name}...")
    
    df_fixed_dates = fix_all_dates(df)
    engine = get_snowflake_engine()
    
    # Using engine.begin() instead of engine.connect() automatically turns on 
    # explicit transaction blocks and issues a COMMIT statement the millisecond the block exits.
    with engine.begin() as conn:
        df_fixed_dates.to_sql(
            name=table_name.upper(), # Force uppercase to align with Snowflake expectations
            con=conn,
            schema=schema.upper(),
            if_exists="append",
            index=False,
            method="multi"
        )
    logging.info(f"✅ Successfully committed metrics table data into: {schema}.{table_name}")


def run_csv_loading(data_raw_dir):
    """Primary operational entry point for the CSV Load layer."""
    logging.info("⏳ Initializing CSV Local Transformations...")
    
    # 1. Read CSV files
    country_df = pd.read_csv(os.path.join(data_raw_dir, "country_raw.csv"))
    country_df.columns = country_df.columns.str.strip().str.upper()

    customer_df = pd.read_csv(os.path.join(data_raw_dir, "customer_raw.csv"))
    customer_df.columns = customer_df.columns.str.strip().str.upper()

    product_df = pd.read_csv(os.path.join(data_raw_dir, "product_raw.csv"))
    product_df.columns = product_df.columns.str.strip().str.upper()

    sales_df = pd.read_csv(os.path.join(data_raw_dir, "sales_raw.csv"))
    sales_df.columns = sales_df.columns.str.strip().str.upper()

    # 2. Clean data
    country_df_clean = clean_country(country_df)
    customer_df_clean = clean_customers(customer_df)
    product_df_clean = clean_products(product_df)
    sales_df_clean = clean_sales(sales_df)

    # 3. Map DataFrames to Snowflake targets
    pipeline_mappings = {
        "country_clean_python": country_df_clean,
        "customers_clean_python": customer_df_clean,
        "products_clean_python": product_df_clean,
        "sales_clean_python": sales_df_clean
    }

    engine = get_snowflake_engine()

    # 4. Loop with fixed metadata lookups
    for target_table, dataframe in pipeline_mappings.items():
        if table_has_data(engine, target_table, schema="staging"):
            logging.info(f"⏩ {target_table.upper()} already has records. Skipping upload to protect existing data.")
        else:
            load_dataframe_to_snowflake(df=dataframe, table_name=target_table, schema="staging")


# =============================================================================
# TRACK 2 REST API LOADING LOGIC (With Staging & Watermarks)
# =============================================================================

def create_api_staging_table(engine):
    """Creates the Snowflake API table if it does not exist yet."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS staging.api_users_stg (
        user_id INTEGER,
        full_name STRING,
        username STRING,
        email_address STRING,
        street STRING,
        suite STRING,
        city STRING,
        zipcode STRING,
        latitude STRING,
        longitude STRING,
        phone_number STRING,
        website STRING,
        company_name STRING,
        company_catch_phrase STRING,
        company_bs STRING,
        load_timestamp TIMESTAMP_NTZ
    );
    """
    logging.info("Checking if staging.api_users_stg table exists...")
    with engine.begin() as conn:
        conn.execute(text(create_table_sql))


def get_latest_watermark(engine) -> datetime:
    """Finds the newest timestamp already saved in Snowflake."""
    query = "SELECT MAX(load_timestamp) FROM staging.api_users_stg;"

    with engine.connect() as conn:
        result = conn.execute(text(query)).scalar()

    if result is None:
        logging.info("Table is empty. Using 1970-01-01 as the starting point.")
        return datetime(1970, 1, 1)

    logging.info(f"The latest data in Snowflake is from: {result}")
    return pd.to_datetime(result).to_pydatetime()


def parse_file_timestamp(filename: str) -> datetime:
    """Extracts the date and time from the file name."""
    match = re.search(r"(\d{4}\d{2}\d{2}_\d{2}\d{2}\d{2})", filename)
    if not match:
        raise ValueError(f"Could not find a timestamp in filename: {filename}")

    timestamp_str = match.group(1)
    return datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")


def transform_and_flatten_json(file_path: str, file_timestamp: datetime) -> pd.DataFrame:
    """Reads the JSON file and flattens the nested data into rows and columns."""
    logging.info(f"Opening and flattening file: {file_path}")
    with open(file_path, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    df_raw = pd.json_normalize(raw_data)

    column_mapping = {
        "id": "user_id",
        "name": "full_name",
        "username": "username",
        "email": "email_address",
        "address.street": "street",
        "address.suite": "suite",
        "address.city": "city",
        "address.zipcode": "zipcode",
        "address.geo.lat": "latitude",
        "address.geo.lng": "longitude",
        "phone": "phone_number",
        "website": "website",
        "company.name": "company_name",
        "company.catchPhrase": "company_catch_phrase",
        "company.bs": "company_bs",
    }

    df_flat = df_raw.rename(columns=column_mapping)
    df_flat = df_flat[list(column_mapping.values())]
    df_flat["load_timestamp"] = file_timestamp

    return df_flat


def run_api_loading(data_raw_dir):
    """Primary operational entry point for the REST API Load layer."""
    logging.info("⏳ Initializing API Local Transformations and Watermark Checks...")
    engine = get_snowflake_engine()
    
    create_api_staging_table(engine)
    snowflake_watermark = get_latest_watermark(engine)

    target_filename = "20260628_173411_users_raw.json"
    file_path = os.path.join(data_raw_dir, target_filename)

    if not os.path.exists(file_path):
        logging.error(f"Critical staging file not found: {file_path}")
        return

    file_timestamp = parse_file_timestamp(target_filename)

    if file_timestamp <= snowflake_watermark:
        logging.info(f"Skipping '{target_filename}'. This file was already loaded.")
        return

    df_to_load = transform_and_flatten_json(file_path, file_timestamp)
    logging.info(f"Uploading {len(df_to_load)} rows to staging.api_users_stg...")

    with engine.begin() as conn:
        df_to_load.to_sql(
            name="API_USERS_STG",
            con=conn,
            schema="STAGING",
            if_exists="append",
            index=False,
            method="multi",
        )
    logging.info("🎉 API Staging dataset successfully synchronized.")


# =============================================================================
# FRAMEWORK ORCHESTRATION LAYER
# =============================================================================

def main():
    logging.info("🚀 Automation Framework Executive Orchestration Started...")

    project_root = Path(__file__).resolve().parent.parent
    data_raw_dir = str(project_root / "data" / "raw")

    run_csv_loading(data_raw_dir)
    run_api_loading(data_raw_dir)

    logging.info("🎉 Framework completed execution for all tracks.")


if __name__ == "__main__":
    main()
