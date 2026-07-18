"""Module: Transform (T) Layer.

Reads raw localized CSV datasets out of 'data/raw/', maps columns to uppercase,
triggers individual structural cleaning utilities, logs statistics, and writes 
intermediate Parquet and CSV structures to 'data/staging/'.
"""

import logging
from pathlib import Path
import pandas as pd

# Configure basic logging fallback metrics for independent module execution testing
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# ================================================================================================
# DETACHED WORKER TRANSFORMATION UTILITIES OR  TRACK 1: CUSTOMER CSV CLEANING FUNCTIONS (Workers)
# ================================================================================================


def clean_customers(df):
    """Clean and standardize customer profile data."""
    customers_clean = df.copy()
    customers_clean.columns = customers_clean.columns.str.strip().str.upper()

    customers_clean = customers_clean[[
        "ID", "NAME", "GENDER", "DATEOFBIRTH", "EMAIL", "COUNTRY", "CITY", "CREATED_AT", "UPDATED_AT"
    ]]

    customers_clean["COUNTRY"] = customers_clean["COUNTRY"].fillna("Unknown")

    customers_clean["DATEOFBIRTH"] = pd.to_datetime(
        customers_clean["DATEOFBIRTH"],
        format="%d.%m.%y",
        errors="coerce"
    )

    customers_clean["CREATED_AT"] = pd.to_datetime(
        customers_clean["CREATED_AT"],
        errors="coerce"
    )

    customers_clean["UPDATED_AT"] = pd.to_datetime(
        customers_clean["UPDATED_AT"],
        errors="coerce"
    )
    
    return customers_clean


def clean_products(df):
    """Clean, standardize, and filter product catalog data."""
    products_clean = df.copy()
    products_clean.columns = products_clean.columns.str.strip().str.upper()

    products_clean["COLOR"] = (
        products_clean["COLOR"]
        .str.lower()
        .str.title()
    )

    products_clean["CREATED_AT"] = pd.to_datetime(
        products_clean["CREATED_AT"],
        format="%m/%d/%Y %H:%M",
        errors="coerce"
    )

    products_clean["UPDATED_AT"] = pd.to_datetime(
        products_clean["UPDATED_AT"],
        format="%m/%d/%Y %H:%M",
        errors="coerce"
    )

    products_clean = products_clean[[
        "ID", "NAME", "CODE", "CATEGORY", "PRICE", "CURRENCY", "COLOR", "CREATED_AT", "UPDATED_AT"
    ]] 
    
    return products_clean


def clean_sales(df):
    """Clean transaction metrics and structure sales data."""
    sales_clean = df.copy()
    sales_clean.columns = sales_clean.columns.str.strip().str.upper()

    sales_clean["QUANTITY"] = sales_clean["QUANTITY"].fillna(0)
    sales_clean["TOTAL_AMOUNT"] = sales_clean["TOTAL_AMOUNT"].fillna(0)
    sales_clean["CURRENCY"] = sales_clean["CURRENCY"].fillna("UNKNOWN")

    sales_clean["CREATED_AT"] = pd.to_datetime(
        sales_clean["CREATED_AT"],
        format="%m/%d/%Y %H:%M",
        errors="coerce"
    )

    sales_clean["UPDATED_AT"] = pd.to_datetime(
        sales_clean["UPDATED_AT"],
        format="%m/%d/%Y %H:%M",
        errors="coerce"
    )

    sales_clean["SALES_DATE"] = pd.to_datetime(
        sales_clean["SALES_DATE"],
        errors="coerce"
    )

    sales_clean = sales_clean[[
        "CUSTOMER_ID", "PRODUCT_ID", "SALES_DATE", "QUANTITY", "TOTAL_AMOUNT", "CURRENCY", "CREATED_AT", "UPDATED_AT"
    ]]
    
    return sales_clean


def clean_country(df):
    """Propagate country structural data and remove duplicate records."""
    country_clean = df.copy()
    country_clean.columns = country_clean.columns.str.strip().str.upper()
    
    country_clean = country_clean[["CODE", "NAME"]]
    country_clean = country_clean.drop_duplicates()

    return country_clean


# ================================================================================================
# MASTER MANAGER ENCAPSULATION FUNCTION OR  TRACK 1: MAIN CSV TRANSFORMATION ENTRY POINT (Manager)
# ================================================================================================


def run_customer_csv_transformations() -> dict:
    """Primary operational coordinator for the Customer CSV Transformation layer.

    REPLACES old main() to load raw CSV streams, clean tables via Pandas, 
    and export backup staging snapshots to local storage.

    Returns
    -------
    dict
        A dictionary containing the active cleaned pandas DataFrames.
    """
    logging.info("📊 Initializing Customer CSV Data Transformations Pipeline Layer...")

    # Establish relative directory boundaries
    raw_data_dir = Path("data/raw")
    staging_data_dir = Path("data/staging")
    staging_data_dir.mkdir(parents=True, exist_ok=True)

    # 1. Ingest raw CSV data objects into memory
    logging.info("Reading raw CSV files from data/raw folder...")
    country_df = pd.read_csv(raw_data_dir / "country_raw.csv")
    customer_df = pd.read_csv(raw_data_dir / "customer_raw.csv")
    product_df = pd.read_csv(raw_data_dir / "product_raw.csv")
    sales_df = pd.read_csv(raw_data_dir / "sales_raw.csv")

    # 2. Trigger individual data transformation cleaning jobs
    logging.info("Applying custom worker transforms on DataFrames...")
    customer_df_clean = clean_customers(customer_df)
    product_df_clean = clean_products(product_df)
    sales_df_clean = clean_sales(sales_df)
    country_df_clean = clean_country(country_df)

    # 3. Print verification metric outputs to screen
    logging.info("✅ Pandas transformation step executions completed.")
    logging.info(f"🔹 Cleaned Country Table size:   {len(country_df_clean)} rows")
    logging.info(f"🔹 Cleaned Customers Table size: {len(customer_df_clean)} rows")
    logging.info(f"🔹 Cleaned Products Table size:  {len(product_df_clean)} rows")
    logging.info(f"🔹 Cleaned Sales Table size:     {len(sales_df_clean)} rows")

    logging.info(f"Country duplicates BEFORE execution: {country_df.duplicated().sum()}")
    logging.info(f"Country duplicates AFTER execution:  {country_df_clean.duplicated().sum()}")

    # 4. Map cleaned structures to our local table loop array
    staging_tables = {
        "country": country_df_clean,
        "customer": customer_df_clean,
        "product": product_df_clean,
        "sales": sales_df_clean
    }

    # 5. Loop export to populate local data/staging folders (CSV and Parquet formats)
    logging.info("Writing intermediate backup data snapshots to data/staging/...")
    for table_name, df in staging_tables.items():
        csv_file = staging_data_dir / f"{table_name}_staging.csv"
        parquet_file = staging_data_dir / f"{table_name}_staging.parquet"

        # Overwrite/write local storage files
        df.to_csv(csv_file, index=False)
        try:
            df.to_parquet(parquet_file, index=False)
            logging.info(f"💾 Saved intermediate file: {csv_file.name}")
            logging.info(f"💾 Saved intermediate file: {parquet_file.name}")
        except Exception as e:
            logging.error(f"❌ Failed to archive snapshot parquet file {parquet_file.name}: {e}")

    # Return the clean dictionary memory tables back to our master orchestrator script
    return staging_tables


# =============================================================================
# STANDALONE ISOLATED RUN CAPABILITY
# =============================================================================

if __name__ == "__main__":
    logging.info("=== Executing Standalone Transform Module Diagnostic Test ===")
    run_customer_csv_transformations()
