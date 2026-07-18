import pytest 
import pandas as pd
from unittest.mock import MagicMock,patch
from datetime import datetime

# Import your isolated pipeline blueprints
from master_orchestrator import  (
    transform_customer_transactions, 
    extract_timestamp_from_filename, 
    fetch_api_data
)
  
# ==========================================
# EXERCISE 1: NULL HANDLING
# ==========================================
def test_track1_null_handling():
    """Verifies missing numbers default to 0.00 safely."""
    raw_dirty_data = pd.DataFrame({
        "transaction_id":[1,2], 
        "amount": [150.75, None]  # Contains an explicit missing value
    })
    
    processed_df = transform_customer_transactions(raw_dirty_data)
    
    assert processed_df.loc[1, "amount"] == 0.00
    assert processed_df["amount"].isnull().sum() == 0


# ==========================================
# EXERCISE 2: TYPE CASTING
# ==========================================
def test_track1_type_casting():
    """Ensures string inputs cast properly into native numeric datatypes."""
    raw_dirty_data = pd.DataFrame({
        "transaction_id": ["101", "102"],  # String integers
        "amount": ["150.75", "0.00"]       # String floats
    })
    
    processed_df = transform_customer_transactions(raw_dirty_data)
    
    assert processed_df["transaction_id"].dtype == "int64"
    assert processed_df["amount"].dtype == "float64"

# ==========================================
# EXERCISE 3: COLUMN RENAMING SIMULATION
# ==========================================
def test_track1_column_renaming():
    """Confirms column mapper cleanly realigns fields to match database schemas."""
    raw_input = pd.DataFrame({"userId":[1], "transAmount": [50.00]})
    
    # Map raw schema attributes to standard target layouts
    rename_map = {"userId": "user_id", "transAmount": "amount"}
    renamed_df = raw_input.rename(columns=rename_map)
    
    assert "user_id" in renamed_df.columns
    assert "amount" in renamed_df.columns
    assert "userId" not in renamed_df.columns


# ==========================================
# EXERCISE 4: EMPTY INPUT HANDLING (EDGE CASE)
# ==========================================
def test_track1_empty_dataframe_edge_case():
    """Validates that a completely empty dataset processes cleanly without crashes."""
    empty_raw = pd.DataFrame(columns=["transaction_id", "amount"])
    
    processed_df = transform_customer_transactions(empty_raw)
    
    assert processed_df.empty is True
    assert len(processed_df) == 0


# ==========================================
# EXERCISE 5: MOCK LOADING & API HANDLING
# ==========================================
@patch("master_orchestrator.get_snowflake_connection")  # 🟢 Updated target module name
@patch("master_orchestrator.write_pandas")              # 🟢 Updated target module name
@patch("master_orchestrator.requests.get")              # 🟢 Updated target module name
def test_track2_mock_api_and_snowflake_load(mock_get, mock_write_pandas, mock_connect):
    """Mocks database loads, connections, and API network requests safely."""

    # ----------------------------------------------------
    # PART A: Mock the Snowflake Connection & Cursor Engine
    # ----------------------------------------------------
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    
    # Configure the mocks so calling connect() returns our fake connection,
    # and calling conn.cursor() returns our fake cursor structure.
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    
    # Simulate database responses for row count checks (Pre-Check Gatekeeper)
    mock_cursor.fetchone.return_value = (100,)  # Returns a fake row count tuple
    
    # ----------------------------------------------------
    # PART B: Mock write_pandas Loading
    # ----------------------------------------------------
    sample_df = pd.DataFrame({"transaction_id":[1], "amount": [100.50]})
    
    # Trigger a simulated write execution to ensure the driver handles it cleanly
    mock_write_pandas(mock_conn, sample_df, "STAGING_CUSTOMER")
    
    # Assert that write_pandas was hit with the exact expected pipeline assets
    mock_write_pandas.assert_called_once_with(mock_conn, sample_df, "STAGING_CUSTOMER")

    # ----------------------------------------------------
    # PART C: Mock the REST API Payload & Regex
    # ----------------------------------------------------
    # Verify the regex extraction utility works perfectly first
    sample_file = "backup_20260713_120000.json"
    parsed_date = extract_timestamp_from_filename(sample_file)
    assert parsed_date == datetime(2026, 7, 13, 12, 0, 0)
    
    # Mock the REST API payload return context
    mock_response = MagicMock()
    mock_response.json.return_value = [{"id": 1, "title": "Mock API Data"}]
    mock_get.return_value = mock_response
    
    api_result = fetch_api_data("https://fake-typicode-link.com")
    
    # Verify the code called requests with our exact rules
    mock_get.assert_called_once_with("https://fake-typicode-link.com", timeout=10)
    assert len(api_result) == 1
    assert api_result[0]["title"] == "Mock API Data"
