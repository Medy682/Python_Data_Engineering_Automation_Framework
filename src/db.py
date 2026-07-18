# For your notes
# A docstring generally answers three questions:
#--What does this function do?
#--What are the important steps it performs?
#--What does it return?

import os
from pathlib import Path
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine
from cryptography.hazmat.primitives import serialization

# Configure basic logging fallback metrics for independent module execution testing
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


env_path = Path(__file__).resolve().parents[1] / "environment_variable.env"
load_dotenv(env_path)

def get_snowflake_engine():

       """
    Creates and returns a SQLAlchemy engine for connecting
    to Snowflake using RSA key-pair authentication and
    environment variables.

    The function:
    - Reads Snowflake credentials from environment variables
    - Loads the RSA private key from the configured file path
    - Converts the key into DER bytes
    - Builds a Snowflake SQLAlchemy connection string
    - Creates and returns a reusable SQLAlchemy engine

    Returns:
        sqlalchemy.engine.Engine:
            A SQLAlchemy engine connected to Snowflake.
    """

       user = os.getenv("SNOWFLAKE_USER")
       account = os.getenv("SNOWFLAKE_ACCOUNT")
       warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
       database = os.getenv("SNOWFLAKE_DATABASE")
       schema = os.getenv("SNOWFLAKE_SCHEMA")
       role = os.getenv("SNOWFLAKE_ROLE")
       
        # 1. Look for the environment variable set by Docker or your system
       key_path_str = os.getenv(
        "SNOWFLAKE_PRIVATE_KEY_PATH", 
        "/app/secrets/rsa_key.p8"  # Default Docker fallback
    )
    
    # 2. Convert to a proper Path object
       key_path = Path(key_path_str)
    
    # 3. SMART CHECK: If running locally on Windows, point to your local key
       if not key_path.exists() and os.name == 'nt':
        # Fallback to your exact Windows machine path when Docker isn't running
        key_path = Path("C:/Users/Kidima/PyCharmMiscProject/Hazel/rsa_key.p8")
        logging.info(f"🔄 Docker secret path not found. Falling back to local Windows key: {key_path}")

    # 4. Open and read the key safely
       if not key_path.exists():
               raise FileNotFoundError(f"❌ Critical Error: Snowflake private key could not be found at {key_path}")
       
       with open(key_path, "rb") as key_file:
               p_key = serialization.load_pem_private_key(
                   key_file.read(),
                   password=None
               )
       
       private_key_bytes = p_key.private_bytes(
               encoding=serialization.Encoding.DER,
               format=serialization.PrivateFormat.PKCS8,
               encryption_algorithm=serialization.NoEncryption()
           )
       
       connection_string = (
               f"snowflake://{user}@{account}/"
               f"{database}/{schema}"
               f"?warehouse={warehouse}"
               f"&role={role}"
           )
       
       engine = create_engine(
               connection_string,
               connect_args={
                   "private_key": private_key_bytes
               }
           )
       
       return engine  
       
 

