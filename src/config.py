import os
from dotenv import load_dotenv
import getpass

# Load environment variables
load_dotenv()

class Config:
    """Configuration for the pipeline"""
    
    # Database config
    DB_HOST = "localhost"
    DB_PORT = 5432
    DB_NAME = "property_data"
    DB_USER = "postgres"
    DB_PASSWORD = os.getenv('DB_PASSWORD', '202304')
    
    # Data paths
    RAW_DATA_PATH = "data/raw"
    PROCESSED_DATA_PATH = "data/processed"
    
    # Data processing config
    REQUIRED_COLUMNS = ['price', 'suburb']
    
    # Logging
    LOG_LEVEL = "INFO"