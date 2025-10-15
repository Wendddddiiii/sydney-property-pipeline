
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    """Configuration for the pipeline"""
    
    # Database config
    DB_HOST = "localhost"
    DB_PORT = 5432
    DB_NAME = "property_data"
    DB_USER = "postgres"
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'yourpassword')
    
    # Data paths
    RAW_DATA_PATH = "data/raw"
    PROCESSED_DATA_PATH = "data/processed"
    
    # Data processing config
    REQUIRED_COLUMNS = ['price', 'suburb']  # 根据你下载的数据集调整
    
    # Logging
    LOG_LEVEL = "INFO"
