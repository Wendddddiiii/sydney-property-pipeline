import pandas as pd
import logging
from datetime import datetime
from src.config import Config

logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataLoader:
    # Load and process property data from CSV

    def __init__(self):
        self.config = Config()

    def load_raw_data(self, filename = 'housing_data.csv'):
        filepath = f"{self.config.RAW_DATA_PATH}/{filename}"

        try: 
            df = pd.read_csv(filepath)
            logger.info(f"Loaded {len(df)} records from {filepath}")
            logger.info(f"Columns: {df.columns.tolist()}")
            return df
        except FileNotFoundError:
            logger.error(f"File not found: {filepath}")
            logger.error("Please download a dataset and place at data/raw/")
            raise
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise
    

    def explore_data(self, df):
        # quick data exploration
        logger.info("\n=== Data overview ===")
        logger.info(f"Shape: {df.shape}")
        logger.info(f"Columns: {df.columns.tolist()}")
        logger.info(f"\nFirst few rows: \n{df.head()}")
        logger.info(f"\nData types:\n{df.dtypes}")
        logger.info(f"\nMissing values: \n{df.isnull().sum()}")
        logger.info(f"\nBasic Stats:\n{df.describe()}")

    def clean_data(self, df):
        #basic data cleaning
        initial_count = len(df)

        df = df.drop_duplicates()
        logger.info(f"Removed {initial_count - len(df)} duplicate rows")

        columns = [
            'price',
            'date_sold',
            'suburb',
            'num_bath',
            'num_bed',
            'num_parking',
            'property_size',
            'type',
            'suburb_population',
            'suburb_median_income',
            'suburb_sqkm',
            'suburb_lat',
            'suburb_lng',
            'suburb_elevation',
            'cash_rate',
            'property_inflation_index',
            'km_from_cbd'
        ]

        
        logger.info(f"Available columns: {df.columns.tolist()}") 
        df = df.dropna(subset=['price', 'suburb', 'type', 'km_from_cbd'])
        logger.info(f"Cleaned data: {len(df)} records remaining")
        return df

    def save_processed_data(self, df, filename = None):
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"properties_processed_{timestamp}.csv"
        
        filepath = f"{self.config.PROCESSED_DATA_PATH}/{filename}"
        df.to_csv(filepath, index = False)
        logger.info(f"Saved processed data to {filepath}")
        return filepath

def main():
    loader = DataLoader()
    logger.info("Loading raw data...")
    df = loader.load_raw_data()

    loader.explore_data(df)

    logger.info("\nCleaning data...")
    df_clean = loader.clean_data(df)

    logger.info("\nSaving processed data...")
    loader.save_processed_data(df_clean)
    logger.info("\n Data loading test complete.")


if __name__ == "__main__":
    main()