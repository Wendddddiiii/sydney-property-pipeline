import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
from config import Config
from db_setup import DatabaseSetup

logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseLoader:
    """load data from csv into postgresql"""
    def __init__(self):
        self.config = Config()
        self.db = DatabaseSetup()
        self.db.connect()

    def load_csv_to_db(self, csv_path, table_name = 'properties_raw'):
        try:
            logger.info(f"Reading CSV from {csv_path}...")
            df = pd.read_csv(csv_path)
            logger.info(f"Loaded {len(df)} rows")

            if 'date_sold' in df.columns:
                df['date_sold'] = pd.to_datetime(df['date_sold'], errors='coerce', format='%d/%m/%Y')

            columns = df.columns.tolist()
            values = [tuple(x) for x in df.values]

            cols_str = ', '.join(columns)
            query = f"INSERT INTO {table_name}({cols_str}) VALUES %s"

            # Execute batch insert
            logger.info(f"Inserting data into {table_name}...")
            execute_values(self.db.cursor, query, values, page_size=1000)
            self.db.conn.commit()

            logger.info(f"Successfully inserted {len(df)} rows into {table_name}")

        except Exception as e:
            logger.error(f"Error loading data: {e}")
            self.db.conn.rollback()
            raise
    
    def verify_data(self, table_name='properties_raw'):
        """Verify data was loaded correctly"""
        try:
            # Count rows
            query = f"SELECT COUNT(*) FROM {table_name};"  # 确保这里是 {table_name}
            self.db.cursor.execute(query)
            count = self.db.cursor.fetchone()[0]
            logger.info(f"Total rows in {table_name}: {count}")
            
            # Show sample
            query = f"SELECT * FROM {table_name} LIMIT 5;"
            self.db.cursor.execute(query)
            rows = self.db.cursor.fetchall()
            
            logger.info(f"\nSample data from {table_name}:")
            for row in rows:
                logger.info(row)
            
            # Show basic stats
            query = f"""
            SELECT 
                COUNT(*) as total_properties,
                AVG(price) as avg_price,
                MIN(price) as min_price,
                MAX(price) as max_price,
                COUNT(DISTINCT suburb) as num_suburbs
            FROM {table_name}
            WHERE price IS NOT NULL;
            """
            self.db.cursor.execute(query)
            stats = self.db.cursor.fetchone()
            
            logger.info(f"\nBasic statistics:")
            logger.info(f"Total properties: {stats[0]}")
            logger.info(f"Average price: ${stats[1]:,.2f}")
            logger.info(f"Min price: ${stats[2]:,.2f}")
            logger.info(f"Max price: ${stats[3]:,.2f}")
            logger.info(f"Number of suburbs: {stats[4]}")
            
        except Exception as e:
            logger.error(f"Error verifying data: {e}")
            raise

    def close(self):
        self.db.close()


def main():
    loader = DatabaseLoader()

    try:
        import os
        import glob
        processed_files = glob.glob('data/processed/*csv')
        if not processed_files:
            logger.error("No processed CSV files found in data/processed")
            logger.info("Please run data_loader.py first")
            return
        #use most recent file
        latest_file = max(processed_files, key=os.path.getctime)
        logger.info(f"Using file: {latest_file}")

        loader.load_csv_to_db(latest_file)
        loader.verify_data()
        logger.info("\nData loading complete!")
    except Exception as e:
        logger.error(f"Failed to load data: {e}")
    
    finally:
        loader.close()


if __name__ == "__main__":
    main()