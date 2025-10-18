import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
from datetime import datetime
from config import Config
from db_setup import DatabaseSetup

logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ETLPipeline:
    def __init__(self):
        self.config = Config()
        self.db = DatabaseSetup()
        self.db.connect()
    
    def extract_from_raw(self):
        query = """
        select 
            price,
            date_sold,
            suburb,
            num_bath,
            num_bed,
            num_parking,
            property_size,
            type,
            km_from_cbd
        from properties_raw
        where price is not NULL
            and suburb is not NULL
            and type is not NULL;
        """ 
        try:
            logger.info("extracting data from properties_raw")
            df = pd.read_sql(query, self.db.conn)
            logger.info(f"Extracted {len(df)} records")
            return df
        except Exception as e:
            logger.error(f"Eroor extracting data: {e}")
            raise
    
    def transform_data(self, df):
        logger.info("Starting data transformations")
        initial_count = len(df)
        df['price_per_sqm'] = df.apply(
            lambda row: row['price']/ row['property_size']
            if pd.notnull(row['property_size']) and row['property_size'] > 0
            else None,
            axis = 1
        )
        logger.info("Calculated price per square meter")

        df['is_house'] = df['type'].apply(
            lambda x: True if 'house' in str(x).lower() else False
        )
        logger.info("created is_house flag")

        def categorize_distance(km):
            if pd.isnull(km):
                return None
            elif km < 5:
                return 'Inner City'
            elif km < 10:
                return 'Inner Suburbs'
            elif km < 20:
                return 'Middle Suburbs'
            else:
                return 'Outer Suburbs'
        
        df['distance_category'] = df['km_from_cbd'].apply(categorize_distance)
        logger.info("Categorized distance from CBD")
        #remove outliers (properties > $10m or < $100k)
        df = df[(df['price'] >= 100000) & (df['price'] <= 10000000)]
        logger.info(f"Removed outliers: {initial_count - len(df)} records")

        df['num_parking'] = df['num_parking'].fillna(0)
        logger.info("Filled missing parking spaces")

        if 'data_sold' in df.columns:
            df['date_sold'] = pd.to_datetime(df['date_sold'], errors='coerce')
        
        logger.info(f"Transformation complete: {len(df)} records ready for loading")
        return df

    def load_to_processed(self, df):
        try:
            logger.info("Clearing properties_processed table")
            self.db.cursor.execute("TRUNCATE TABLE properties_processed;")
            self.db.conn.commit()

            columns = [
                'price', 'date_sold', 'suburb', 'num_bath', 'num_bed',
                'num_parking', 'property_size', 'type', 'km_from_cbd',
                'price_per_sqm', 'is_house', 'distance_category'
            ]
            #convert dataframe to list of tuples
            values = []
            for _, row in df.iterrows():
                values.append(tuple(row[col] if pd.notnull(row[col])else None for col in columns))

            cols_str = ', '.join(columns)
            query = f"INSERT INTO properties_processed ({cols_str}) VALUES %s"
            logger.info(f"Loading {len(values)} records to properties_processed")
            execute_values(self.db.cursor, query, values, page_size=1000)
            self.db.conn.commit()

            logger.info(f"Successfully loaded {len(values)} records to properties_processed")

        except Exception as e:
            logger.error(f"error loading data: {e}")
            self.db.conn.rollback()
            raise

    def run_data_quality_checks(self):
        logger.info("\n===Running data quality checks ===")

        checks = []
        query = """ select count(*) from properties_processed where price is NULL or suburb is NULL or type is NULL;"""
        self.db.cursor.execute(query)
        null_count = self.db.cursor.fetchone()[0]
        checks.append(("No nulls in critical columns", null_count == 0, f"{null_count} nulls found"))
        query = "select count(*) from properties_processed where price <= 0;"
        self.db.cursor.execute(query)
        invalid_price = self.db.cursor.fetchone()[0]
        checks.append(("All prices positive", invalid_price == 0, f"{invalid_price} invalid prices"))

        query = "select count(*) from properties_processed where km_from_cbd < 0;"
        self.db.cursor.execute(query)
        invalid_dist = self.db.cursor.fetchone()[0]
        checks.append(("all distances are valid", invalid_dist == 0, f"{invalid_dist} invalid distances"))

                       
        query = """
        select count(*) from properties_processed where property_size > 0 and price_per_sqm is NULL;
        """
        self.db.cursor.execute(query)
        missing_cal = self.db.cursor.fetchone()[0]
        checks.append(("price per sqm calculated", missing_cal == 0, f"{missing_cal} missing"))

        all_passed = True
        for check_name, passed, detail in checks:
            status = "PASS" if passed else "FAIL"
            logger.info(f"{status}: {check_name} ({detail})")
            if not passed:
                all_passed = False
            
        if all_passed:
            logger.info("\n All data quality checks passed!")
        else:
            logger.warning("\n Some data quality checks failed")
        return all_passed
    
    def get_summary_stats(self):
        query = """
        select 
            count(*) as total_records,
            count(distinct suburb) as unique_suburbs,
            count(distinct type) as unique_types,
            avg(price)::NUMERIC(10,2) as avg_price,
            min(price)::NUMERIC(10,2) as min_price,
            max(price)::NUMERIC(10,2) as max_price,
            avg(num_bed)::NUMERIC(3,1) as avg_bedrooms,
            avg(price_per_sqm)::NUMERIC(10,2) as avg_price_per_sqm
        from properties_processed;
        """

        self.db.cursor.execute(query)
        stats = self.db.cursor.fetchone()

        logger.info("\n===Summary Statistics===")
        logger.info(f"Total records: {stats[0]:,}")
        logger.info(f"Unique suburbs: {stats[1]}")
        logger.info(f"Property types: {stats[2]}")
        logger.info(f"Average price: ${stats[3]:,.2f}")
        logger.info(f"Price range: ${stats[4]:,.2f} - ${stats[5]:,.2f}")
        logger.info(f"Average bedrooms: {stats[6]}")
        logger.info(f"Average price/sqm: ${stats[7]:,.2f}" if stats[7] else "N/A")

    def close(self):
        self.db.close()
    
def main():
    pipeline = ETLPipeline()
    try:
        logger.info("=" * 60)
        logger.info("STARTING ETL PIPELINE")
        logger.info("=" * 60)

        #extract 
        df_raw = pipeline.extract_from_raw()
        df_transformed = pipeline.transform_data(df_raw)

        pipeline.load_to_processed(df_transformed)
        pipeline.run_data_quality_checks()
        pipeline.get_summary_stats()
        
        logger.info("\n" + "=" * 60)
        logger.info("ETL Pipeline Complete")
        logger.info("=" * 60)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
    finally:
        pipeline.close()

if __name__ == "__main__":
    main()
    