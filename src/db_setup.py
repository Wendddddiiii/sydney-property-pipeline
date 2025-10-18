import psycopg2
from psycopg2 import sql
import logging
from src.config import Config

logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

class DatabaseSetup:

    def __init__(self):
        self.config = Config()
        self.conn = None
        self.cursor = None

    def connect(self):
        try: 
            self.conn = psycopg2.connect(
                host = self.config.DB_HOST,
                port = self.config.DB_PORT,
                database = self.config.DB_NAME,
                user = self.config.DB_USER,
                password = self.config.DB_PASSWORD
            )
            self.cursor = self.conn.cursor()
            logger.info("Successfully connected to database")
        except Exception as e:
            logger.error(f"failed to connect to database: {e}")
            raise
    

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")


    def create_raw_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS properties_raw (
            id SERIAL PRIMARY KEY,
            price DECIMAL(12, 2),
            date_sold DATE,
            suburb VARCHAR(100),
            num_bath INTEGER,
            num_bed INTEGER,
            num_parking INTEGER,
            property_size DECIMAL(10, 2),
            type VARCHAR(50),
            suburb_population INTEGER,
            suburb_median_income DECIMAL(10, 2),
            suburb_sqkm DECIMAL(10, 2),
            suburb_lat DECIMAL(10, 6),
            suburb_lng DECIMAL(10, 6),
            suburb_elevation DECIMAL(10, 2),
            cash_rate DECIMAL(5, 4),
            property_inflation_index DECIMAL(10, 4),
            km_from_cbd DECIMAL(10, 2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        try:
            self.cursor.execute(create_table_query)
            self.conn.commit()
            logger.info("Table 'properties_raw' created successfully")
        except Exception as e:
            logger.error(f"error creating table: {e}")
            self.conn.rollback()
            raise

    def create_processed_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS properties_processed(
            id SERIAL PRIMARY KEY,
            price DECIMAL(12, 2) NOT NULL,
            date_sold DATE,
            suburb VARCHAR(100) NOT NULL,
            num_bath INTEGER,
            num_bed INTEGER,
            num_parking INTEGER,
            property_size DECIMAL(10, 2),
            type VARCHAR(50) NOT NULL,
            km_from_cbd DECIMAL(10, 2),
            price_per_sqm DECIMAL(10, 2),
            is_house BOOLEAN,
            distance_category VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            -- Index for common queries
            CONSTRAINT valid_price CHECK(price > 0),
            CONSTRAINT valid_distance CHECK(km_from_cbd >= 0)
        );


        -- Create indexes
        CREATE INDEX IF NOT EXISTS idx_suburb ON properties_processed(suburb);
        CREATE INDEX IF NOT EXISTS idx_type ON properties_processed(type);
        CREATE INDEX IF NOT EXISTS idx_price ON properties_processed(price);
        CREATE INDEX IF NOT EXISTS idx_date ON properties_processed(date_sold);

        """

        try:
            self.cursor.execute(create_table_query)
            self.conn.commit()
            logger.info("Table 'properties_processed' created successfully with indexes")
        except Exception as e:
            logger.error(f"Error creating processed data: {e}")
            self.conn.rollback()
            raise

    def check_tables(self):
        """List all tables in the database"""
        query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public';
        """

        try:
            self.cursor.execute(query)
            tables = self.cursor.fetchall()
            logger.info(f"Tables in database: {[t[0] for t in tables]}")
            return tables
        except Exception as e:
            logger.error(f"Error checking tables: {e}")
            raise
    

    def drop_all_tables(self):
        """Drop all tables (use carefully!)"""
        query = """
        DROP TABLE IF EXISTS properties_raw CASCADE;
        DROP TABLE IF EXISTS properties_processed CASCADE;
        """
        
        try:
            self.cursor.execute(query)
            self.conn.commit()
            logger.info("All tables dropped")
        except Exception as e:
            logger.error(f"Error dropping tables: {e}")
            self.conn.rollback()
            raise


def main():
    db = DatabaseSetup()

    try:
        db.connect()

        logger.info("Checking existing tables...")
        db.check_tables()

        # Check existing tables
        logger.info("\nCreating tables...")
        db.check_tables()

        # Create tables
        logger.info("\nCreating tables...")
        db.create_raw_table()
        db.create_processed_table()

        # Verify
        logger.info("\nVerifying tables created...")
        db.check_tables()
        
        logger.info("\nDatabase setup complete!")
    except Exception as e:
        logger.error(f"Setup failed: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    main()