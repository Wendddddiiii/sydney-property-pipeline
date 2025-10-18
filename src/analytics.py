import logging
from db_setup import DatabaseSetup
import pandas as pd

logging.basicConfig(level = logging.INFO)
logger = logging.getLogger(__name__)

class PropertyAnalytics:
    
    def __init__(self):
        self.db = DatabaseSetup()
        self.db.connect()

    def price_by_distance(self):
        logger.info("\n===Price Analysis by Distance from CBD")

        query = """
        select
            distance_category,
            count(*) as num_properties,
            avg(price)::NUMERIC(10, 2) as avg_price,
            avg(price_per_sqm)::NUMERIC(10, 2) as avg_price_per_sqm,
            avg(num_bed)::NUMERIC(3,1) as avg_bedrooms
        from properties_processed
        where distance_category is not NULL
        group by distance_category
        order by
            case distance_category
                when 'Inner City' then 1
                when 'Inner Suburbs' then 2
                when 'Middle Suburbs' then 3
                when 'Outer Suburbs' then 4
            end;
        """

        df = pd.read_sql(query, self.db.conn)
        print(df.to_string(index = False))
        return df

    def house_vs_apt(self):
        logger.info("\n=== House v.s. Apt")

        query = """
        select 
            case when is_house then 'House' else 'Apt' end as property_category,
            count(*) as count,
            avg(price)::NUMERIC(10, 2) as avg_price,
            avg(price_per_sqm)::NUMERIC(10, 2) as avg_price_per_sqm,
            avg(num_bed)::NUMERIC(3, 1) as avg_bedrooms,
            avg(num_bath)::NUMERIC(3, 1) as avg_bathrooms
        from properties_processed
        group by is_house
        order by is_house desc;
        """

        df = pd.read_sql(query, self.db.conn)
        print(df.to_string(index=False))
        return df
    
    def top_suburbs_by_value(self):
        #Find suburbs with best value (lower price per sqm)
        logger.info("\n=== Top 10 Suburbs by Value (Price/SqM) ===")
        query = """
            select suburb, count(*) as num_properties, avg(price)::NUMERIC(10, 2) as avg_price, avg(price_per_sqm)::NUMERIC(10, 2) as avg_price_per_sqm, avg(km_from_cbd)::NUMERIC(4,1) as avg_distance_cbd
            from properties_processed
            where price_per_sqm is not NULL
            group by suburb
            having count(*) >= 10 
            order by avg_price_per_sqm asc
            limit 10;
        """

        df = pd.read_sql(query, self.db.conn)
        print(df.to_string(index=False))
        return df

    def most_expensive_suburbs(self):
        logger.info("\n === Top 10 most expensive suburbs===")
        query = """
        SELECT 
            suburb,
            COUNT(*) as num_properties,
            AVG(price)::NUMERIC(10,2) as avg_price,
            MAX(price)::NUMERIC(10,2) as max_price,
            AVG(km_from_cbd)::NUMERIC(4,1) as avg_distance_cbd
        FROM properties_processed
        GROUP BY suburb
        HAVING COUNT(*) >= 5
        ORDER BY avg_price DESC
        LIMIT 10;
        """
        
        df = pd.read_sql(query, self.db.conn)
        print(df.to_string(index=False))
        return df

    def close(self):
        self.db.close()

def main():
    analytics = PropertyAnalytics()

    try: 
        analytics.price_by_distance()
        analytics.house_vs_apt()
        analytics.top_suburbs_by_value()
        analytics.most_expensive_suburbs()
        logger.info("\n Analytics Complete")
    except Exception as e:
        logger.error(f"Analytics failed: {e}")
    finally:
        analytics.close()

if __name__ == "__main__":
    main()


