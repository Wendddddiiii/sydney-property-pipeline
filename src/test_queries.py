import logging
from db_setup import DatabaseSetup

logging.basicConfig(level = logging.INFO)
logger = logging.getLogger(__name__)


def run_test_queries():
    db = DatabaseSetup()
    db.connect()

    try:
        # Top 10 most expensive suburbs
        logger.info("\n=== Top 10 Most Expensive suburbs")
        query = """
        select suburb, count(*) as num_properties, avg(price) as avg_price, max(price) as max_price
        from properties_raw where price is not NULL 
        group by suburb
        order by avg_price desc
        limit 10;
        """
        db.cursor.execute(query)
        results = db.cursor.fetchall()
        for row in results:
            print(f"{row[0]}: Avg ${row[2]:,.0f}, Max ${row[3]:,.0f} ({row[1]} properties)")    

        # Properties by Type
        logger.info("\n=== Properties by type ===")
        query = """
        select type, count(*) as count, avg(price) as avg_price
        from properties_raw
        where price is not NULL
        group by type 
        order by count desc;
        """
        db.cursor.execute(query)
        results = db.cursor.fetchall()
        for row in results:
            print(f"{row[0]}: {row[1]} properties, Avg ${row[2]:,.0f}")
        

        # Price by distance from CBD
        logger.info("\n=== Average price by distance from CBD===")
        query = """
        SELECT distance_range, num_properties, avg_price
        from (
            select 
                CASE
                    WHEN km_from_cbd < 5 THEN '0-5km'
                    WHEN km_from_cbd < 10 THEN '5-10km'
                    WHEN km_from_cbd < 20 THEN '10-20km'
                    ELSE '20km+'
                END AS distance_range,
                COUNT(*) AS num_properties,
                AVG(price) AS avg_price
            FROM properties_raw
            WHERE price IS NOT NULL AND km_from_cbd IS NOT NULL
            GROUP BY distance_range
        ) t 
        ORDER BY
            CASE 
                WHEN distance_range = '0-5km' THEN 1
                WHEN distance_range = '5-10km' THEN 2
                WHEN distance_range = '10-20km' THEN 3
                ELSE 4
            END;
        """

        db.cursor.execute(query)
        results = db.cursor.fetchall()
        for row in results:
            print(f"{row[0]}: {row[1]} properties, Avg ${row[2]:,.0f}")
        logger.info("\nTest queries complete")
    except Exception as e:
        logger.error(f"Query failed: {e}")
    finally:
        db.close()
    

if __name__ == "__main__":
    run_test_queries()