import psycopg2
import os
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        database=os.getenv("DB_NAME", "master"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASS", "postgres"),
        port=os.getenv("DB_PORT", 5432)
    )

def create_table(conn):
    create_script = '''
    CREATE TABLE IF NOT EXISTS ev_vehicles (
        vin VARCHAR(20) PRIMARY KEY,
        county VARCHAR(50),
        city VARCHAR(50),
        state VARCHAR(2),
        postal_code VARCHAR(10),
        model_year INT,
        make VARCHAR(30),
        model VARCHAR(30),
        electric_vehicle_type VARCHAR(50),
        cafv_eligibility VARCHAR(100),
        electric_range INT,
        base_msrp INT,
        legislative_district INT,
        dol_vehicle_id BIGINT,
        vehicle_location VARCHAR(100),
        electric_utility VARCHAR(100),
        census_tract BIGINT
    );
    '''
    with conn.cursor() as cur:
        cur.execute(create_script)
        conn.commit()
        logging.info("Table ev_vehicles ensured.")
#test
def main():
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        create_table(conn)
    except Exception as error:
        logging.error(f"An error occurred: {error}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    main()