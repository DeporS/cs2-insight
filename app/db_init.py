import json
import psycopg2
from datetime import datetime
import os
import logging

# Setup logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": "db",
    "port": 5432
}

def create_tables(conn):
    """Create tables if they don't exist."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS skins (
                id SERIAL PRIMARY KEY,
                market_hash_name TEXT UNIQUE
            );
                market_hash_name TEXT UNIQUE
            );

            CREATE TABLE IF NOT EXISTS snapshots (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS skin_prices (
                id SERIAL PRIMARY KEY,
                skin_id INT REFERENCES skins(id),
                snapshot_id INT REFERENCES snapshots(id),
                suggested_price NUMERIC,
                min_price NUMERIC,
                quantity INT
            );
            """)
            conn.commit()
            print("Tables created (If not existed).")
    except Exception as e:
        logging.error(f"Error creating tables: {e}")
        conn.rollback()
        raise


def insert_snapshot(conn):
    """Create a new snapshot and return its ID."""
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO snapshots (timestamp) VALUES (%s) RETURNING id;", (datetime.now(),))
            snapshot_id = cur.fetchone()[0]
            conn.commit()
            logging.info(f"Inserted snapshot with ID: {snapshot_id}")
            return snapshot_id
    except Exception as e:
        logging.error(f"Error inserting snapshot: {e}")
        conn.rollback()
        raise


def insert_data(conn, data, snapshot_id):
    """Insert skin and price data for a given snapshot."""
    try:
        with conn.cursor() as cur:
            for item in data:
                cur.execute("""
                    INSERT INTO skins (market_hash_name)
                    VALUES (%s)
                    ON CONFLICT (market_hash_name) DO NOTHING
                    RETURNING id;
                """, (item["market_hash_name"],))
                
                res = cur.fetchone()
                if res:
                    skin_id = res[0]
                else:
                    cur.execute("SELECT id FROM skins WHERE market_hash_name = %s;", (item["market_hash_name"],))
                    skin_id = cur.fetchone()[0]

                cur.execute("""
                    INSERT INTO skin_prices (skin_id, snapshot_id, suggested_price, min_price, quantity)
                    VALUES (%s, %s, %s, %s, %s);
                """, (
                    skin_id,
                    snapshot_id,
                    item["suggested_price"],
                    item["min_price"],
                    item["quantity"]
                ))

            conn.commit()
            logging.info(f"Inserted {len(data)} records. Snapshot ID: {snapshot_id}.")
    except Exception as e:
        logging.error(f"Error inserting data: {e}")
        conn.rollback()
        raise


def load_data_to_postgres():
    """Main ETL entrypoint: load JSON and write to database."""
    conn = None
    try:
        with open("skins.json", "r", encoding="utf-8") as f:
            data = json.load(f)

        conn = psycopg2.connect(**DB_CONFIG)
        logging.info("Database connection established.")

        create_tables(conn)
        snapshot_id = insert_snapshot(conn)
        insert_data(conn, data, snapshot_id)

        logging.info("Data successfully loaded into database.")
    except FileNotFoundError:
        logging.exception(f"JSON file not found at path: {json_path}")
    except OperationalError as e:
        logging.exception(f"Database connection error: {e}")
    except Exception as e:
        logging.exception(f"Unexpected error: {e}")
    finally:
        if conn:
            try:
                conn.close()
                logging.info("Connection closed.")
            except Exception as e:
                logging.error(f"Error closing connection: {e}")
