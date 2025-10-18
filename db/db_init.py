import json
import psycopg2
from datetime import datetime
import os
import logging
import pandas as pd

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
                        
            CREATE TABLE IF NOT EXISTS daily_avg_skin_prices (
                skin_id INT REFERENCES skins(id),
                avg_price NUMERIC,
                avg_suggested NUMERIC,
                avg_quantity NUMERIC,
                date DATE DEFAULT CURRENT_DATE,
                PRIMARY KEY (skin_id, date)
            );
                        
            CREATE TABLE IF NOT EXISTS skin_stats (
                id SERIAL PRIMARY KEY,
                skin_id INT REFERENCES skins(id),
                snapshot_id INT REFERENCES snapshots(id),
                avg_6h NUMERIC,
                avg_24h NUMERIC,
                avg_7d NUMERIC,
                delta_6h NUMERIC,
                delta_24h NUMERIC,
                delta_7d NUMERIC,
                alert_type TEXT
            );

            """)
            conn.commit()
            print("Tables created (If not existed).")
    except Exception as e:
        logging.exception(f"Error creating tables: {e}")
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
        logging.exception(f"Error inserting snapshot: {e}")
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
        logging.exception(f"Error inserting data: {e}")
        conn.rollback()
        raise


def load_data_to_postgres():
    """Main ETL entrypoint: load JSON and write to database."""
    conn = None
    json_path = "skins.json"
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        conn = psycopg2.connect(**DB_CONFIG)
        logging.info("Database connection established.")

        create_tables(conn)
        snapshot_id = insert_snapshot(conn)
        insert_data(conn, data, snapshot_id)

        logging.info("Data successfully loaded into database.")
    except FileNotFoundError:
        logging.exception(f"JSON file not found at path: {json_path}")
    except Exception as e:
        logging.exception(f"Unexpected error: {e}")
    finally:
        if conn:
            try:
                conn.close()
                logging.info("Connection closed.")
            except Exception as e:
                logging.exception(f"Error closing connection: {e}")


def calculate_daily_avg():
    """Calculate daily average prices and save them into daily_avg_prices table."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO daily_avg_skin_prices (skin_id, avg_price, avg_suggested, avg_quantity, date)
                SELECT 
                    sp.skin_id,
                    ROUND(AVG(min_price), 2) AS avg_price,
                    ROUND(AVG(suggested_price), 2) AS avg_suggested,
                    ROUND(AVG(quantity), 2) AS avg_quantity,
                    CURRENT_DATE AS date
                FROM skin_prices sp
                JOIN snapshots s ON sp.snapshot_id = s.id
                WHERE s.timestamp >= NOW() - INTERVAL '24 hours'
                GROUP BY sp.skin_id
                ON CONFLICT (skin_id, date) DO UPDATE 
                SET 
                    avg_price = EXCLUDED.avg_price,
                    avg_suggested = EXCLUDED.avg_suggested,
                    avg_quantity = EXCLUDED.avg_quantity;
            """)
            conn.commit()
            logging.info("Daily average prices calculated and stored.")
    except Exception as e:
        logging.exception(f"Error calculating daily averages: {e}")
    finally:
        if conn:
            try:
                conn.close()
                logging.info("Connection closed.")
            except Exception as e:
                logging.exception(f"Error closing connection: {e}")


def calculate_stats_and_alerts():
    """Compute rolling averages (6h, 24h, 7d) and detect alerts."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    sp.skin_id,
                    sp.min_price,
                    s.timestamp,
                    s.id AS snapshot_id
                FROM skin_prices sp
                JOIN snapshots s ON sp.snapshot_id = s.id
                WHERE s.timestamp >= NOW() - INTERVAL '7 days'
                ORDER BY sp.skin_id, s.timestamp;
            """)
            rows = cur.fetchall()

        df = pd.DataFrame(rows, columns=['skin_id', 'min_price', 'timestamp', 'snapshot_id'])
        if df.empty:
            logging.info("No data available for stats calculation.")
            return
        
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values(["skin_id", "timestamp"])

        def compute_group(group):
            group = group.set_index("timestamp")

            group["min_price"] = pd.to_numeric(group["min_price"], errors="coerce")

            group["avg_6h"] = group["min_price"].rolling("6h").mean().round(2)
            group["avg_24h"] = group["min_price"].rolling("24h").mean().round(2)
            group["avg_7d"] = group["min_price"].rolling("7d").mean().round(2)

            group["delta_6h"] = ((group["min_price"] - group["avg_6h"]) / group["avg_6h"]).round(4)
            group["delta_24h"] = ((group["min_price"] - group["avg_24h"]) / group["avg_24h"]).round(4)
            group["delta_7d"] = ((group["min_price"] - group["avg_7d"]) / group["avg_7d"]).round(4)
            return group.reset_index()

        df_stats = df.groupby("skin_id").apply(compute_group).reset_index(drop=True)

        # detect alerts
        def detect_alerts(row):
            if row["delta_6h"] is None or pd.isna(row["delta_6h"]):
                return None
            if row["delta_6h"] >= 0.10:
                return "6H_PRICE_SPIKE"
            elif row["delta_6h"] < -0.10:
                return "6H_PRICE_DROP"
            return None

        df_stats["alert_type"] = df_stats.apply(detect_alerts, axis=1)

        df_latest = df_stats.sort_values("timestamp").groupby("skin_id").tail(1)

        with conn.cursor() as cur:
            for _, row in df_latest.iterrows():
                cur.execute("""
                    INSERT INTO skin_stats 
                    (skin_id, snapshot_id, avg_6h, avg_24h, avg_7d, delta_6h, delta_24h, delta_7d, alert_type)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, (
                    int(row["skin_id"]),
                    row["snapshot_id"],
                    float(row["avg_6h"]) if pd.notna(row["avg_6h"]) else None,
                    float(row["avg_24h"]) if pd.notna(row["avg_24h"]) else None,
                    float(row["avg_7d"]) if pd.notna(row["avg_7d"]) else None,
                    float(row["delta_6h"]) if pd.notna(row["delta_6h"]) else None,
                    float(row["delta_24h"]) if pd.notna(row["delta_24h"]) else None,
                    float(row["delta_7d"]) if pd.notna(row["delta_7d"]) else None,
                    row["alert_type"]
                ))
            conn.commit()
            logging.info(f"Calculated stats and inserted {len(df_latest)} rows.")
    except Exception as e:
        logging.exception(f"Error calculating stats and alerts: {e}")
    finally:
        if conn:
            try:
                conn.close()
                logging.info("Connection closed.")
            except Exception as e:
                logging.exception(f"Error closing connection: {e}")