import json
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import os

# load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": "db",
    "port": 5432
}

def create_tables(conn):
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
        """)
        conn.commit()
        print("Tables created (If not existed).")


def insert_snapshot(conn):
    """Creates new snapshot, returns its ID"""
    with conn.cursor() as cur:
        cur.execute("INSERT INTO snapshots (timestamp) VALUES (%s) RETURNING id;", (datetime.now(),))
        snapshot_id = cur.fetchone()[0]
        conn.commit()
        return snapshot_id 


def insert_data(conn, data, snapshot_id):
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
        print(f"Inserted {len(data)} records. Snapshot ID: {snapshot_id}.")



def main():
    # Read data from json
    with open("skins.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    conn = psycopg2.connect(**DB_CONFIG)
    create_tables(conn)
    snapshot_id = insert_snapshot(conn)
    insert_data(conn, data, snapshot_id)
    conn.close()
    print("Inserted data correctly.")

if __name__ == "__main__":
    main()