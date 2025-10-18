import logging
from fastapi import FastAPI, HTTPException
import psycopg2
import psycopg2.extras
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": "db",
    "port": 5432
}

app = FastAPI(title="CS2 Insight - Prices")

def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise HTTPException(status_code=500, detail="Database connection error")
    
@app.get("/prices/")
def get_latest_prices(limit: int = 100):
    """Fetch the latest skin prices from the database."""
    
    sql = """
    SELECT sk.market_hash_name, sp.suggested_price, sp.min_price, sp.quantity
    FROM skin_prices sp
    JOIN snapshots s ON sp.snapshot_id = s.id
    JOIN skins sk ON sp.skin_id = sk.id
    WHERE s.timestamp = (SELECT MAX(timestamp) FROM snapshots)
    """

    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (limit,))
            rows = cur.fetchall()

        for r in rows:
            if r.get("min_price") is not None:
                r["min_price"] = float(r["min_price"])
        return {"count": len(rows), "results": rows}
    except Exception as e:
        logger.error(f"Error fetching latest prices: {e}")
        raise HTTPException(status_code=500, detail="Error fetching latest prices")
    finally:
        try:
            conn.close()
        except:
            pass