import base64
import requests
from requests.exceptions import RequestException, Timeout, ConnectionError
import os
import json
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def save_data_json():
    """Fetch data from Skinport API and save filtered JSON locally."""
    try:
        user_id = os.getenv("MY_USER_ID")
        api_key = os.getenv("MY_SECRET_API")
        if not user_id or not api_key:
            raise RuntimeError("User_id or API code missing")

        # Combine ID and Secret code with a colon 
        creds = f"{user_id}:{api_key}"
        token = base64.b64encode(creds.encode()).decode()
        headers = {
            "Authorization": f"Basic {token}",
            "Accept-Encoding": "br",   # requested by skinport
            "User-Agent": "CS2Insight/1.0"  
        }

        BASE = "https://api.skinport.com/v1"

        params = {
            "app_id": 730,      # CS:GO
            "currency": "USD",
            "tradable": 0
        }

        # GET
        logging.info("Fetching data from Skinport API...")
        resp = requests.get(f"{BASE}/items", headers=headers, params=params, timeout=15)

        # check status and convert json
        if resp.status_code == 200:
            skins = resp.json()
            if not isinstance(skins, list):
                raise ValueError("API response is not a list as expected.")

            # filter relevant keys
            keys_to_keep = ["market_hash_name", "suggested_price", "min_price", "quantity"]
            filtered_items = [
                {k: item.get(k) for k in keys_to_keep} for item in skins
            ]
            
            with open("skins.json", "w", encoding="utf-8") as f:
                json.dump(filtered_items, f, indent=4)
            logging.info(f"Filtered data saved to skins.json with {len(filtered_items)} items.")

        else:
            logging.error(f"Error {resp.status_code}: {resp.text}")


    except (ConnectionError, Timeout) as e:
        logging.exception(f"Network error occurred: {e}")
    except RequestException as e:
        logging.exception(f"Request error occurred: {e}")
    except (OSError, json.JSONDecodeError) as e:
        logging.exception(f"File or JSON error occurred: {e}")
    except Exception as e:
        logging.exception(f"An unexpected error occurred: {e}")
    finally:
        logging.info("Data fetch attempt completed.")

