from dotenv import load_dotenv
import base64
import requests
import os
import brotli
import json


def save_data_json():

    load_dotenv()   

    # store API
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
    resp = requests.get(f"{BASE}/items", headers=headers, params=params)

    # check status and convert json
    if resp.status_code == 200:
        skins = resp.json()

        # filter response with useful keys
        keys_to_keep = ["market_hash_name", "suggested_price", "min_price", "quantity"]
        filtered_items = [
            {k: item.get(k) for k in keys_to_keep} for item in skins
        ]
        
        with open("skins.json", "w", encoding="utf-8") as f:
            json.dump(filtered_items, f, indent=4)

    else:
        print(f"Error {resp.status_code}: {resp.text}")

