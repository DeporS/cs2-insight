import requests
import os
from dotenv import load_dotenv
load_dotenv()

CSFLOAT_API_KEY = os.getenv("CSFLOAT_API_KEY")

headers = {
    "Authorization": CSFLOAT_API_KEY
}

def get_listings():
    url = "https://csfloat.com/api/v1/listings"
    params = {
        "sort_by": "lowest_price",
        "limit": 5
    }

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching listings: {response.status_code}")
        return None
    

print(get_listings())
