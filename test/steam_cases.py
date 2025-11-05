import requests
import time
import json

# https://steamcommunity.com/market/pricehistory/?country=DE&currency=3&appid=730&market_hash_name=Prisma%20Case

BASE_URL = "https://steamcommunity.com/market/pricehistory/?country=PL&currency=1&appid=730&market_hash_name="

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/128.0.0.0 Safari/537.36"
}

def get_prisma_case():
    try:
        response = requests.get(BASE_URL + "Prisma%20Case", headers=headers, timeout=10)
        response.raise_for_status()  # Raise an error for HTTP errors
        data = response.json()
        return data
    except requests.Timeout:
        print("Request timed out")
    except requests.RequestException as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    prisma_case_data = get_prisma_case()
    if prisma_case_data:
        print(json.dumps(prisma_case_data, indent=4))