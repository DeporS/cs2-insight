import requests
import time
import json

BASE_URL = "https://steamcommunity.com/market/priceoverview/?country=PL&currency=1&appid=730&market_hash_name="

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/128.0.0.0 Safari/537.36"
}

def get_case_data(case_name):
    try:
        response = requests.get(f"{BASE_URL}{case_name}", headers=headers, timeout=10)
        response.raise_for_status()  # Raise an error for HTTP errors
        return response.json()
    except requests.Timeout:
        print(f"Request for {case_name} timed out, retrying in 5 seconds...")
        time.sleep(5)  # Wait before retrying
        return get_case_data(case_name)
    except requests.RequestException as e:
        print(f"An error occurred for {case_name}: {e}, retrying in 10 seconds...")
        time.sleep(10)
        return get_case_data(case_name)


def get_all_cases():
    with open("test/cases.json", "r") as f:
        cases = json.load(f)
    
    for case in cases:
        case_name = case["name"]
        
        data = get_case_data(case_name)
        if data:
            print(f"Case: {case_name}, Data: {data}")
            # Process the data as needed

        time.sleep(3)  # Be polite and avoid hitting rate limits

if __name__ == "__main__":
    get_all_cases()