import requests
import time
import json

BASE_URL = "https://steamcommunity.com/market/search/render/"

BUCKET_SIZE = 300
MAX_CALLS = 200
bucket_start = time.time()
call_count = 0

def can_make_request():
    global bucket_start, call_count
    now = time.time()
    if now - bucket_start > BUCKET_SIZE:
        # nowy bucket, reset licznika
        bucket_start = now
        call_count = 0
    if call_count < MAX_CALLS:
        call_count += 1
        return True
    return False

def get_all_cs2_items(base_time_sleep=2):
    all_items = []
    start = 0
    count = 1

    while True:
        while not can_make_request():
            print("Osiągnięto limit zapytań, czekam na reset...")
            time.sleep(5)

        params = {
            "query": "appid:730",
            "start": start,
            "count": count,
            "norender": 1
        }

        headers = {"User-Agent": "Mozilla/5.0 (compatible; CS2Fetcher/1.0)"}

        try:
            response = requests.get(BASE_URL, params=params, headers=headers, timeout=10)
        except requests.exceptions.RequestException as e:
            print(f"Błąd połączenia: {e}, czekam 10s...")
            time.sleep(10)
            continue

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 300))
            print(f"Zbyt wiele zapytań. Czekam {retry_after} sekund...")
            # base_time_sleep += 0.1 # Incremental backoff
            # print(f"Zwiększam czas oczekiwania do {base_time_sleep} sekund między zapytaniami.")
            time.sleep(retry_after)
            continue

        if response.status_code != 200:
            print(f"Błąd: {response.status_code}")
            break

        data = response.json()
        if not data.get("results"):
            print("Brak dalszych wyników.")
            break

        items = data["results"]
        all_items.extend(items)

        print(f"Pobrano {len(items)} (start={start})")

        if start % 5 == 0:
            with open("items_cache.json", "w") as f:
                json.dump(all_items, f, indent=4)

        start += count

        items = data.get("results", [])
        if len(items) < count:
            print("Brak dalszych wyników.")
            with open("items_cache.json", "w") as f:
                json.dump(all_items, f, indent=4)
            break

        time.sleep(base_time_sleep)

    return all_items


if __name__ == "__main__":
    items = get_all_cs2_items()
    print(f"\nŁącznie pobrano {len(items)} przedmiotów.")
    print(items)