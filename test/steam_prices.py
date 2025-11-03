import requests
import time

BASE_URL = "https://steamcommunity.com/market/search/render/"

def get_all_cs2_items():
    all_items = []
    start = 0
    count = 100

    while True:
        params = {
            "query": "appid:730",
            "start": start,
            "count": count,
            "norender": 1
        }

        response = requests.get(BASE_URL, params=params)
        if response.status_code != 200:
            print(f"Błąd: {response.status_code}")
            break

        data = response.json()
        if not data.get("results"):
            print("Brak dalszych wyników.")
            break

        items = data["results"]
        all_items.extend(items)

        total = data.get("total_count", 0)
        print(f"Pobrano {len(items)} / {total} (start={start})")

        start += count
        if start >= total:
            break

        time.sleep(5)  # 🔹 mały delay, żeby uniknąć bana

    return all_items


if __name__ == "__main__":
    items = get_all_cs2_items()
    print(f"\n✅ Łącznie pobrano {len(items)} przedmiotów.")
    print(items)