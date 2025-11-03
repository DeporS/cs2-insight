import asyncio
import aiohttp
import urllib.parse

STEAM_MARKET_URL = "https://steamcommunity.com/market/priceoverview/"

semaphore = asyncio.Semaphore(1)

async def fetch_skin_data(session, skin_name):
    """Pobiera dane o pojedynczym skinie z rynku Steam."""
    params = {
        "country": "PL",
        "currency": 6,  # PLN
        "appid": 730,   # CS2
        "market_hash_name": skin_name
    }
    url = f"{STEAM_MARKET_URL}?{urllib.parse.urlencode(params)}"
    try:
        async with session.get(url) as resp:
            if resp.status == 200:
                data = await resp.json()
                print(f"[✔] {skin_name}: {data}")
                return {skin_name: data}
            elif resp.status == 429:
                print(f"[⏳] Za dużo zapytań — czekam i ponawiam: {skin_name}")
                await asyncio.sleep(3)
                return await fetch_skin_data(session, skin_name)
            else:
                print(f"[✖] {skin_name}: HTTP {resp.status}")
    except Exception as e:
        print(f"[⚠️] Błąd przy {skin_name}: {e}")
    return {skin_name: None}

async def main():
    # 🔹 tutaj podajesz jakie skiny chcesz pobrać
    skins = [
        "AK-47 | Redline",
        "AWP | Dragon Lore",
        "Desert Eagle | Blaze",
        "Butterfly Knife | Fade",
        "Flip Knife | Doppler",
        "M4A1-S | Icarus Fell",
        "Glock-18 | Water Elemental"
    ]

    conditions = [
        "(Factory New)",
        "(Minimal Wear)",
        "(Field-Tested)",
        "(Well-Worn)",
        "(Battle-Scarred)"
    ]

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_skin_data(session, f"{name} {condition}") for name in skins for condition in conditions]
        results = await asyncio.gather(*tasks)
    
    print("\n=== WYNIKI ===")
    for r in results:
        print(r)

if __name__ == "__main__":
    asyncio.run(main())
