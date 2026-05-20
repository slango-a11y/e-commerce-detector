"""
bulk_scan_10days.py
───────────────────
Jednorazowe pobranie i skan domen z ostatnich 10 dni.
Pobiera z cenk/nrd (nrd-last-10-days.txt), filtruje .pl/.com.pl,
skanuje i zapisuje wyniki do zakładki "Sklepy - od razu" w Sheets.
Pomija domeny które już są w Kolejce (deduplikacja).
"""

import os, re, datetime, time, json, asyncio
import requests, gspread, aiohttp
from google.oauth2.service_account import Credentials
from collections import Counter

TARGET_TLD  = ["pl", "com.pl"]
CONCURRENT  = 50
TIMEOUT     = 10
MAX_BYTES   = 150_000
BATCH_SIZE  = 200
BATCH_PAUSE = 3

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

SHEET_QUEUE = "Kolejka"
SHEET_NOW   = "Sklepy - od razu"
SHEET_LATER = "Sklepy - po 14 dniach"
HEADER_SHOPS = ["domena", "title", "platforma", "url", "data_rejestracji", "data_skanu"]

PLATFORMS = {
    "WooCommerce": [
        "wp-content/plugins/woocommerce", "woocommerce", "wc-ajax",
    ],
    "Shopify": [
        "cdn.shopify.com", "Shopify.theme", "shopify-section", "/cdn/shop/",
    ],
    "PrestaShop": [
        "var prestashop =", "/modules/ps_", "id_product_attribute",
    ],
    "Magento": [
        "var BLANK_URL", "Mage.Cookies", "mage/cookies", "Magento_Ui",
    ],
    "IdoSell": [
        "iai-shop.com", "idosell.com", "iaisystem", "cdn.idosell.com",
    ],
    "Shoper": [
        "shoper.pl", "sklep-shoper.pl", "cdn.shoper.pl", "shoperstatic.com",
    ],
    "SkyShop": [
        "sky-shop.pl",
        "Sklep internetowy na oprogramowaniu Sky-Shop",
        "skyshopapp.com",
        "/sklep/userdata/",
        "skyshop",
    ],
    "AtomStore": [
        "atomstore.pl", "atomstore", "powered by: atomstore",
        "powered by atomstore", "utm_source=client_shop",
    ],
    "SOTE": [
        "sote.pl", "sote-shop", "soteshop",
    ],
    "ShopGold": [
        "shopgold.pl", "shopgold-",
    ],
    "osCommerce": [
        "oscommerce", "catalog/includes/",
    ],
    "Comarch e-Sklep": [
        "e-sklep.pl", "comarch.com/e-sklep", "comarchesklep",
    ],
    "RedCart": [
        "redcart.pl", "rc-cdn.redcart", "redcart_",
    ],
    "OpenCart": [
        "catalog/view/theme", "route=common/home", "index.php?route=",
        "opencart",
    ],
    "BaseLinker": [
        "baselinker.com",
    ],
    "Selly": [
        "selly.pl", "selly-cdn", "selly_",
    ],
    "Selesto": [
        "selesto.pl", "cdn.selesto.pl", "selesto-",
    ],
    "TakeDrop": [
        "takedrop.pl", "takebuilder", "taketrust", "takedrop",
    ],
    "2ClickShop": [
        "2clickshop", "2click.pl",
    ],
    "Wix eCommerce": [
        "wixstatic.com", "wix.com",
    ],
    "Własny sklep": [
        "dodaj do koszyka", "add to cart", "kup teraz", "koszyk",
        "id=\"cart\"", "class=\"cart\"", "class=\"basket\"",
        "/cart", "checkout",
    ],
}

# ── SHEETS ────────────────────────────────────────────────────

def get_sheets_client():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_CREDENTIALS"]), scopes=SCOPES)
    return gspread.authorize(creds)

def ensure_sheet(spreadsheet, name, header):
    try:
        ws = spreadsheet.worksheet(name)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=name, rows=500000, cols=len(header))
        ws.append_row(header)
        print(f"  ✅ Utworzono zakładkę '{name}'")
    return ws

def get_existing_domains(ws, col=2) -> set:
    try:
        vals = ws.col_values(col)
        return set(v.strip().lower() for v in vals[1:] if v.strip())
    except Exception:
        return set()

def batch_append(ws, rows, label=""):
    saved = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        try:
            ws.append_rows(batch, value_input_option="RAW")
            saved += len(batch)
            print(f"  📥 {label} {saved}/{len(rows)}...")
        except gspread.exceptions.APIError as e:
            if "429" in str(e):
                print(f"  ⏳ Limit API — czekam 60s...")
                time.sleep(60)
                try:
                    ws.append_rows(batch, value_input_option="RAW")
                    saved += len(batch)
                except Exception as e2:
                    print(f"  ❌ {e2}")
            else:
                print(f"  ❌ {e}")
        if i + BATCH_SIZE < len(rows):
            time.sleep(BATCH_PAUSE)
    return saved

# ── POBIERANIE DOMEN ──────────────────────────────────────────

def fetch_domains() -> list[str]:
    url = "https://raw.githubusercontent.com/cenk/nrd/main/nrd-last-10-days.txt"
    print(f"📥 Pobieram: nrd-last-10-days.txt")
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
        if r.status_code == 200:
            all_domains = [l.strip().lower() for l in r.text.splitlines() if l.strip()]
            filtered = []
            for d in all_domains:
                for tld in TARGET_TLD:
                    if d.endswith(f".{tld}"):
                        filtered.append(d)
                        break
            print(f"   Wszystkich: {len(all_domains)} | .pl/.com.pl: {len(filtered)}")
            return list(set(filtered))
    except Exception as e:
        print(f"  ❌ {e}")
    return []

# ── SKANOWANIE ────────────────────────────────────────────────

def extract_title(html: str) -> str:
    m = re.search(r"<title[^>]*>([^<]{1,200})</title>", html, re.IGNORECASE)
    if m:
        return re.sub(r"\s+", " ", m.group(1).strip())[:150]
    return ""

def detect_platform(html: str):
    hl = html.lower()
    for platform, sigs in PLATFORMS.items():
        for sig in sigs:
            if sig.lower() in hl:
                return platform
    return None

async def fetch_page(session, url):
    try:
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=TIMEOUT),
            allow_redirects=True, max_redirects=5, ssl=False,
        ) as resp:
            if 200 <= resp.status < 300:
                content = await resp.content.read(MAX_BYTES)
                return content.decode("utf-8", errors="ignore"), str(resp.url)
    except Exception:
        pass
    return None, None

async def scan_domain(session, domain, semaphore):
    async with semaphore:
        for scheme in ("https", "http"):
            html, final_url = await fetch_page(session, f"{scheme}://{domain}")
            if html:
                platform = detect_platform(html)
                if platform:
                    return {
                        "domain":   domain,
                        "title":    extract_title(html),
                        "platform": platform,
                        "url":      final_url,
                    }
                return None
    return None

async def scan_all(domains):
    results = []
    semaphore = asyncio.Semaphore(CONCURRENT)
    connector = aiohttp.TCPConnector(limit=CONCURRENT, ssl=False)
    headers   = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    done = shops = 0

    print(f"\n🔍 Skanuję {len(domains)} domen...")
    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        tasks = [scan_domain(session, d, semaphore) for d in domains]
        for coro in asyncio.as_completed(tasks):
            r = await coro
            done += 1
            if r:
                results.append(r)
                shops += 1
                print(f"  🛒 {r['domain']} → {r['platform']} | {r['title'][:50]}")
            if done % 500 == 0:
                print(f"  ... {done}/{len(domains)} | sklepy: {shops}")
    return results

# ── GŁÓWNA FUNKCJA ────────────────────────────────────────────

async def main():
    today = datetime.date.today().isoformat()
    print("=" * 55)
    print("  JEDNORAZOWY SKAN — ostatnie 10 dni")
    print(f"  Data: {today}")
    print("=" * 55)

    # 1. Pobierz domeny
    domains = fetch_domains()
    if not domains:
        print("❌ Brak domen.")
        return

    # 2. Połącz z Sheets
    print("\n🔗 Łączę z Google Sheets...")
    gc = get_sheets_client()
    sh = gc.open_by_key(os.environ["SPREADSHEET_ID"])
    ws_now   = ensure_sheet(sh, SHEET_NOW,   HEADER_SHOPS)
    ws_later = ensure_sheet(sh, SHEET_LATER, HEADER_SHOPS)

    # 3. Pomiń domeny które już są w "Sklepy - od razu"
    existing = get_existing_domains(ws_now, col=1)
    new_domains = [d for d in domains if d not in existing]
    print(f"   Już zeskanowanych: {len(existing)} | Nowych: {len(new_domains)}")

    if not new_domains:
        print("✅ Wszystkie domeny już zeskanowane.")
        return

    # 4. Skanuj
    results = await scan_all(new_domains)

    # 5. Zapisz do "Sklepy - od razu"
    if results:
        print(f"\n💾 Zapisuję {len(results)} sklepów...")
        batch_append(ws_now,
            [[r["domain"], r["title"], r["platform"], r["url"], today, today]
             for r in results],
            "[Sklepy-od-razu]")

    # Podsumowanie
    print(f"\n{'='*55}")
    print(f"  GOTOWE!")
    print(f"  Przeskanowano: {len(new_domains)} domen")
    print(f"  Znaleziono sklepów: {len(results)}")
    if results:
        print(f"\n  Platformy:")
        for p, n in Counter(r["platform"] for r in results).most_common():
            print(f"    {p}: {n}")
    print(f"{'='*55}")

if __name__ == "__main__":
    asyncio.run(main())
