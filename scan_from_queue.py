"""
scan_from_queue.py
──────────────────
Codziennie pobiera z zakładki "Kolejka" domeny zarejestrowane
dokładnie 30 dni temu (lub między 28-35 dni — żeby nic nie wpaść
w weekend), skanuje je pod kątem e-commerce i języka polskiego,
a wyniki zapisuje do zakładki z datą skanu np. "Wyniki 2026-05-19".

Struktura zakładki "Kolejka":
  A: data_rejestracji  B: domena  C: zeskanowano  D: data_skanu

Struktura zakładki "Wyniki RRRR-MM-DD":
  domena | platforma | jezyk | data_rejestracji | url | data_skanu
"""

import os
import re
import json
import asyncio
import datetime
import gspread
from google.oauth2.service_account import Credentials
import aiohttp

try:
    from langdetect import detect as langdetect_detect
    LANGDETECT = True
except ImportError:
    LANGDETECT = False
    print("⚠️  langdetect niedostępny")

# ── KONFIGURACJA ──────────────────────────────────────────────

# Skanuj domeny z tego okna czasowego (dni wstecz)
SCAN_WINDOW_MIN = 28   # minimum 28 dni temu
SCAN_WINDOW_MAX = 35   # maximum 35 dni temu (łapie weekendy i braki)

CONCURRENT  = 50
TIMEOUT     = 10
MAX_BYTES   = 150_000
TARGET_LANG = "pl"

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

QUEUE_SHEET   = "Kolejka"
RESULTS_PREFIX = "Wyniki"

# Platformy e-commerce
PLATFORMS = {
    "WooCommerce":      ["wp-content/plugins/woocommerce", "woocommerce"],
    "Shopify":          ["cdn.shopify.com", "Shopify.theme"],
    "PrestaShop":       ["var prestashop =", "/modules/ps_", "prestashop"],
    "Magento":          ["var BLANK_URL", "Mage.Cookies", "mage/cookies"],
    "IdoSell":          ["iai-shop.com", "idosell.com"],
    "Shoper":           ["shoper.pl", "sklep-shoper.pl"],
    "SkyShop":          ["skyshop.pl", "skyshopapp.com"],
    "SOTE":             ["sote.pl", "sote-shop"],
    "ShopGold":         ["shopgold.pl"],
    "osCommerce":       ["oscommerce", "catalog/includes/"],
    "Comarch e-Sklep":  ["e-sklep.pl", "comarch.com/e-sklep"],
    "RedCart":          ["redcart.pl", "rc-cdn.redcart"],
    "OpenCart":         ["catalog/view/theme", "route=common/home"],
    "BaseLinker":       ["baselinker.com"],
    "Selly":            ["selly.pl", "selly-cdn"],
}

# ── GOOGLE SHEETS ─────────────────────────────────────────────

def get_sheets_client():
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)

def get_domains_to_scan(ws) -> list[dict]:
    """
    Pobiera z kolejki domeny które:
    - zostały zarejestrowane 28-35 dni temu
    - nie były jeszcze skanowane (kolumna C = "NIE")
    """
    today = datetime.date.today()
    scan_dates = set()
    for days_ago in range(SCAN_WINDOW_MIN, SCAN_WINDOW_MAX + 1):
        d = today - datetime.timedelta(days=days_ago)
        scan_dates.add(d.isoformat())

    print(f"  Szukam domen z dat: {sorted(scan_dates)}")

    all_rows = ws.get_all_records()
    to_scan = []
    for i, row in enumerate(all_rows, start=2):  # start=2 bo wiersz 1 = nagłówek
        reg_date = str(row.get("data_rejestracji", "")).strip()
        domain   = str(row.get("domena", "")).strip().lower()
        scanned  = str(row.get("zeskanowano", "")).strip().upper()

        if reg_date in scan_dates and scanned == "NIE" and domain:
            to_scan.append({
                "row":      i,
                "domain":   domain,
                "reg_date": reg_date,
            })

    return to_scan

def mark_as_scanned(ws, row_numbers: list[int]):
    """Oznacza domeny jako zeskanowane w kolejce (batch update)."""
    today = datetime.date.today().isoformat()
    updates = []
    for row in row_numbers:
        updates.append({
            "range": f"C{row}:D{row}",
            "values": [["TAK", today]],
        })
    if updates:
        ws.batch_update(updates)

def save_results(spreadsheet, results: list[dict]):
    """Zapisuje wyniki do zakładki 'Wyniki RRRR-MM-DD'."""
    if not results:
        return

    sheet_name = f"{RESULTS_PREFIX} {datetime.date.today().isoformat()}"

    try:
        ws = spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=6)

    header = ["domena", "platforma", "jezyk", "data_rejestracji", "url", "data_skanu"]
    today  = datetime.date.today().isoformat()

    # Sprawdź czy nagłówek już jest
    existing = ws.get_all_values()
    if not existing or existing[0] != header:
        ws.clear()
        ws.append_row(header)

    rows = [
        [
            r["domain"],
            r["platform"],
            r["language"],
            r["reg_date"],
            r["url"],
            today,
        ]
        for r in results
    ]

    # Batch po 500
    for i in range(0, len(rows), 500):
        ws.append_rows(rows[i:i+500], value_input_option="RAW")

    print(f"\n✅ Wyniki zapisane do zakładki: '{sheet_name}'")

# ── SKANOWANIE ────────────────────────────────────────────────

async def fetch_page(session: aiohttp.ClientSession, url: str) -> str | None:
    try:
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=TIMEOUT),
            allow_redirects=True,
            max_redirects=5,
            ssl=False,
        ) as resp:
            if resp.status == 200:
                content = await resp.content.read(MAX_BYTES)
                return content.decode("utf-8", errors="ignore")
    except Exception:
        pass
    return None

def detect_platform(html: str) -> str | None:
    html_lower = html.lower()
    for platform, signatures in PLATFORMS.items():
        for sig in signatures:
            if sig.lower() in html_lower:
                return platform
    return None

def detect_language(html: str) -> str:
    if not LANGDETECT:
        return "?"
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) < 50:
        return "?"
    try:
        return langdetect_detect(text[:3000])
    except Exception:
        return "?"

async def check_domain(
    session: aiohttp.ClientSession,
    item: dict,
    semaphore: asyncio.Semaphore,
) -> dict | None:
    async with semaphore:
        for scheme in ("https", "http"):
            url = f"{scheme}://{item['domain']}"
            html = await fetch_page(session, url)
            if html:
                platform = detect_platform(html)
                if platform:
                    lang = detect_language(html)
                    if lang in (TARGET_LANG, "?"):
                        return {
                            "domain":   item["domain"],
                            "platform": platform,
                            "language": lang,
                            "reg_date": item["reg_date"],
                            "url":      url,
                            "row":      item["row"],
                        }
                return None  # strona działa, ale nie ma sklepu
    return None

async def scan_domains(items: list[dict]) -> list[dict]:
    results = []
    semaphore = asyncio.Semaphore(CONCURRENT)
    connector = aiohttp.TCPConnector(limit=CONCURRENT, ssl=False)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    print(f"\n🔍 Skanuję {len(items)} domen...")

    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        tasks = [check_domain(session, item, semaphore) for item in items]
        done = 0
        shops = 0
        for coro in asyncio.as_completed(tasks):
            result = await coro
            done += 1
            if result:
                results.append(result)
                shops += 1
                print(
                    f"  🛒 [{result['reg_date']}] {result['domain']} "
                    f"→ {result['platform']} ({result['language']})"
                )
            if done % 100 == 0:
                print(f"  ... {done}/{len(items)} | sklepy: {shops}")

    return results

# ── GŁÓWNA FUNKCJA ────────────────────────────────────────────

async def main():
    today = datetime.date.today().isoformat()

    print("=" * 55)
    print("  SKAN KOLEJKI — domeny sprzed 28-35 dni")
    print(f"  Dziś: {today}")
    print(f"  Skanuje domeny z: {SCAN_WINDOW_MIN}-{SCAN_WINDOW_MAX} dni temu")
    print("=" * 55)

    # 1. Połącz z Sheets
    print("\n🔗 Łączę z Google Sheets...")
    gc = get_sheets_client()
    sh = gc.open_by_key(os.environ["SPREADSHEET_ID"])

    try:
        queue_ws = sh.worksheet(QUEUE_SHEET)
    except gspread.WorksheetNotFound:
        print(f"❌ Brak zakładki '{QUEUE_SHEET}'. Uruchom najpierw collect_domains.py.")
        return

    # 2. Pobierz domeny do skanu
    print(f"\n📋 Pobieram domeny z zakładki '{QUEUE_SHEET}'...")
    items = get_domains_to_scan(queue_ws)
    print(f"   Do skanowania: {len(items)} domen")

    if not items:
        print(
            "\n⚠️  Brak domen do skanowania w tym oknie czasowym.\n"
            "   Powody: za mało danych w kolejce (uruchom collect najpierw)\n"
            f"   lub brak domen z dat {SCAN_WINDOW_MIN}-{SCAN_WINDOW_MAX} dni temu."
        )
        return

    # 3. Skanuj
    results = await scan_domains(items)

    # 4. Oznacz jako zeskanowane (nawet te bez sklepu)
    all_row_numbers = [item["row"] for item in items]
    print(f"\n✏️  Oznaczam {len(all_row_numbers)} domen jako zeskanowane...")
    mark_as_scanned(queue_ws, all_row_numbers)

    # 5. Zapisz wyniki
    if results:
        save_results(sh, results)
        print(f"\n🎉 Znaleziono {len(results)} polskich sklepów!")
    else:
        print("\n📭 Brak sklepów w tej partii domen.")

    # Podsumowanie
    from collections import Counter
    if results:
        print("\n📊 Platformy:")
        for p, n in Counter(r["platform"] for r in results).most_common():
            print(f"   {p}: {n}")

if __name__ == "__main__":
    asyncio.run(main())
