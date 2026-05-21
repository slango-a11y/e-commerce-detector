"""
scan_from_queue.py v2
─────────────────────
Codziennie pobiera z "Kolejka" domeny sprzed 28-35 dni,
skanuje je ponownie i wyniki wrzuca do "Sklepy - po 30 dniach".
"""
 
import os, re, json, asyncio, datetime, time
import gspread, aiohttp
from google.oauth2.service_account import Credentials
 
SCAN_WINDOW_MIN = 13
SCAN_WINDOW_MAX = 16
CONCURRENT  = 50
TIMEOUT     = 10
MAX_BYTES   = 150_000
BATCH_SIZE  = 200
BATCH_PAUSE = 3
 
SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
 
SHEET_QUEUE  = "Kolejka"
SHEET_LATER  = "Sklepy - po 30 dniach"
HEADER_SHOPS = ["domena", "title", "platforma", "url", "data_rejestracji", "data_skanu"]
 
PLATFORMS = {
    "WooCommerce": [
        "wp-content/plugins/woocommerce",
        "woocommerce-cart",
        "woocommerce-checkout",
        "add-to-cart",
        "wc-ajax=get_refreshed_fragments",
        "woocommerce-product",
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
 
def get_domains_to_scan(ws) -> list[dict]:
    today = datetime.date.today()
    scan_dates = set()
    for d in range(SCAN_WINDOW_MIN, SCAN_WINDOW_MAX + 1):
        scan_dates.add((today - datetime.timedelta(days=d)).isoformat())
 
    print(f"  Szukam domen z dat: {sorted(scan_dates)}")
    all_rows = ws.get_all_records()
    to_scan = []
    for i, row in enumerate(all_rows, start=2):
        reg_date = str(row.get("data_rejestracji", "")).strip()
        domain   = str(row.get("domena", "")).strip().lower()
        scanned  = str(row.get("zeskanowano", "")).strip().upper()
        if reg_date in scan_dates and scanned == "NIE" and domain:
            to_scan.append({"row": i, "domain": domain, "reg_date": reg_date})
    return to_scan
 
def mark_as_scanned(ws, row_numbers: list[int]):
    today = datetime.date.today().isoformat()
    updates = [{"range": f"G{r}:G{r}", "values": [["TAK"]]} for r in row_numbers]
    # kolumna G = zeskanowano (7), H = data_skanu (8) — nowy układ z v4
    updates += [{"range": f"H{r}:H{r}", "values": [[today]]} for r in row_numbers]
    if updates:
        try:
            ws.batch_update(updates)
        except Exception as e:
            print(f"  ⚠️  Błąd oznaczania: {e}")
 
def save_results(spreadsheet, results: list[dict]):
    try:
        ws = spreadsheet.worksheet(SHEET_LATER)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=SHEET_LATER, rows=500000, cols=6)
        ws.append_row(HEADER_SHOPS)
 
    today = datetime.date.today().isoformat()
    rows = [[r["domain"], r["title"], r["platform"], r["url"], r["reg_date"], today]
            for r in results]
 
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        try:
            ws.append_rows(batch, value_input_option="RAW")
        except gspread.exceptions.APIError as e:
            if "429" in str(e):
                time.sleep(60)
                ws.append_rows(batch, value_input_option="RAW")
        if i + BATCH_SIZE < len(rows):
            time.sleep(BATCH_PAUSE)
 
    print(f"\n✅ Wyniki zapisane do '{SHEET_LATER}'")
 
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
                return await resp.content.read(MAX_BYTES)
    except Exception:
        pass
    return None
 
async def check_domain(session, item, semaphore):
    async with semaphore:
        for scheme in ("https", "http"):
            raw = await fetch_page(session, f"{scheme}://{item['domain']}")
            if raw:
                html     = raw.decode("utf-8", errors="ignore")
                platform = detect_platform(html)
                if platform:
                    return {
                        "domain":   item["domain"],
                        "title":    extract_title(html),
                        "platform": platform,
                        "url":      f"{scheme}://{item['domain']}",
                        "reg_date": item["reg_date"],
                        "row":      item["row"],
                    }
                return None
    return None
 
async def scan_domains(items):
    results = []
    semaphore = asyncio.Semaphore(CONCURRENT)
    connector = aiohttp.TCPConnector(limit=CONCURRENT, ssl=False)
    headers   = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    done = shops = 0
 
    print(f"\n🔍 Skanuję {len(items)} domen (runda 2 — po 30 dniach)...")
    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        tasks = [check_domain(session, item, semaphore) for item in items]
        for coro in asyncio.as_completed(tasks):
            result = await coro
            done += 1
            if result:
                results.append(result)
                shops += 1
                print(f"  🛒 [{result['reg_date']}] {result['domain']} → {result['platform']}")
            if done % 100 == 0:
                print(f"  ... {done}/{len(items)} | sklepy: {shops}")
    return results
 
# ── GŁÓWNA FUNKCJA ────────────────────────────────────────────
 
async def main():
    today = datetime.date.today().isoformat()
    print("=" * 55)
    print("  SKAN PO 30 DNIACH")
    print(f"  Dziś: {today} | okno: {SCAN_WINDOW_MIN}-{SCAN_WINDOW_MAX} dni temu")
    print("=" * 55)
 
    print("\n🔗 Łączę z Google Sheets...")
    gc = get_sheets_client()
    sh = gc.open_by_key(os.environ["SPREADSHEET_ID"])
 
    try:
        queue_ws = sh.worksheet(SHEET_QUEUE)
    except gspread.WorksheetNotFound:
        print(f"❌ Brak zakładki '{SHEET_QUEUE}'.")
        return
 
    print(f"\n📋 Pobieram domeny z '{SHEET_QUEUE}'...")
    items = get_domains_to_scan(queue_ws)
    print(f"   Do skanowania: {len(items)} domen")
 
    if not items:
        print("\n⚠️  Brak domen w oknie 28-35 dni. Za wcześnie lub kolejka pusta.")
        return
 
    results = await scan_domains(items)
 
    # Oznacz jako zeskanowane
    all_rows = [item["row"] for item in items]
    print(f"\n✏️  Oznaczam {len(all_rows)} domen jako zeskanowane...")
    mark_as_scanned(queue_ws, all_rows)
 
    if results:
        save_results(sh, results)
        print(f"\n🎉 Znaleziono {len(results)} sklepów po 30 dniach!")
    else:
        print("\n📭 Brak nowych sklepów w tej partii.")
 
    from collections import Counter
    if results:
        print("\n📊 Platformy:")
        for p, n in Counter(r["platform"] for r in results).most_common():
            print(f"   {p}: {n}")
 
if __name__ == "__main__":
    asyncio.run(main())
