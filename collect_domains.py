"""
collect_domains.py v4
─────────────────────
Codziennie:
1. Pobiera nowe domeny .pl i .com.pl
2. Skanuje je od razu (title + platforma + czy strona działa)
3. Zapisuje WSZYSTKIE do "Kolejka" (z title i strona_dziala)
4. Sklepy wykryte od razu → "Sklepy - od razu"
 
Za 30 dni scan_from_queue.py → "Sklepy - po 30 dniach"
"""
 
import os, re, datetime, time, json, asyncio, socket
import requests, gspread, aiohttp
from google.oauth2.service_account import Credentials
 
# ── KONFIGURACJA ──────────────────────────────────────────────
 
TARGET_TLD  = ["pl", "com.pl"]
CONCURRENT  = 50
TIMEOUT     = 10
MAX_BYTES   = 150_000
BATCH_SIZE  = 200
BATCH_PAUSE = 3
 
SOURCES = [
    "https://raw.githubusercontent.com/whoisextractor/newly-registered-domains/main/nrd-1d.txt",
    "https://raw.githubusercontent.com/cenk/nrd/main/nrd-last-10-days.txt",
]
 
SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
 
SHEET_QUEUE  = "Kolejka"
SHEET_NOW    = "Sklepy - od razu"
SHEET_LATER  = "Sklepy - po 14 dniach"
SHEET_FIRMS  = "Nowe firmy"
 
HEADER_QUEUE = ["data_rejestracji", "domena", "title", "strona_dziala", "url_docelowy", "platforma", "rejestrator", "hosting", "zeskanowano", "data_skanu"]
HEADER_SHOPS = ["domena", "title", "platforma", "url", "rejestrator", "hosting", "data_rejestracji", "data_skanu"]
HEADER_FIRMS = ["domena", "title", "url", "rejestrator", "hosting", "data_rejestracji", "data_skanu", "slowa_kluczowe"]
 
# Wykrywanie firm: KONTAKT + co najmniej jedno z FIRMA_KEYWORDS
CONTACT_KEYWORDS = ["kontakt", "skontaktuj się", "contact"]
FIRMA_KEYWORDS   = [
    "o firmie", "o nas", "firma", "rozwiązania",
    "praca", "referencje", "kariera", "about us", "blog",
]
 
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
        "atomstore.pl",
        "atomstore",
        "powered by: atomstore",
        "powered by atomstore",
        "utm_source=client_shop",
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
        "selesto.pl",
        "cdn.selesto.pl",
        "selesto-",
    ],
    "TakeDrop": [
        "takedrop.pl",
        "takebuilder",
        "taketrust",
        "takedrop",
    ],
    "2ClickShop": [
        "2clickshop",
        "2click.pl",
    ],
    "Wix eCommerce": [
        "wixstatic.com", "wix.com",
    ],
    "Własny sklep": [
        "dodaj do koszyka",
        "add to cart",
        "kup teraz",
        "do koszyka",
    ],
}
 
# ── GOOGLE SHEETS ─────────────────────────────────────────────
 
def get_sheets_client():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_CREDENTIALS"]), scopes=SCOPES)
    return gspread.authorize(creds)
 
def ensure_sheet(spreadsheet, name, header):
    try:
        ws = spreadsheet.worksheet(name)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=name, rows=1000, cols=len(header))
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
                    print(f"  ❌ Pominięto batch: {e2}")
            else:
                print(f"  ❌ Błąd: {e}")
        if i + BATCH_SIZE < len(rows):
            time.sleep(BATCH_PAUSE)
    return saved
 
# ── REJESTRATOR I HOSTING ────────────────────────────────────
 
def get_registrar(domain: str) -> str:
    """Pobiera rejestratora domeny przez RDAP DNS.pl (dla .pl) lub rdap.org (inne)."""
    try:
        # Dla domen .pl używamy RDAP NASK
        if domain.endswith(".pl"):
            r = requests.get(
                f"https://rdap.dns.pl/domain/{domain}",
                timeout=8, headers={"User-Agent": "Mozilla/5.0"}
            )
            if r.status_code == 200:
                data = r.json()
                # Szukaj rejestratora w entities
                for entity in data.get("entities", []):
                    roles = entity.get("roles", [])
                    if "registrar" in roles:
                        # Nazwa rejestratora
                        vcardArray = entity.get("vcardArray", [])
                        if vcardArray and len(vcardArray) > 1:
                            for vcard in vcardArray[1]:
                                if vcard[0] == "fn":
                                    return str(vcard[3])[:80]
                        # Fallback: handle
                        return entity.get("handle", "")[:80]
        else:
            # Dla innych TLD używamy rdap.org
            r = requests.get(
                f"https://rdap.org/domain/{domain}",
                timeout=8, headers={"User-Agent": "Mozilla/5.0"}
            )
            if r.status_code == 200:
                data = r.json()
                for entity in data.get("entities", []):
                    if "registrar" in entity.get("roles", []):
                        vcardArray = entity.get("vcardArray", [])
                        if vcardArray and len(vcardArray) > 1:
                            for vcard in vcardArray[1]:
                                if vcard[0] == "fn":
                                    return str(vcard[3])[:80]
    except Exception:
        pass
    return ""
 
def get_hosting(domain: str) -> str:
    """Pobiera nazwę hostingu przez IP → ipinfo.io."""
    try:
        ip = socket.gethostbyname(domain)
        r = requests.get(
            f"https://ipinfo.io/{ip}/org",
            timeout=6, headers={"User-Agent": "Mozilla/5.0"}
        )
        if r.status_code == 200:
            org = r.text.strip()
            # Format: "AS12345 OVH SAS" — usuwamy numer AS
            org = re.sub(r"^AS\d+\s*", "", org)
            return org[:80]
    except Exception:
        pass
    return ""
 
def get_domain_info(domain: str) -> tuple[str, str]:
    """Zwraca (rejestrator, hosting) dla domeny."""
    registrar = get_registrar(domain)
    hosting   = get_hosting(domain)
    return registrar, hosting
 
 
 
def fetch_raw(url):
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
        if r.status_code == 200:
            return [l.strip().lower() for l in r.text.splitlines() if l.strip()]
    except Exception as e:
        print(f"  ⚠️  {e}")
    return []
 
def filter_tld(domains):
    result = []
    for d in domains:
        for tld in TARGET_TLD:
            if d.endswith(f".{tld}"):
                result.append(d)
                break
    return result
 
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
    return "brak danych"
 
def detect_firm(html: str) -> str | None:
    """
    Zwraca znalezione słowa kluczowe jeśli strona wygląda jak firma,
    None jeśli nie spełnia kryteriów.
    Kryteria: ma KONTAKT + co najmniej jedno z FIRMA_KEYWORDS.
    """
    hl = html.lower()
    has_contact = any(kw in hl for kw in CONTACT_KEYWORDS)
    if not has_contact:
        return None
    found = [kw for kw in FIRMA_KEYWORDS if kw in hl]
    if found:
        return ", ".join(found[:5])  # max 5 słów kluczowych
    return None
 
# Domeny zaparkowane / na sprzedaż — traktujemy jako "nie działa"
PARKED_URLS = [
    "sklep.premium.pl", "aftermarket.pl", "domeny.pl",
    "sedo.com", "dan.com", "parkingcrew.net", "bodis.com",
    "ddregistrar.pl", "nazwa.pl/sklep",
]
PARKED_TITLES = [
    "oferta domeny", "domena na sprzedaż", "domain for sale",
    "ta domena jest na sprzedaż", "buy this domain",
    "domena jest dostępna", "jest dostępna na sprzedaż",
    "oferta sprzedaży domeny", "parked domain",
    "premium.pl - międzynarodowa giełda",
    "strona w budowie", "coming soon",
]
 
# Słowa które dyskwalifikują stronę jako sklep/firmę
EXCLUDED_TITLES = [
    "casino", "kasyno", "zakłady sportowe", "bukmacher",
    "darmowe spiny", "bonus bez depozytu", "sloty",
    "poker online", "ruletka online",
]
 
def is_parked(final_url: str, title: str) -> bool:
    url_lower   = final_url.lower()
    title_lower = title.lower()
    if any(p in url_lower   for p in PARKED_URLS):   return True
    if any(p in title_lower for p in PARKED_TITLES): return True
    return False
 
def is_gambling(title: str) -> bool:
    """Zwraca True jeśli strona to kasyno/bukmacher."""
    tl = title.lower()
    return any(kw in tl for kw in EXCLUDED_TITLES)
 
 
async def fetch_page(session, url):
    try:
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=TIMEOUT),
            allow_redirects=True,
            max_redirects=5,
            ssl=False,
        ) as resp:
            if 200 <= resp.status < 300:
                final_url = str(resp.url)
                return await resp.content.read(MAX_BYTES), final_url
    except Exception:
        pass
    return None, None
 
async def scan_domain(session, domain, semaphore):
    async with semaphore:
        for scheme in ("https", "http"):
            raw, final_url = await fetch_page(session, f"{scheme}://{domain}")
            if raw:
                html  = raw.decode("utf-8", errors="ignore")
                title = extract_title(html)
 
                # Sprawdź czy domena zaparkowana / na sprzedaż
                if is_parked(final_url, title):
                    return {
                        "domain":    domain,
                        "title":     title,
                        "platform":  "brak danych",
                        "url":       "",
                        "dziala":    "NIE",
                        "firm_kw":   None,
                        "registrar": "",
                        "hosting":   "",
                    }
 
                # Wyklucz kasyna i bukmacherów
                if is_gambling(title):
                    return {
                        "domain":    domain,
                        "title":     title,
                        "platform":  "brak danych",
                        "url":       "",
                        "dziala":    "NIE",
                        "firm_kw":   None,
                        "registrar": "",
                        "hosting":   "",
                    }
 
                platform = detect_platform(html)
                firm_kw  = detect_firm(html) if platform == "brak danych" else None
                registrar, hosting = get_domain_info(domain)
                return {
                    "domain":     domain,
                    "title":      title,
                    "platform":   platform,
                    "url":        final_url or f"{scheme}://{domain}",
                    "dziala":     "TAK",
                    "firm_kw":    firm_kw,
                    "registrar":  registrar,
                    "hosting":    hosting,
                }
    return {
        "domain":     domain,
        "title":      "",
        "platform":   "brak danych",
        "url":        "",
        "dziala":     "NIE",
        "firm_kw":    None,
        "registrar":  "",
        "hosting":    "",
    }
 
async def scan_all(domains):
    semaphore = asyncio.Semaphore(CONCURRENT)
    connector = aiohttp.TCPConnector(limit=CONCURRENT, ssl=False)
    headers   = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    results   = []
    shops = done = 0
 
    print(f"\n🔍 Skanuję {len(domains)} domen...")
    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        tasks = [scan_domain(session, d, semaphore) for d in domains]
        for coro in asyncio.as_completed(tasks):
            r = await coro
            results.append(r)
            done += 1
            if r["platform"] != "brak danych":
                shops += 1
                print(f"  🛒 {r['domain']} → {r['platform']} | {r['title'][:50]}")
            elif r["firm_kw"]:
                print(f"  🏢 {r['domain']} → firma | {r['title'][:40]} | [{r['firm_kw']}]")
            if done % 200 == 0:
                firms = sum(1 for x in results if x.get("firm_kw"))
                print(f"  ... {done}/{len(domains)} | działa: {sum(1 for x in results if x['dziala']=='TAK')} | sklepy: {shops} | firmy: {firms}")
 
    dziala = sum(1 for r in results if r["dziala"] == "TAK")
    print(f"\n✅ Skan gotowy — działa: {dziala}/{len(domains)} | sklepy od razu: {shops}")
    return results
 
# ── GŁÓWNA FUNKCJA ────────────────────────────────────────────
 
async def main():
    today = datetime.date.today().isoformat()
    print("=" * 55)
    print("  ZBIERANIE + SKAN NATYCHMIASTOWY v4")
    print(f"  Data: {today} | TLD: {', '.join(TARGET_TLD)}")
    print("=" * 55)
 
    # 1. Pobierz domeny
    all_raw = []
    for url in SOURCES:
        print(f"\n📥 {url.split('/')[-1]}")
        doms = fetch_raw(url)
        filt = filter_tld(doms)
        print(f"   Wszystkich: {len(doms)} | .pl/.com.pl: {len(filt)}")
        all_raw.extend(filt)
    all_raw = list(set(all_raw))
    print(f"\n📊 Unikalnych: {len(all_raw)}")
    if not all_raw:
        print("❌ Brak domen.")
        return
 
    # 2. Sheets
    print("\n🔗 Łączę z Google Sheets...")
    gc = get_sheets_client()
    sh = gc.open_by_key(os.environ["SPREADSHEET_ID"])
    ws_queue = ensure_sheet(sh, SHEET_QUEUE,  HEADER_QUEUE)
    ws_now   = ensure_sheet(sh, SHEET_NOW,    HEADER_SHOPS)
    ws_later = ensure_sheet(sh, SHEET_LATER,  HEADER_SHOPS)
    ws_firms = ensure_sheet(sh, SHEET_FIRMS,  HEADER_FIRMS)
 
    # 3. Duplikaty
    existing    = get_existing_domains(ws_queue, col=2)
    new_domains = [d for d in all_raw if d not in existing]
    print(f"   Już w kolejce: {len(existing)} | Nowych: {len(new_domains)}")
    if not new_domains:
        print("✅ Brak nowych domen.")
        return
 
    # 4. Skanuj natychmiast
    results = await scan_all(new_domains)
 
    # 5. Zapisz kolejkę (wszystkie + title + dziala + platforma + rejestrator + hosting)
    print(f"\n💾 Zapisuję do '{SHEET_QUEUE}'...")
    batch_append(ws_queue,
        [[today, r["domain"], r["title"], r["dziala"], r["url"],
          r["platform"], r["registrar"], r["hosting"], "NIE", ""]
         for r in results],
        "[Kolejka]")
 
    # 6. Zapisz sklepy wykryte od razu
    shops_now = [r for r in results if r["platform"] != "brak danych"]
    if shops_now:
        print(f"\n🛒 Zapisuję {len(shops_now)} sklepów do '{SHEET_NOW}'...")
        batch_append(ws_now,
            [[r["domain"], r["title"], r["platform"], r["url"],
              r["registrar"], r["hosting"], today, today]
             for r in shops_now],
            "[Sklepy-od-razu]")
 
    # 7. Zapisz nowe firmy
    firms_now = [r for r in results if r.get("firm_kw") and r["platform"] == "brak danych"]
    if firms_now:
        print(f"\n🏢 Zapisuję {len(firms_now)} firm do '{SHEET_FIRMS}'...")
        batch_append(ws_firms,
            [[r["domain"], r["title"], r["url"],
              r["registrar"], r["hosting"], today, today, r["firm_kw"]]
             for r in firms_now],
            "[Nowe-firmy]")
 
    # Podsumowanie
    from collections import Counter
    dziala = sum(1 for r in results if r["dziala"] == "TAK")
    firms_now = [r for r in results if r.get("firm_kw") and r["platform"] == "brak danych"]
    print(f"\n{'='*55}")
    print(f"  GOTOWE!")
    print(f"  Nowych domen:            {len(new_domains)}")
    print(f"  Strony które działają:   {dziala}")
    print(f"  Sklepy wykryte od razu:  {len(shops_now)}")
    print(f"  Nowe firmy:              {len(firms_now)}")
    if shops_now:
        print(f"\n  Platformy:")
        for p, n in Counter(r["platform"] for r in shops_now).most_common():
            print(f"    {p}: {n}")
    print(f"  Za 14 dni: druga runda w '{SHEET_LATER}'")
    print(f"{'='*55}")
 
if __name__ == "__main__":
    asyncio.run(main())
