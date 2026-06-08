"""
collect_certstream.py v1
─────────────────────────────────────────────────────────────────
Słucha CertStream (live CT logs) i wyłapuje nowe domeny e-commerce.
Działa przez MAX_RUNTIME sekund (domyślnie 3600 = 1h), po czym
zapisuje kandydatów do zakładki "Kolejka" w Google Sheets.

Uruchamiaj jako osobny GitHub Action: certstream.yml
Najlepiej 2x dziennie (rano + wieczór) po ~60 min każde.

Filtrowanie:
  1. TLD: .pl, .com.pl, .net.pl, .org.pl, .eu
  2. Słowa kluczowe w nazwie domeny sugerujące sklep
  3. Sygnatury platform w CNAME (Shoper, SkyShop) — natychmiastowy +
  4. Deduplikacja względem istniejącej Kolejki

Wymagania pip: certstream tldextract gspread google-auth
"""

import os, re, json, time, datetime, socket, threading
import gspread
from google.oauth2.service_account import Credentials

try:
    import certstream
except ImportError:
    import subprocess, sys
    subprocess.run([sys.executable, "-m", "pip", "install", "certstream", "tldextract", "-q"])
    import certstream

try:
    import tldextract
except ImportError:
    import subprocess, sys
    subprocess.run([sys.executable, "-m", "pip", "install", "tldextract", "-q"])
    import tldextract

# ── KONFIGURACJA ─────────────────────────────────────────────

MAX_RUNTIME   = int(os.environ.get("CERTSTREAM_RUNTIME", "3600"))  # sekundy
BATCH_FLUSH   = 100        # zapisuj do Sheets co N nowych domen
BATCH_PAUSE   = 2
MAX_QUEUE     = 5000       # max domen w jednym uruchomieniu (bezpiecznik)

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
SHEET_QUEUE = "Kolejka"
# Kolumny zgodne z istniejącym arkuszem:
# A=data_rejestracji, B=domena, C=title, D=strona_dziala, E=url_docelowy,
# F=platforma, G=rejestrator, H=hosting, I=data_rejestracji_whois,
# J=zeskanowano, K=data_skanu, L=zrodlo
HEADER_QUEUE = [
    "data_rejestracji", "domena", "title", "strona_dziala",
    "url_docelowy", "platforma", "rejestrator", "hosting",
    "data_rejestracji_whois", "zeskanowano", "data_skanu", "zrodlo",
]

# TLD które nas interesują
POLISH_TLDS = (".pl", ".com.pl", ".net.pl", ".org.pl", ".eu")

# Słowa kluczowe w domenie → prawdopodobny sklep
SHOP_KEYWORDS = re.compile(
    r"(sklep|shop|store|butik|hurt|hurtowni|kosmet|perfum|meble|moda|"
    r"odzie[zż]|ubrania|outlet|zabawki|dzieci|baby|bio|eko|zdrowie|"
    r"kawa|herbat|sport|fitness|ogrod|dom|kuchnia|elektronik|"
    r"telefon|laptop|komputer|rower|auto|motoryz|zwierz|pet|"
    r"apteka|suplement|kwiat|bi[zż]uteri|zegarek|torebk|buty|obuwie|"
    r"druk|prezent|upomink|galeri|art|foto|hobby|gry|ksiazk|"
    r"sprzet|narzedzi|budowlan|wnetrz|dekoracj|lampki|oswietl)",
    re.IGNORECASE,
)

# Domeny techniczne platform — natychmiastowy sygnał
PLATFORM_CNAME_HINTS = {
    "shoparena.pl":  "Shoper",
    "dcsaas.tech":   "Shoper",
    "mysky-shop.pl": "SkyShop",
    "sky-shop.pl":   "SkyShop",
}

# Wzorce do odrzucenia
BLACKLIST_PATTERNS = re.compile(
    r"(casino|kasyno|bukmacher|bet|poker|slot|hazard|porno|xxx|"
    r"spam|phish|malware|crack|warez|torrent|download|"
    r"agencja|software|studio|lab|tech|digital|media|marketing|"
    r"consulting|serwis|service|holding|invest|finance|bank|"
    r"kancelaria|adwokat|radca|prawnik|notari|ubezpiecz|"
    r"blog|news|portal|forum|wiki|info|magazyn)",
    re.IGNORECASE,
)

# ── GLOBALNE STATE ────────────────────────────────────────────

seen_domains   = set()
pending_rows   = []
stats          = {"total_certs": 0, "candidates": 0, "saved": 0}
lock           = threading.Lock()
sheets_client  = None
spreadsheet    = None
queue_ws       = None
existing_queue = set()
start_time     = time.time()

# ── GOOGLE SHEETS ─────────────────────────────────────────────

def init_sheets():
    global sheets_client, spreadsheet, queue_ws, existing_queue
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_CREDENTIALS"]), scopes=SCOPES)
    sheets_client = gspread.authorize(creds)
    spreadsheet   = sheets_client.open_by_key(os.environ["SPREADSHEET_ID"])

    try:
        queue_ws = spreadsheet.worksheet(SHEET_QUEUE)
    except gspread.WorksheetNotFound:
        queue_ws = spreadsheet.add_worksheet(
            title=SHEET_QUEUE, rows=500000, cols=len(HEADER_QUEUE))
        queue_ws.append_row(HEADER_QUEUE)

    # Załaduj istniejące domeny żeby nie duplikować
    print("  Ładuję istniejącą kolejkę...")
    all_rows = queue_ws.get_all_values()
    for row in all_rows[1:]:
        if row and row[1]:
            existing_queue.add(row[1].strip().lower())
    print(f"  Już w kolejce: {len(existing_queue)} domen")

def flush_to_sheets():
    global pending_rows
    with lock:
        if not pending_rows:
            return
        batch = list(pending_rows)
        pending_rows = []

    try:
        queue_ws.append_rows(batch, value_input_option="RAW")
        stats["saved"] += len(batch)
        print(f"  💾 Zapisano {len(batch)} domen (łącznie: {stats['saved']})")
    except gspread.exceptions.APIError as e:
        if "429" in str(e):
            time.sleep(60)
            queue_ws.append_rows(batch, value_input_option="RAW")
            stats["saved"] += len(batch)
    except Exception as e:
        print(f"  ⚠️  Błąd zapisu: {e}")

# ── DNS HINT ──────────────────────────────────────────────────

def get_platform_from_dns(domain: str) -> str | None:
    try:
        fqdn = socket.getfqdn(domain).lower()
        for pattern, platform in PLATFORM_CNAME_HINTS.items():
            if pattern in fqdn:
                return platform
    except Exception:
        pass
    return None

# ── FILTROWANIE DOMEN ─────────────────────────────────────────

def is_candidate(domain: str) -> tuple[bool, str]:
    """
    Zwraca (True, powód) jeśli domena jest kandydatem do skanowania.
    Powód: 'keyword' | 'cname_shoper' | 'cname_skyshop'
    """
    d = domain.lower().strip("*.").rstrip(".")

    # TLD
    if not any(d.endswith(tld) for tld in POLISH_TLDS):
        return False, ""

    # Usuń subdomeny www
    if d.startswith("www."):
        d = d[4:]

    # Duplikat
    if d in seen_domains or d in existing_queue:
        return False, ""

    # Blacklist
    extracted = tldextract.extract(d)
    root = extracted.domain
    if BLACKLIST_PATTERNS.search(root):
        return False, ""

    # Słowa kluczowe w nazwie domeny
    if SHOP_KEYWORDS.search(root):
        return True, "keyword"

    # CNAME hint (sprawdzamy DNS tylko dla kandydatów bez słowa kluczowego)
    # żeby nie zalewać DNS queries
    platform = get_platform_from_dns(d)
    if platform:
        return True, f"cname_{platform.lower()}"

    return False, ""

# ── CALLBACK CERTSTREAM ───────────────────────────────────────

def certstream_callback(message, context):
    if time.time() - start_time > MAX_RUNTIME:
        raise KeyboardInterrupt("Czas minął")

    if message.get("message_type") != "certificate_update":
        return

    stats["total_certs"] += 1

    cert    = message.get("data", {}).get("leaf_cert", {})
    domains = cert.get("all_domains", [])
    today   = datetime.date.today().isoformat()

    for raw_domain in domains:
        domain = raw_domain.lower().replace("*.", "").rstrip(".")

        if len(pending_rows) >= MAX_QUEUE:
            return

        ok, reason = is_candidate(domain)
        if not ok:
            continue

        with lock:
            if domain in seen_domains:
                continue
            seen_domains.add(domain)

        stats["candidates"] += 1
        platform_hint = ""
        if reason.startswith("cname_"):
            pname = reason.replace("cname_", "").title()
            platform_hint = {"shoper": "Shoper", "skyshop": "SkyShop"}.get(
                pname.lower(), ""
            )

        row = [
            today,           # A: data_rejestracji
            domain,          # B: domena
            "",              # C: title
            "",              # D: strona_dziala
            "",              # E: url_docelowy
            platform_hint,   # F: platforma (hint z DNS)
            "",              # G: rejestrator
            "",              # H: hosting
            "",              # I: data_rejestracji_whois
            "NIE",           # J: zeskanowano
            "",              # K: data_skanu
            f"certstream:{reason}",  # L: zrodlo
        ]

        with lock:
            pending_rows.append(row)

        # Flush co BATCH_FLUSH domen
        if stats["candidates"] % BATCH_FLUSH == 0:
            flush_to_sheets()
            elapsed = int(time.time() - start_time)
            print(
                f"  📡 {elapsed}s | certs: {stats['total_certs']:,} | "
                f"kandydaci: {stats['candidates']} | "
                f"zapisane: {stats['saved']}"
            )

# ── GŁÓWNA FUNKCJA ────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  COLLECT CERTSTREAM v1")
    print(f"  Start: {datetime.datetime.now().isoformat()}")
    print(f"  Max runtime: {MAX_RUNTIME}s ({MAX_RUNTIME//60} min)")
    print("=" * 60)

    print("\n🔗 Łączę z Google Sheets...")
    init_sheets()

    print(f"\n📡 Słucham CertStream (max {MAX_RUNTIME}s)...")
    print("  Filtry: TLD .pl/.com.pl/.eu + słowa e-commerce + CNAME hints\n")

    try:
        certstream.listen_for_events(
            certstream_callback,
            url="wss://certstream.calidog.io/",
        )
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"\n⚠️  CertStream error: {e}")

    # Zapisz resztę
    print("\n💾 Zapisuję pozostałe domeny...")
    flush_to_sheets()

    elapsed = int(time.time() - start_time)
    print("\n" + "=" * 60)
    print(f"  Czas: {elapsed}s")
    print(f"  Certyfikaty przetworzone: {stats['total_certs']:,}")
    print(f"  Kandydaci e-commerce:     {stats['candidates']}")
    print(f"  Zapisano do kolejki:      {stats['saved']}")
    print("=" * 60)

if __name__ == "__main__":
    main()
