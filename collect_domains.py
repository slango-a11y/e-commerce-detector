"""
collect_domains.py
──────────────────
Codziennie pobiera nowe domeny z cenk/nrd i whoisextractor,
filtruje po TLD i zapisuje do zakładki "Kolejka" w Google Sheets.

Struktura zakładki "Kolejka":
  A: data_rejestracji  B: domena  C: zeskanowano (TAK/NIE)  D: data_skanu
"""

import os
import re
import gzip
import datetime
import requests
import gspread
from google.oauth2.service_account import Credentials
import json

# ── KONFIGURACJA ──────────────────────────────────────────────

TARGET_TLD = ["pl", "com.pl", "com", "eu", "net", "org"]

SOURCES = [
    # whoisextractor - lista z poprzedniego dnia
    "https://raw.githubusercontent.com/whoisextractor/newly-registered-domains/main/nrd-1d.txt",
    # cenk/nrd - lista z ostatnich 10 dni (deduplikacja po stronie arkusza)
    "https://raw.githubusercontent.com/cenk/nrd/main/nrd-last-10-days.txt",
]

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

QUEUE_SHEET  = "Kolejka"
HEADER_ROW   = ["data_rejestracji", "domena", "zeskanowano", "data_skanu"]

# ── FUNKCJE ───────────────────────────────────────────────────

def get_sheets_client():
    creds_json = os.environ["GOOGLE_CREDENTIALS"]
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)

def fetch_domains(url: str) -> list[str]:
    try:
        headers = {"User-Agent": "Mozilla/5.0 (ecommerce-research-bot)"}
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code == 200:
            return [l.strip().lower() for l in r.text.splitlines() if l.strip()]
    except Exception as e:
        print(f"  ⚠️  Błąd pobierania {url}: {e}")
    return []

def filter_tld(domains: list[str]) -> list[str]:
    result = []
    for d in domains:
        for tld in TARGET_TLD:
            if d.endswith(f".{tld}"):
                result.append(d)
                break
    return result

def ensure_queue_sheet(spreadsheet):
    """Tworzy zakładkę Kolejka jeśli nie istnieje, dodaje nagłówek."""
    try:
        ws = spreadsheet.worksheet(QUEUE_SHEET)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=QUEUE_SHEET, rows=1000, cols=4)
        ws.append_row(HEADER_ROW)
        print(f"  ✅ Utworzono zakładkę '{QUEUE_SHEET}'")
    return ws

def get_existing_domains(ws) -> set[str]:
    """Pobiera listę domen już zapisanych w kolejce (kolumna B)."""
    try:
        col = ws.col_values(2)  # kolumna B = domena
        return set(d.strip().lower() for d in col[1:] if d.strip())  # [1:] pomija nagłówek
    except Exception:
        return set()

def save_to_queue(ws, domains: list[str], reg_date: str):
    """Zapisuje nowe domeny do kolejki (batch żeby nie przekroczyć limitów API)."""
    rows = [[reg_date, d, "NIE", ""] for d in domains]

    # Batch po 500 wierszy żeby nie przekroczyć limitu API Google
    batch_size = 500
    saved = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        ws.append_rows(batch, value_input_option="RAW")
        saved += len(batch)
        print(f"  📥 Zapisano {saved}/{len(rows)} domen...")

    return saved

# ── GŁÓWNA FUNKCJA ────────────────────────────────────────────

def main():
    today = datetime.date.today().isoformat()

    print("=" * 55)
    print("  ZBIERANIE DOMEN → KOLEJKA")
    print(f"  Data: {today}")
    print(f"  TLD: {', '.join(TARGET_TLD)}")
    print("=" * 55)

    # 1. Pobierz domeny ze źródeł
    all_raw = []
    for url in SOURCES:
        print(f"\n📥 Pobieram: {url.split('/')[-1]}")
        domains = fetch_domains(url)
        filtered = filter_tld(domains)
        print(f"   Znaleziono: {len(domains)} | Po filtrze TLD: {len(filtered)}")
        all_raw.extend(filtered)

    # Deduplikacja lokalna
    all_raw = list(set(all_raw))
    print(f"\n📊 Unikalnych domen do zapisania: {len(all_raw)}")

    if not all_raw:
        print("❌ Brak domen. Źródła niedostępne.")
        return

    # 2. Połącz z Google Sheets
    print("\n🔗 Łączę z Google Sheets...")
    gc = get_sheets_client()
    sh = gc.open_by_key(os.environ["SPREADSHEET_ID"])
    ws = ensure_queue_sheet(sh)

    # 3. Sprawdź które domeny już są w kolejce (unikaj duplikatów)
    print("🔍 Sprawdzam duplikaty w kolejce...")
    existing = get_existing_domains(ws)
    new_domains = [d for d in all_raw if d not in existing]
    print(f"   Już w kolejce: {len(existing)} | Nowych: {len(new_domains)}")

    if not new_domains:
        print("✅ Wszystkie domeny już są w kolejce. Nic do dodania.")
        return

    # 4. Zapisz do kolejki
    print(f"\n💾 Zapisuję {len(new_domains)} nowych domen...")
    saved = save_to_queue(ws, new_domains, today)

    print(f"\n✅ GOTOWE — Dodano {saved} domen do zakładki '{QUEUE_SHEET}'")
    print(f"   Data rejestracji: {today}")
    print(f"   Zostaną zeskanowane za ~30 dni")

if __name__ == "__main__":
    main()
