#!/usr/bin/env python3
"""
send_to_sheets.py
-----------------
Wysyła wyniki skanowania e-commerce do Google Sheets.
Uruchamiany automatycznie przez GitHub Actions po każdym skanie.
"""

import csv
import glob
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Konfiguracja
# ---------------------------------------------------------------------------

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "1eWUYZeYs1fcYpDzNnW9s8n7HMtC7j2RWN6hdEAkxDis")

# Nagłówki kolumn w arkuszu
HEADERS = [
    "Data skanu",
    "Data rejestracji domeny",
    "Domena",
    "URL",
    "Platforma",
    "Język",
    "Kod HTTP",
]

# ---------------------------------------------------------------------------
# Autoryzacja Google
# ---------------------------------------------------------------------------

def get_sheets_service():
    """Autoryzuje się do Google Sheets API przez Service Account."""
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError:
        print("Instaluję biblioteki Google...")
        os.system("pip install google-auth google-api-python-client -q")
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_json:
        print("BŁĄD: brak zmiennej środowiskowej GOOGLE_CREDENTIALS")
        sys.exit(1)

    creds_dict = json.loads(creds_json)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds)


# ---------------------------------------------------------------------------
# Znajdź plik CSV z wynikami
# ---------------------------------------------------------------------------

def find_results_file() -> Path:
    """Szuka najnowszego pliku ecommerce_ONLY_*.csv w folderze results/."""
    pattern = "results/ecommerce_ONLY_*.csv"
    files = sorted(glob.glob(pattern), reverse=True)
    if not files:
        print(f"Brak pliku wyników ({pattern})")
        sys.exit(0)  # Nie błąd – po prostu nic nie znaleziono dziś
    return Path(files[0])


# ---------------------------------------------------------------------------
# Wyczyść lub utwórz zakładkę na dziś
# ---------------------------------------------------------------------------

def ensure_sheet_tab(service, spreadsheet_id: str, tab_name: str) -> int:
    """Tworzy zakładkę jeśli nie istnieje. Zwraca sheet_id."""
    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = meta.get("sheets", [])

    for sheet in sheets:
        if sheet["properties"]["title"] == tab_name:
            return sheet["properties"]["sheetId"]

    # Utwórz nową zakładkę
    body = {"requests": [{"addSheet": {"properties": {"title": tab_name}}}]}
    resp = service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body=body
    ).execute()
    return resp["replies"][0]["addSheet"]["properties"]["sheetId"]


# ---------------------------------------------------------------------------
# Główna funkcja
# ---------------------------------------------------------------------------

def main():
    scan_date = datetime.now().strftime("%Y-%m-%d")
    tab_name  = f"Skan {scan_date}"

    print(f"Data skanu: {scan_date}")

    # Znajdź wyniki
    results_file = find_results_file()
    print(f"Plik wyników: {results_file}")

    # Wczytaj CSV
    rows_to_send = [HEADERS]
    count = 0

    with open(results_file, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows_to_send.append([
                scan_date,
                row.get("registration_date", ""),
                row.get("domain", ""),
                row.get("url", ""),
                row.get("platform", ""),
                row.get("language", ""),
                str(row.get("http_code", "")),
            ])
            count += 1

    if count == 0:
        print("Brak sklepów e-commerce do wysłania – arkusz nie zostanie zaktualizowany.")
        return

    print(f"Znaleziono {count} sklepów – wysyłam do Google Sheets...")

    # Połącz z API
    service = get_sheets_service()

    # Upewnij się że zakładka istnieje
    ensure_sheet_tab(service, SPREADSHEET_ID, tab_name)

    # Wyślij dane
    range_name = f"'{tab_name}'!A1"
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=range_name,
        valueInputOption="RAW",
        body={"values": rows_to_send},
    ).execute()

    # Pogrub nagłówek
    sheet_id = ensure_sheet_tab(service, SPREADSHEET_ID, tab_name)
    service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"requests": [
            # Pogrubienie nagłówka
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat": {"bold": True},
                            "backgroundColor": {"red": 0.2, "green": 0.6, "blue": 0.2},
                            "foregroundColorStyle": {
                                "rgbColor": {"red": 1, "green": 1, "blue": 1}
                            },
                        }
                    },
                    "fields": "userEnteredFormat(textFormat,backgroundColor,foregroundColorStyle)",
                }
            },
            # Zamroź pierwszy wiersz
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheet_id,
                        "gridProperties": {"frozenRowCount": 1},
                    },
                    "fields": "gridProperties.frozenRowCount",
                }
            },
            # Auto-szerokość kolumn
            {
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": len(HEADERS),
                    }
                }
            },
        ]},
    ).execute()

    print(f"✓ Gotowe! {count} sklepów wysłanych do zakładki „{tab_name}"")
    print(f"  Arkusz: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")


if __name__ == "__main__":
    main()
