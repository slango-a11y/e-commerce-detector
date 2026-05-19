#!/usr/bin/env python3
"""
E-commerce Platform Detector v2
================================
Nowe funkcje:
  - Wybór zakresu dat (--date-from / --date-to)
  - Więcej platform: SkyShop, SOTE, ShopGold, osCommerce, Comarch e-Sklep, RedCart
  - Wykrywanie języka strony (--lang pl,en,de,...)
  - Obsługa wielu TLD: .pl, .com, .com.pl, .eu itd. (--tld pl,com,com.pl)
  - Kolumna: data rejestracji domeny
  - Kolumna: wykryty język strony

Użycie:
    py ecommerce_detector.py
    py ecommerce_detector.py --date-from 2026-05-01 --date-to 2026-05-19
    py ecommerce_detector.py --tld pl,com,com.pl --lang pl,en
    py ecommerce_detector.py --file moje_domeny.txt
"""

import asyncio
import aiohttp
import csv
import logging
import os
import re
import sys
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# KONFIGURACJA
# ---------------------------------------------------------------------------

CONCURRENT_REQUESTS = 50
REQUEST_TIMEOUT     = 10
MAX_RESPONSE_BYTES  = 150_000
FALLBACK_TO_HTTP    = True
OUTPUT_DIR          = Path("results")
DEFAULT_TLDS        = ["pl"]
DEFAULT_LANGS: list[str] = []

# ---------------------------------------------------------------------------
# ŹRÓDŁA DOMEN
# ---------------------------------------------------------------------------

DOMAIN_SOURCES = [
    {
        "name": "whoisextractor",
        "url_template": (
            "https://raw.githubusercontent.com/whoisextractor/newly-registered-domains"
            "/main/{date}/domains.txt"
        ),
        "static": False,
    },
    {
        "name": "cenk_nrd",
        "url_template": "https://raw.githubusercontent.com/cenk/nrd/main/nrd-last-10-days.txt",
        "static": True,
    },
]

# ---------------------------------------------------------------------------
# SYGNATURY PLATFORM E-COMMERCE
# ---------------------------------------------------------------------------

PLATFORMS: dict[str, list[str]] = {
    "Shopify": [
        "cdn.shopify.com",
        "shopify.com/s/files",
        "window.Shopify",
    ],
    "WooCommerce": [
        "wp-content/plugins/woocommerce",
        'class="woocommerce',
        "WooCommerce",
    ],
    "PrestaShop": [
        "var prestashop =",
        "/modules/ps_",
        "PrestaShop",
    ],
    "Magento": [
        "Magento_Ui",
        "mage/bootstrap",
        "magentoRegion",
    ],
    "Shoper": [
        "cdn.shoper.pl",
        "shoper-static",
    ],
    "IdoSell": [
        "idosell.com",
        "iai-shop.com",
        "iaicdn.com",
    ],
    "SkyShop": [
        "static.sky-shop.pl",
        "sky-shop.pl",
        "skyshop.pl",
    ],
    "SOTE": [
        "soteshop",
        "cdn.sote.pl",
        "sote.pl",
    ],
    "ShopGold": [
        "cdn.shopgold.pl",
        "shopgold.pl",
    ],
    "osCommerce": [
        "oscommerce",
        "osCsid",
        "osC_Products",
    ],
    "Comarch e-Sklep": [
        "e-sklep.comarch",
        "cdn.comarch",
        "comarchesklep",
    ],
    "RedCart": [
        "cdn.redcart.pl",
        "redcart.pl",
        "rc_shop",
    ],
    "Selly": [
        "static.selly.pl",
        "selly.pl",
    ],
    "OpenCart": [
        "catalog/view/theme",
        "route=common/home",
    ],
}

# ---------------------------------------------------------------------------
# WYKRYWANIE JĘZYKA
# ---------------------------------------------------------------------------

LANG_HINTS: dict[str, list[str]] = {
    "pl": ["dodaj do koszyka", "strona główna", "sklep internetowy", "zamów"],
    "en": ["add to cart", "checkout", "shopping cart", "about us"],
    "de": ["in den warenkorb", "startseite", "kasse"],
    "fr": ["ajouter au panier", "accueil"],
    "uk": ["кошик", "головна"],
}


def detect_language(html: str) -> Optional[str]:
    html_lower = html.lower()
    m = re.search(r'<html[^>]+lang=["\']?([a-z]{2})', html_lower)
    if m:
        return m.group(1)
    m = re.search(r'content-language["\s:=]+([a-z]{2})', html_lower)
    if m:
        return m.group(1)
    for lang, hints in LANG_HINTS.items():
        if any(h in html_lower for h in hints):
            return lang
    return None


def detect_platform(html: str) -> Optional[str]:
    html_lower = html.lower()
    for platform, signatures in PLATFORMS.items():
        if any(sig.lower() in html_lower for sig in signatures):
            return platform
    return None

# ---------------------------------------------------------------------------
# POBIERANIE DOMEN
# ---------------------------------------------------------------------------

async def fetch_text(session: aiohttp.ClientSession, url: str) -> Optional[str]:
    headers = {"User-Agent": "Mozilla/5.0"}
    if os.environ.get("GITHUB_TOKEN"):
        headers["Authorization"] = f"token {os.environ['GITHUB_TOKEN']}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=60), headers=headers) as r:
            if r.status != 200:
                return None
            return (await r.read()).decode("utf-8", errors="ignore")
    except Exception:
        return None


async def collect_domains(date_from: date, date_to: date, tlds: list[str]) -> list[tuple[str, str]]:
    all_domains: dict[str, str] = {}
    static_fetched: set[str] = set()

    async with aiohttp.ClientSession() as session:
        current = date_from
        while current <= date_to:
            date_str = current.strftime("%Y-%m-%d")

            for source in DOMAIN_SOURCES:
                if source["static"]:
                    if source["name"] in static_fetched:
                        continue
                    url = source["url_template"]
                    static_fetched.add(source["name"])
                else:
                    url = source["url_template"].format(date=date_str)

                log.info(f"Pobieranie [{source['name']}] {date_str if not source['static'] else '(static)'}")
                text = await fetch_text(session, url)

                if text is None:
                    log.warning(f"  Brak odpowiedzi – pomijam")
                    continue

                lines = [l.strip().lower() for l in text.splitlines() if l.strip()]
                for domain in lines:
                    if any(domain == tld or domain.endswith(f".{tld}") for tld in tlds):
                        if domain not in all_domains:
                            all_domains[domain] = date_str

                log.info(f"  Łącznie unikalnych domen: {len(all_domains):,}")

            current += timedelta(days=1)

    return sorted(all_domains.items())

# ---------------------------------------------------------------------------
# SPRAWDZANIE DOMENY
# ---------------------------------------------------------------------------

async def check_domain(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    domain: str,
    reg_date: str,
    lang_filter: list[str],
) -> Optional[dict]:
    result = {
        "domain": domain,
        "registration_date": reg_date,
        "url": "",
        "status": "unreachable",
        "platform": None,
        "language": None,
        "http_code": None,
        "error": None,
    }

    async with semaphore:
        for scheme in (["https", "http"] if FALLBACK_TO_HTTP else ["https"]):
            url = f"{scheme}://{domain}"
            try:
                async with session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
                    allow_redirects=True,
                    max_redirects=5,
                ) as resp:
                    result["url"] = str(resp.url)
                    result["http_code"] = resp.status

                    if resp.status == 200:
                        chunk = await resp.content.read(MAX_RESPONSE_BYTES)
                        html = chunk.decode("utf-8", errors="ignore")

                        lang = detect_language(html)
                        result["language"] = lang

                        if lang_filter and lang not in lang_filter:
                            return None  # nie ten język

                        platform = detect_platform(html)
                        result["platform"] = platform
                        result["status"] = "ecommerce" if platform else "no_ecommerce"
                        return result
                    else:
                        result["status"] = f"http_{resp.status}"
                        return result

            except aiohttp.ClientConnectorError:
                result["error"] = "connection_refused"
            except asyncio.TimeoutError:
                result["error"] = "timeout"
            except Exception as e:
                result["error"] = str(e)[:80]

        return result

# ---------------------------------------------------------------------------
# GŁÓWNA PĘTLA
# ---------------------------------------------------------------------------

log = logging.getLogger("ecommerce_detector")


async def run(date_from, date_to, tlds, lang_filter, local_file=None):
    OUTPUT_DIR.mkdir(exist_ok=True)
    label = f"{date_from}_{date_to}" if date_from != date_to else str(date_from)
    output_file    = OUTPUT_DIR / f"ecommerce_results_{label}.csv"
    ecommerce_file = OUTPUT_DIR / f"ecommerce_ONLY_{label}.csv"

    if local_file:
        raw = local_file.read_text(encoding="utf-8", errors="ignore").splitlines()
        domain_pairs = [
            (d.strip().lower(), "unknown") for d in raw
            if d.strip() and any(d.strip().lower().endswith(f".{t}") for t in tlds)
        ]
        log.info(f"Wczytano {len(domain_pairs):,} domen z {local_file}")
    else:
        domain_pairs = await collect_domains(date_from, date_to, tlds)

    if not domain_pairs:
        log.error("Brak domen do sprawdzenia.")
        return

    total = len(domain_pairs)
    log.info(f"\n{'='*60}")
    log.info(f"Sprawdzanie {total:,} domen | TLD: {tlds} | concurrent: {CONCURRENT_REQUESTS}")
    if lang_filter:
        log.info(f"Filtr języka: {lang_filter}")
    log.info(f"{'='*60}\n")

    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS + 10, ssl=False)
    ua = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept-Language": "pl-PL,pl;q=0.9,en;q=0.8",
    }

    fieldnames = ["domain", "registration_date", "url", "status", "platform", "language", "http_code", "error"]
    ecommerce_results = []
    done = 0

    async with aiohttp.ClientSession(connector=connector, headers=ua) as session:
        with open(output_file, "w", newline="", encoding="utf-8") as f_all:
            writer = csv.DictWriter(f_all, fieldnames=fieldnames)
            writer.writeheader()

            for i in range(0, total, 500):
                batch = domain_pairs[i:i + 500]
                tasks = [check_domain(session, semaphore, d, r, lang_filter) for d, r in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for res in results:
                    done += 1
                    if res is None or isinstance(res, Exception):
                        continue
                    writer.writerow(res)
                    if res.get("platform"):
                        ecommerce_results.append(res)

                log.info(f"Postęp: {done:,}/{total:,} ({done/total*100:.1f}%) | Sklepy: {len(ecommerce_results):,}")

    if ecommerce_results:
        with open(ecommerce_file, "w", newline="", encoding="utf-8") as f_ec:
            writer_ec = csv.DictWriter(f_ec, fieldnames=fieldnames)
            writer_ec.writeheader()
            writer_ec.writerows(ecommerce_results)

    log.info(f"\n{'='*60}")
    log.info("PODSUMOWANIE")
    log.info(f"Sprawdzono:   {done:,}")
    log.info(f"Znaleziono:   {len(ecommerce_results):,} sklepów")

    platform_counts: dict[str, int] = {}
    lang_counts:     dict[str, int] = {}
    for r in ecommerce_results:
        p = r["platform"] or "?"
        platform_counts[p] = platform_counts.get(p, 0) + 1
        l = r["language"] or "?"
        lang_counts[l] = lang_counts.get(l, 0) + 1

    log.info("\nPlatformy:")
    for p, c in sorted(platform_counts.items(), key=lambda x: -x[1]):
        log.info(f"  {p:<25} {c:>5,}")

    log.info("\nJęzyki:")
    for l, c in sorted(lang_counts.items(), key=lambda x: -x[1]):
        log.info(f"  {l:<10} {c:>5,}")

    log.info(f"\nWszystkie → {output_file}")
    log.info(f"Sklepy    → {ecommerce_file}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("detector.log", encoding="utf-8"),
        ],
    )

    today = date.today()
    yesterday = str(today - timedelta(days=1))

    parser = argparse.ArgumentParser(
        description="E-commerce detector v2",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Przykłady:
  py ecommerce_detector.py
  py ecommerce_detector.py --date-from 2026-05-01 --date-to 2026-05-19
  py ecommerce_detector.py --tld pl,com,com.pl,eu
  py ecommerce_detector.py --lang pl
  py ecommerce_detector.py --file domeny.txt --tld pl,com
        """,
    )
    parser.add_argument("--date-from", default=yesterday,
        help="Od daty RRRR-MM-DD (domyślnie: wczoraj)")
    parser.add_argument("--date-to",   default=yesterday,
        help="Do daty RRRR-MM-DD (domyślnie: wczoraj)")
    parser.add_argument("--tld", default="pl",
        help="TLD oddzielone przecinkami, np. pl,com,com.pl (domyślnie: pl)")
    parser.add_argument("--lang", default="",
        help="Filtr języków, np. pl,en (domyślnie: wszystkie)")
    parser.add_argument("--file", "-f", help="Lokalny plik z domenami")
    parser.add_argument("--concurrent", "-c", type=int, default=CONCURRENT_REQUESTS,
        help=f"Równoległe zapytania (domyślnie: {CONCURRENT_REQUESTS})")
    parser.add_argument("--github-token", default=os.environ.get("GITHUB_TOKEN"))

    args = parser.parse_args()

    CONCURRENT_REQUESTS = args.concurrent
    if args.github_token:
        os.environ["GITHUB_TOKEN"] = args.github_token

    try:
        date_from = date.fromisoformat(args.date_from)
        date_to   = date.fromisoformat(args.date_to)
    except ValueError:
        print("Błąd: daty w formacie RRRR-MM-DD")
        sys.exit(1)

    if date_from > date_to:
        print("Błąd: --date-from musi być <= --date-to")
        sys.exit(1)

    tlds        = [t.strip().lstrip(".").lower() for t in args.tld.split(",")  if t.strip()]
    lang_filter = [l.strip().lower()             for l in args.lang.split(",") if l.strip()]
    local_file  = Path(args.file) if args.file else None

    if local_file and not local_file.exists():
        print(f"Błąd: plik nie istnieje: {local_file}")
        sys.exit(1)

    asyncio.run(run(date_from, date_to, tlds, lang_filter, local_file))
