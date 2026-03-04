import os
import re
import json
import time
import random
from datetime import datetime
from typing import Optional, Dict, Any, List
from urllib.parse import urljoin

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ============================================================
# Slovenijales scraper (GitHub/CI friendly) - stable
# ============================================================

SHOP_NAME = "Slovenijales"
BASE_URL = "https://trgovina.slovenijales.si"
DDV_RATE = 0.22

SLOVENIJALES_CATEGORIES = {
    "LESNI MATERIALI": [
        "https://trgovina.slovenijales.si/lesni-materiali/lepljene-plosce",
        "https://trgovina.slovenijales.si/lesni-materiali/gradbene-plosce-in-les",
        "https://trgovina.slovenijales.si/lesni-materiali/opazne-plosce",
        "https://trgovina.slovenijales.si/lesni-materiali/lepljeni-nosilci",
        "https://trgovina.slovenijales.si/lesni-materiali/vezane-plosce",
        "https://trgovina.slovenijales.si/lesni-materiali/letve-palice-in-rocaji",
    ],
    "PLOSKOVNI MATERIALI": [
        "https://trgovina.slovenijales.si/ploskovni-materiali/iverne-plosce",
        "https://trgovina.slovenijales.si/ploskovni-materiali/oplemenitene-iverne-plosce",
        "https://trgovina.slovenijales.si/ploskovni-materiali/vlaknene-plosce",
        "https://trgovina.slovenijales.si/ploskovni-materiali/kuhinjski-pulti-in-obloge",
        "https://trgovina.slovenijales.si/ploskovni-materiali/kompaktne-plosce",
    ],
    "TALNE IN STENSKE OBLOGE": [
        "https://trgovina.slovenijales.si/talne-in-stenske-obloge/talne-obloge",
        "https://trgovina.slovenijales.si/talne-in-stenske-obloge/masivne-obloge",
        "https://trgovina.slovenijales.si/talne-in-stenske-obloge/zakljucni-profili-in-letve",
        "https://trgovina.slovenijales.si/talne-in-stenske-obloge/vodoodporne-stenske-obloge-rocko",
        "https://trgovina.slovenijales.si/talne-in-stenske-obloge/akusticni-paneli",
    ],
}

EXPORT_EXCEL = True

SLEEP_MIN = float(os.environ.get("SCRAPE_SLEEP_MIN", "4.0"))
SLEEP_MAX = float(os.environ.get("SCRAPE_SLEEP_MAX", "6.0"))
STARTUP_JITTER_CI = (0.5, 3.0)
STARTUP_JITTER_LOCAL = (2.0, 12.0)

FLUSH_JSON_EVERY = int(os.environ.get("FLUSH_JSON_EVERY", "50"))
MAX_PAGES = int(os.environ.get("MAX_PAGES", "250"))
MAX_BLOCK_RETRIES = int(os.environ.get("MAX_BLOCK_RETRIES", "3"))

_ALLOWED_EM = {
    "ar", "ha",
    "kam", "kg", "km", "kwh", "kw", "wat",
    "kpl", "kos", "kos dan", "kos mes",
    "m", "m2", "m3",
    "cm", "kN", "km2", "kg/m3", "kg/h", "kg/l",
    "m/dan", "m/h", "m/min", "m/s",
    "m2 dan", "m2 mes",
    "m3/dan", "m3/h", "m3/min", "m3/s", "m3 d",
    "t", "tm", "t/dan", "t/h", "t/let",
    "h", "min", "s",
    "lit/dan", "lit/h", "lit/min", "lit/s",
    "L",
    "par", "pal", "sto", "skl", "del", "ključ",
    "os", "os d",
    "x",
    "delež",
    "oc", "op",
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
]
_RUN_UA = os.environ.get("SCRAPE_UA") or random.choice(USER_AGENTS)

EXCEL_COLS = [
    "Skupina",
    "Zap",
    "Oznaka / naziv",
    "EAN",
    "Opis",
    "Opis izdelka",
    "EM",
    "Valuta",
    "DDV",
    "Proizvajalec",
    "Veljavnost od",
    "Dobava",
    "Cena / EM (z DDV)",
    "Akcijska cena / EM (z DDV)",
    "Cena / EM (brez DDV)",
    "Akcijska cena / EM (brez DDV)",
    "URL",
    "SLIKA URL",
    "Zaloga po poslovalnicah",
]

_log_file = None
_debug_dir = None
_global_item_counter = 0


def log_and_print(message: str, to_file: bool = True) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"[{ts}] {message}"
    print(msg)
    if to_file and _log_file:
        try:
            _log_file.write(msg + "\n")
            _log_file.flush()
        except Exception:
            pass


def human_sleep(min_s: float, max_s: float) -> None:
    time.sleep(random.uniform(min_s, max_s))


def is_ci() -> bool:
    return os.environ.get("GITHUB_ACTIONS", "").lower() == "true"


def create_output_paths(shop_name: str):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_root = os.environ.get("OUTPUT_DIR", script_dir)

    today = datetime.now().strftime("%Y-%m-%d")
    daily_dir = os.path.join(output_root, "Ceniki_Scraping", shop_name, today)
    os.makedirs(daily_dir, exist_ok=True)

    filename_date = datetime.now().strftime("%d_%m_%Y")
    json_path = os.path.join(daily_dir, f"{shop_name}_Podatki_{filename_date}.json")
    excel_path = os.path.join(daily_dir, f"{shop_name}_Podatki_{filename_date}.xlsx")
    log_path = os.path.join(daily_dir, f"{shop_name}_Scraping_Log_{datetime.now().strftime('%H-%M-%S')}.txt")

    print(f"JSON pot: {json_path}")
    print(f"Excel pot: {excel_path}")
    print(f"Log pot: {log_path}")
    return json_path, excel_path, log_path, daily_dir


def save_debug_html(kind: str, url: str, html: str) -> None:
    global _debug_dir
    if not _debug_dir or not html:
        return
    try:
        safe = re.sub(r"[^a-zA-Z0-9]+", "_", url)[:120]
        path = os.path.join(_debug_dir, f"debug_{kind}_{safe}.html")
        if os.path.exists(path):
            return
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
    except Exception:
        pass


def write_empty_outputs(json_path: str, excel_path: str) -> None:
    try:
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump([], f, ensure_ascii=False, indent=2)
    except Exception:
        pass
    try:
        df = pd.DataFrame([], columns=EXCEL_COLS)
        df.to_excel(excel_path, index=False)
    except Exception:
        pass


def is_block_page(html: str) -> bool:
    if not html:
        return False
    t = html.lower()

    strong_needles = [
        "/cdn-cgi/challenge-platform",
        "cf-chl-",
        "cloudflare ray id",
        "attention required",
        "access denied",
        "request blocked",
        "your request has been blocked",
        "verifying you are human",
        "verify you are human",
        "please enable cookies",
        "enable javascript and cookies",
        "perimeterx",
        "px-captcha",
        "px-block",
        "datadome",
        "incapsula",
        "sucuri website firewall",
        "ddos-guard",
        "akamai bot manager",
    ]
    if any(n in t for n in strong_needles):
        return True

    if re.search(r"\b(hcaptcha|cf-turnstile|g-recaptcha|px-captcha|data-sitekey)\b", t):
        if any(x in t for x in ("verify", "verifying", "blocked", "access denied", "challenge")):
            return True

    soft = 0
    for n in ("captcha", "challenge", "bot", "blocked", "verify", "verification"):
        if n in t:
            soft += 1
    return soft >= 4


def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1.2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def get_headers(referer: Optional[str] = None) -> Dict[str, str]:
    return {
        "User-Agent": _RUN_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "sl-SI,sl;q=0.9,en-US;q=0.7,en;q=0.5",
        "Connection": "keep-alive",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
        "Referer": referer or BASE_URL,
    }


def get_page_content(session: requests.Session, url: str, referer: Optional[str] = None) -> Optional[str]:
    headers = get_headers(referer=referer)

    for attempt in range(1, MAX_BLOCK_RETRIES + 1):
        try:
            resp = session.get(url, headers=headers, timeout=25)

            if resp.status_code == 403:
                wait = min(120, 10 * attempt + random.uniform(0, 10))
                log_and_print(f"HTTP 403 @ {url} -> sleep {wait:.1f}s (poskus {attempt}/{MAX_BLOCK_RETRIES})")
                time.sleep(wait)
                continue

            if resp.status_code == 429:
                ra = resp.headers.get("Retry-After")
                wait = int(ra) if (ra and ra.isdigit()) else random.randint(30, 120)
                log_and_print(f"HTTP 429 @ {url} -> backoff {wait}s (poskus {attempt}/{MAX_BLOCK_RETRIES})")
                time.sleep(wait)
                continue

            if resp.status_code in (500, 502, 503, 504):
                wait = random.randint(10, 60)
                log_and_print(f"HTTP {resp.status_code} @ {url} -> backoff {wait}s (poskus {attempt}/{MAX_BLOCK_RETRIES})")
                time.sleep(wait)
                continue

            if not resp.ok:
                log_and_print(f"HTTP {resp.status_code} @ {url}")
                return None

            html = resp.text or ""
            if is_block_page(html):
                wait = min(300, 30 * attempt + random.uniform(0, 30))
                log_and_print(f"[BLOCK] challenge @ {url} -> sleep {wait:.1f}s (poskus {attempt}/{MAX_BLOCK_RETRIES})")
                save_debug_html("block", url, html)
                time.sleep(wait)
                continue

            return html

        except requests.RequestException as e:
            wait = min(60, 3 * attempt + random.uniform(0, 6))
            log_and_print(f"Request error @ {url}: {e} -> sleep {wait:.1f}s (poskus {attempt}/{MAX_BLOCK_RETRIES})")
            time.sleep(wait)

    log_and_print(f"[BLOCK] Preveč poskusov, preskakujem URL: {url}")
    return None


def _parse_float_any(price_str: str) -> Optional[float]:
    if not price_str:
        return None
    s = str(price_str).strip()
    s = re.sub(r"[^\d,\.]", "", s)
    if not s:
        return None
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        if "," in s and "." not in s:
            s = s.replace(",", ".")
        if s.count(".") > 1:
            parts = s.split(".")
            s = "".join(parts[:-1]) + "." + parts[-1]
    try:
        return float(s)
    except Exception:
        return None


def fmt_2dec(val: Optional[float]) -> str:
    if val is None:
        return ""
    return f"{val:.2f}".replace(".", ",")


def round_price_2dec(price_str: Optional[str]) -> str:
    v = _parse_float_any(price_str) if price_str else None
    return fmt_2dec(v)


def convert_price_to_without_vat(price_str: Optional[str], vat_rate: float) -> str:
    v = _parse_float_any(price_str) if price_str else None
    if v is None:
        return ""
    return fmt_2dec(v / (1 + vat_rate))


def normalize_em(unit: str) -> str:
    if not unit:
        return "kos"
    u = str(unit).strip()
    u = u.replace("m²", "m2").replace("m³", "m3").replace("²", "2").replace("³", "3")
    u = re.sub(r"\s+", " ", u).strip()
    u = u.replace(".", "").strip().strip("/")
    if u == "l":
        u = "L"
    ul = u.lower()
    if u in _ALLOWED_EM:
        return u
    if ul in _ALLOWED_EM:
        return ul
    if ul in ("kosov", "kos", "kom", "pcs", "pc"):
        return "kos"
    return "kos"


def extract_ean_raw(soup: BeautifulSoup) -> str:
    txt = soup.get_text(" ", strip=True)
    m = re.search(r"(EAN|GTIN)\s*[:#]?\s*([0-9]{6,20})", txt, flags=re.IGNORECASE)
    return m.group(2).strip() if m else ""


def extract_sku(soup: BeautifulSoup) -> str:
    sku = soup.select_one('meta[itemprop="sku"]')
    if sku and sku.get("content"):
        return sku.get("content", "").strip()
    # fallback: sku v besedilu
    txt = soup.get_text(" ", strip=True)
    m = re.search(r"\bSKU\b\s*[:#]?\s*([A-Za-z0-9\-_]{4,})", txt, flags=re.IGNORECASE)
    return m.group(1).strip() if m else ""


def extract_title(soup: BeautifulSoup) -> str:
    h1 = soup.select_one('h1[itemprop="name"]') or soup.select_one("h1")
    return h1.get_text(" ", strip=True) if h1 else ""


def extract_long_description(soup: BeautifulSoup) -> str:
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        return meta_desc.get("content").strip()

    # pri slovenijales pogosto obstaja sekcija z opisom izdelka
    for sel in (".product-desc", ".product-description", "#description", ".tab-content"):
        d = soup.select_one(sel)
        if d:
            txt = d.get_text("\n", strip=True)
            if txt and len(txt) > 30:
                return (txt[:8000].rstrip() + "…") if len(txt) > 8000 else txt
    return ""


def extract_image_url(soup: BeautifulSoup) -> str:
    img = soup.select_one(".flexslider .slides img") or soup.select_one("img[src]")
    return img.get("src", "").strip() if img else ""


def extract_price(soup: BeautifulSoup) -> (str, str):
    """Vrne (cena, akcijska cena) z DDV, oba zaokrožena."""
    new_p = soup.select_one(".product-info-price span.new")
    old_p = soup.select_one(".product-info-price span.old")

    price = ""
    special = ""

    if new_p:
        val = re.search(r"([\d\.,]+)", new_p.get_text(" ", strip=True))
        if val:
            price_candidate = round_price_2dec(val.group(1).strip())
            if old_p:
                # če je old_p prisoten -> new je akcijska
                special = price_candidate
                val_old = re.search(r"([\d\.,]+)", old_p.get_text(" ", strip=True))
                if val_old:
                    price = round_price_2dec(val_old.group(1).strip())
            else:
                price = price_candidate

    return price, special


def extract_stock_by_branches(soup: BeautifulSoup) -> Dict[str, int]:
    """Best-effort: izlušči zalogo po poslovalnicah iz teksta (npr. 'Ljubljana 12 kos')."""
    txt = soup.get_text("\n", strip=True).replace("\xa0", " ")
    # tipični pattern: "PE Ljubljana: 12" ali "Ljubljana 12 kos"
    matches = re.findall(r"([A-ZČŠŽ][A-Za-zČŠŽčšž \-\/]{2,40})\s*[:\-]\s*(\d+)\s*(?:kos|kosi)?", txt)
    stock: Dict[str, int] = {}
    for name, qty in matches:
        name = re.sub(r"\s+", " ", name).strip()
        # filtriraj očitno nerelevantne besede
        if len(name) < 3:
            continue
        if name.lower() in ("opis", "lastnosti", "tehnične lastnosti"):
            continue
        try:
            q = int(qty)
        except Exception:
            continue
        # pustimo samo "razumno" količino
        if 0 <= q <= 9999:
            stock[name] = max(stock.get(name, 0), q)
    return stock


def get_product_links_from_category(session: requests.Session, category_url: str) -> List[str]:
    links: List[str] = []
    stariprvi_url = None

    for page in range(1, MAX_PAGES + 1):
        url = f"{category_url}?page={page}"
        log_and_print(f"  Stran {page}: {url}")

        html = get_page_content(session, url, referer=category_url)
        if not html:
            break

        soup = BeautifulSoup(html, "html.parser")
        products = soup.select('div.single-product.border-left[itemscope]')
        if not products:
            break

        # Preverjanje ponavljanja
        first_a = products[0].select_one(".product-img a")
        first_href = first_a.get("href") if first_a else None
        if page > 1 and first_href and stariprvi_url and first_href == stariprvi_url:
            log_and_print("  Stran se ponavlja. Konec kategorije.")
            break
        stariprvi_url = first_href

        for p in products:
            a = p.select_one(".product-img a")
            if a and a.get("href"):
                href = a["href"]
                full = href if href.startswith("http") else urljoin(BASE_URL, href)
                links.append(full)

        # pagination "Naprej"
        if not soup.select_one('ul.pagination a[aria-label="Naprej"]'):
            break

        human_sleep(2.0, 6.0)

    return list(dict.fromkeys(links))


def extract_product_details(session: requests.Session, url: str, cat_name: str, date_str: str) -> Optional[Dict[str, Any]]:
    global _global_item_counter

    html = get_page_content(session, url, referer=url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    _global_item_counter += 1

    data: Dict[str, Any] = {
        "Skupina": cat_name,
        "Zap": _global_item_counter,
        "Oznaka / naziv": "",
        "EAN": "",
        "Opis": "",
        "Opis izdelka": "",
        "EM": "kos",
        "Valuta": "EUR",
        "DDV": "22",
        "Proizvajalec": "",
        "Veljavnost od": date_str,
        "Dobava": "",
        "Cena / EM (z DDV)": "",
        "Akcijska cena / EM (z DDV)": "",
        "Cena / EM (brez DDV)": "",
        "Akcijska cena / EM (brez DDV)": "",
        "URL": url,
        "SLIKA URL": "",
        "Zaloga po poslovalnicah": "",
    }

    data["Opis"] = extract_title(soup)
    data["Opis izdelka"] = extract_long_description(soup)
    data["Oznaka / naziv"] = extract_sku(soup)
    data["EAN"] = extract_ean_raw(soup)
    data["SLIKA URL"] = extract_image_url(soup)

    price, special = extract_price(soup)
    data["Cena / EM (z DDV)"] = price
    data["Akcijska cena / EM (z DDV)"] = special
    data["Cena / EM (brez DDV)"] = convert_price_to_without_vat(price, DDV_RATE)
    data["Akcijska cena / EM (brez DDV)"] = convert_price_to_without_vat(special, DDV_RATE)

    # EM: slovenijales večinoma prodaja kos; če najde "m2" v naslovu, set m2
    title = (data["Opis"] or "").lower()
    if " m2" in title or " m²" in title:
        data["EM"] = "m2"
    else:
        data["EM"] = normalize_em(data.get("EM") or "kos")

    # Dobavljivost po poslovalnicah
    stock = extract_stock_by_branches(soup)
    if stock:
        data["Zaloga po poslovalnicah"] = json.dumps(stock, ensure_ascii=False)
        data["Dobava"] = "DA" if any(v > 0 for v in stock.values()) else "NE"
    else:
        # fallback: tekstovno
        tl = soup.get_text(" ", strip=True).lower()
        if "ni na zalogi" in tl:
            data["Dobava"] = "NE"
        elif "na zalogi" in tl:
            data["Dobava"] = "DA"
        else:
            data["Dobava"] = ""

    return data


def _item_key(item: dict) -> str:
    return str(item.get("URL") or "")


def save_data_append(new_data: List[Dict[str, Any]], json_path: str) -> None:
    if not new_data:
        return

    all_data: List[Dict[str, Any]] = []
    if os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                all_data = json.load(f)
        except Exception:
            all_data = []

    data_dict = {_item_key(x): x for x in all_data if isinstance(x, dict) and x.get("URL")}
    for x in new_data:
        if isinstance(x, dict) and x.get("URL"):
            data_dict[_item_key(x)] = x

    final_list = list(data_dict.values())
    try:
        final_list.sort(key=lambda x: int(x.get("Zap", 0)))
    except Exception:
        pass

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(final_list, f, ensure_ascii=False, indent=2)

    log_and_print("Shranjen JSON (batch).")


def write_excel_from_json(json_path: str, excel_path: str) -> None:
    if not os.path.exists(json_path):
        df = pd.DataFrame([], columns=EXCEL_COLS)
        df.to_excel(excel_path, index=False)
        return

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    df = pd.DataFrame(data)
    for c in EXCEL_COLS:
        if c not in df.columns:
            df[c] = ""
    df[EXCEL_COLS].to_excel(excel_path, index=False)
    log_and_print("Shranjen Excel (na koncu).")


def main():
    global _log_file, _global_item_counter, _debug_dir

    if is_ci():
        human_sleep(*STARTUP_JITTER_CI)
    else:
        human_sleep(*STARTUP_JITTER_LOCAL)

    json_path, excel_path, log_path, daily_dir = create_output_paths(SHOP_NAME)
    _debug_dir = daily_dir

    try:
        _log_file = open(log_path, "w", encoding="utf-8")
    except Exception:
        return

    session = build_session()

    log_and_print(f"--- Zagon {SHOP_NAME} ---")
    log_and_print(f"User-Agent (stabilen za ta zagon): {_RUN_UA}")

    # Preflight (da ne kuri časa, če je res challenge)
    first_cat = list(SLOVENIJALES_CATEGORIES.values())[0][0]
    test_url = f"{first_cat}?page=1"
    test_html = get_page_content(session, test_url, referer=BASE_URL)
    if not test_html:
        log_and_print("Slovenijales vrača challenge/verification že na prvem testu. Končujem (debug HTML je v output mapi).")
        write_empty_outputs(json_path, excel_path)
        try:
            if _log_file:
                _log_file.close()
        except Exception:
            pass
        return

    # nadaljuj Zap, če json že obstaja
    if os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                d = json.load(f)
            if isinstance(d, list) and d:
                _global_item_counter = max((int(x.get("Zap", 0)) for x in d if isinstance(x, dict)), default=0)
        except Exception:
            pass

    date_str = datetime.now().strftime("%d/%m/%Y")
    buffer: List[Dict[str, Any]] = []

    try:
        for cat, urls in SLOVENIJALES_CATEGORIES.items():
            log_and_print(f"\n--- {cat} ---")
            for u in urls:
                links = get_product_links_from_category(session, u)
                for link in links:
                    log_and_print(f"    Izdelek: {link}")
                    human_sleep(SLEEP_MIN, SLEEP_MAX)

                    det = extract_product_details(session, link, cat, date_str)
                    if det:
                        buffer.append(det)

                    if len(buffer) >= FLUSH_JSON_EVERY:
                        save_data_append(buffer, json_path)
                        buffer = []

                    human_sleep(0.8, 2.2)

                if buffer:
                    save_data_append(buffer, json_path)
                    buffer = []

                human_sleep(2.0, 6.0)

    except Exception as e:
        log_and_print(f"NAPAKA: {e}")
    finally:
        try:
            if buffer:
                save_data_append(buffer, json_path)
        except Exception:
            pass

        if EXPORT_EXCEL:
            try:
                write_excel_from_json(json_path, excel_path)
            except Exception as e:
                log_and_print(f"NAPAKA pri Excel: {e}")

        try:
            if _log_file:
                _log_file.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
