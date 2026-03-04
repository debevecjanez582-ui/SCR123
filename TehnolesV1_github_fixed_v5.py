import os
import re
import json
import time
import random
from datetime import datetime
from typing import Optional, Dict, Any, List
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ============================================================
# TEHNOLES scraper (GitHub/CI friendly) - stable v6
# ============================================================

SHOP_NAME = "Tehnoles"
BASE_URL = "https://www.tehnoles.si"
DDV_RATE = 0.22

EXPORT_EXCEL = True

# Categories
TEHNOLES_CATEGORIES = {
    "Gradbeni material": [
        "https://www.tehnoles.si/gradbeni-material-c-28.aspx",
        "https://www.tehnoles.si/barve-laki-in-premazi-c-31.aspx",
        "https://www.tehnoles.si/lepila-in-kiti-c-32.aspx",
        "https://www.tehnoles.si/izolacije-c-48.aspx",
        "https://www.tehnoles.si/suhomontazni-material-c-17.aspx",
        "https://www.tehnoles.si/kasetni-stropi-c-84.aspx",
        "https://www.tehnoles.si/delovna-zascitna-sredstva-c-69.aspx",
        "https://www.tehnoles.si/delovni-stroji-c-160.aspx",
        "https://www.tehnoles.si/vodovod-c-151.aspx",
    ],
    "Orodje": [
        "https://www.tehnoles.si/rocno-orodje-c-41.aspx",
        "https://www.tehnoles.si/elektricno-orodje-c-40.aspx",
    ],
}

# polite scraping
SLEEP_MIN = float(os.environ.get("SCRAPE_SLEEP_MIN", "4.0"))
SLEEP_MAX = float(os.environ.get("SCRAPE_SLEEP_MAX", "6.0"))
STARTUP_JITTER_CI = (0.5, 3.0)
STARTUP_JITTER_LOCAL = (2.0, 12.0)

BUFFER_FLUSH = int(os.environ.get("BUFFER_FLUSH", "50"))
MAX_PAGES = int(os.environ.get("MAX_PAGES", "250"))
MAX_BLOCK_RETRIES = int(os.environ.get("MAX_BLOCK_RETRIES", "3"))

# občasni počitek
BREAK_EVERY_PRODUCTS = int(os.environ.get("BREAK_EVERY_PRODUCTS", "160"))
BREAK_SLEEP_MIN = float(os.environ.get("BREAK_SLEEP_MIN", "20"))
BREAK_SLEEP_MAX = float(os.environ.get("BREAK_SLEEP_MAX", "90"))

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
]
UA_THIS_RUN = os.environ.get("SCRAPE_UA") or random.choice(USER_AGENTS)

HEADERS = {
    "User-Agent": UA_THIS_RUN,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "sl-SI,sl;q=0.9,en-US;q=0.7,en;q=0.5",
    "Connection": "keep-alive",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
}

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
    """OUTPUT_DIR/Ceniki_Scraping/<SHOP>/<YYYY-MM-DD>/..."""
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
    """Best-effort detekcija *prave* challenge/captcha strani.

    Namen: zmanjšati false-positive.
    - NE flaggamo strani samo zato, ker vsebujejo 'captcha' ali 'recaptcha' skripte.
    - Flag je samo pri zelo značilnih podpisih (Cloudflare/PerimeterX/DataDome/Incapsula...).

    Če je trgovina res prešla na obvezno JS-verifikacijo, bo ta funkcija še vedno ujela te strani.
    """
    if not html:
        return False

    t = html.lower()

    # Cloudflare
    if (
        '/cdn-cgi/challenge-platform' in t
        or 'cf-chl-' in t
        or '__cf_chl' in t
        or 'cloudflare ray id' in t
    ):
        return True

    # PerimeterX
    if ('perimeterx' in t) or ('px-captcha' in t) or ('px-block' in t) or ('_pxappid' in t):
        return True

    # DataDome (običajno vsebuje besedo datadome + captcha/blocked/verify)
    if 'datadome' in t and any(x in t for x in ('captcha', 'blocked', 'verify', 'verifying')):
        return True

    # Incapsula
    if 'incapsula' in t and any(x in t for x in ('request unsuccessful', 'visid_incap', 'incap_ses')):
        return True

    # Akamai (tipično jasno napiše)
    if 'akamai bot manager' in t:
        return True

    # Genericne WAF challenge strani
    if any(x in t for x in ('verifying you are human', 'verify you are human', 'one moment, please', 'attention required')):
        return True

    # Če je captcha widget + challenge kontekst
    if re.search(r"(hcaptcha|cf-turnstile|g-recaptcha|data-sitekey)", t):
        if any(x in t for x in ('verify', 'verifying', 'challenge', 'access denied', 'blocked')):
            return True

    return False

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


def get_page_content(session: requests.Session, url: str, referer: Optional[str] = None) -> Optional[str]:
    headers = dict(HEADERS)
    if referer:
        headers["Referer"] = referer

    for attempt in range(1, MAX_BLOCK_RETRIES + 1):
        human_sleep(SLEEP_MIN, SLEEP_MAX)
        try:
            resp = session.get(url, headers=headers, timeout=30)

            if resp.status_code == 403:
                wait = min(180, 15 * attempt + random.uniform(0, 15))
                log_and_print(f"HTTP 403 @ {url} -> sleep {wait:.1f}s")
                time.sleep(wait)
                continue

            if resp.status_code == 429:
                ra = resp.headers.get("Retry-After")
                wait = int(ra) if (ra and ra.isdigit()) else random.randint(30, 120)
                log_and_print(f"HTTP 429 -> backoff {wait}s: {url}")
                time.sleep(wait)
                continue

            if resp.status_code in (500, 502, 503, 504):
                wait = random.randint(10, 60)
                log_and_print(f"HTTP {resp.status_code} -> backoff {wait}s: {url}")
                time.sleep(wait)
                continue

            if not resp.ok:
                log_and_print(f"HTTP {resp.status_code} @ {url}")
                return None

            html = resp.text or ""
            if is_block_page(html):
                wait = min(300, 30 * attempt + random.uniform(0, 30))
                log_and_print(f"[BLOCK] verification/captcha @ {url} -> sleep {wait:.1f}s")
                save_debug_html("block", url, html)
                time.sleep(wait)
                continue

            return html

        except requests.RequestException as e:
            wait = min(90, 5 * attempt + random.uniform(0, 10))
            log_and_print(f"Request error @ {url}: {e} -> sleep {wait:.1f}s")
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


def extract_long_description(soup: BeautifulSoup) -> str:
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        return meta_desc.get("content").strip()
    # fallback: vsebina produkta
    block = soup.select_one(".product-description") or soup.select_one("#description") or soup.select_one(".productInfo")
    if block:
        txt = block.get_text("\n", strip=True)
        if txt and len(txt) > 30:
            return (txt[:8000].rstrip() + "…") if len(txt) > 8000 else txt
    return ""


def extract_image_url(soup: BeautifulSoup, product_url: str) -> str:
    og = soup.find("meta", attrs={"property": "og:image"})
    if og and og.get("content"):
        return urljoin(BASE_URL, og.get("content").strip())
    img = soup.select_one("img[src]")
    if img and img.get("src"):
        return urljoin(BASE_URL, img.get("src").strip())
    # fallback po patternu (Tehnoles ima /images/Product/large/<id>.jpg)
    m = re.search(r"/images/Product/large/\d+\.(jpg|png|webp)", str(soup), flags=re.IGNORECASE)
    if m:
        return urljoin(BASE_URL, m.group(0))
    return ""


def extract_price(soup: BeautifulSoup) -> str:
    # Tehnoles pogosto uporablja span.priceColor ali podobno
    for sel in ("span.productSpecialPrice", "span.priceColor", ".price", "span[itemprop='price']"):
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            p = round_price_2dec(el.get_text(" ", strip=True))
            if p:
                return p
    return ""


def extract_delivery(soup: BeautifulSoup) -> str:
    txt = soup.get_text(" ", strip=True).lower()
    if "na zalogi" in txt:
        return "DA"
    if "ni na zalogi" in txt or "ni zaloge" in txt:
        return "NE"
    m = re.search(r"\b(dobavni rok|dobava)\b\s*[:\-]?\s*([0-9]+\s*[-–]\s*[0-9]+\s*\w+)", txt)
    return m.group(2).strip() if m else ""


def get_product_links_from_category(session: requests.Session, category_url: str) -> List[str]:
    links: List[str] = []
    last_first = None

    for page in range(1, MAX_PAGES + 1):
        url = f"{category_url}?pagenum={page}"
        log_and_print(f"  Stran {page}: {url}")

        html = get_page_content(session, url, referer=category_url)
        if not html:
            break

        soup = BeautifulSoup(html, "html.parser")
        products = soup.select("li.wrapper_prods.category")
        if not products:
            break

        # anti-loop
        first_a = products[0].select_one(".name a")
        first_href = first_a.get("href") if first_a else None
        if page > 1 and first_href and last_first and first_href == last_first:
            log_and_print("  Stran se ponavlja. Konec kategorije.")
            break
        last_first = first_href

        for item in products:
            a = item.select_one(".name a")
            if a and a.get("href"):
                full = urljoin(BASE_URL, a["href"])
                links.append(full)

        # pager next/prev je ponavadi ena klasa; če ni, break
        if not soup.select_one("a.PagerPrevNextLink"):
            break

    return list(dict.fromkeys(links))


def extract_product_details(session: requests.Session, url: str, group_name: str, date_str: str, referer: str) -> Optional[Dict[str, Any]]:
    global _global_item_counter

    html = get_page_content(session, url, referer=referer)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    _global_item_counter += 1

    data: Dict[str, Any] = {
        "Skupina": group_name,
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
    }

    h1 = soup.select_one("h1.productInfo") or soup.select_one("h1")
    if h1:
        data["Opis"] = h1.get_text(" ", strip=True)

    # ident / šifra artikla: Tehnoles pogosto ima številko v URL parametru "-p-XXXXX.aspx"
    m = re.search(r"-p-(\d+)\.aspx", url, flags=re.IGNORECASE)
    if m:
        data["Oznaka / naziv"] = m.group(1)

    # EAN raw
    data["EAN"] = extract_ean_raw(soup)

    # long description
    data["Opis izdelka"] = extract_long_description(soup)

    # price
    price = extract_price(soup)
    data["Cena / EM (z DDV)"] = price
    data["Cena / EM (brez DDV)"] = convert_price_to_without_vat(price, DDV_RATE)

    # EM: best-effort iz teksta (npr. "... 4m2/pkt ..." -> m2)
    title = (data["Opis"] or "").lower()
    if " m2" in title or " m²" in title or "m2/" in title or "m²/" in title:
        data["EM"] = "m2"
    else:
        data["EM"] = normalize_em(data.get("EM") or "kos")

    # image
    data["SLIKA URL"] = extract_image_url(soup, url)

    # delivery
    data["Dobava"] = extract_delivery(soup)

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
    global _log_file, _debug_dir, _global_item_counter

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

    log_and_print(f"--- Zagon {SHOP_NAME} ---")
    log_and_print(f"UA: {UA_THIS_RUN}")
    log_and_print(f"SLEEP=[{SLEEP_MIN},{SLEEP_MAX}] BUFFER_FLUSH={BUFFER_FLUSH} EXCEL(end)={EXPORT_EXCEL} MAX_PAGES={MAX_PAGES}")

    session = build_session()



    # Warm-up: pridobi osnovne piškotke (cookie consent / session)

    try:

        session.get(BASE_URL, headers={'User-Agent': (_RUN_UA if '_RUN_UA' in globals() else HEADERS.get('User-Agent', 'Mozilla/5.0'))}, timeout=20)

    except Exception:

        pass
    # Preflight (da ne kuri časa)
    test_url = f"{list(TEHNOLES_CATEGORIES.values())[0][0]}?pagenum=1"
    test_html = get_page_content(session, test_url, referer=BASE_URL)
    
