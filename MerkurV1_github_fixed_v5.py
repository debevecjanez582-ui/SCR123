import os
import re
import json
import time
import random
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
from urllib.parse import urljoin, urlparse, urlencode, urlunparse, parse_qsl

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ============================================================
# MERKUR scraper (GitHub/CI friendly) - refactor v5
# ------------------------------------------------------------
# Namen: čim bolj podobna logika kot OBI/Kalcer:
#  - stabilen User-Agent na run (bolj "naravno")
#  - retry/backoff (429/5xx) + captcha/verification detekcija
#  - "polite scraping": jitter sleep + občasni počitek
#  - JSON checkpoint v batchih, Excel 1x na koncu
#  - cene vedno 2 decimalki (tudi če je format drugačen)
#  - EM normalizacija: če ni v whitelist -> kos
#  - EAN se ohrani (brez validacije dolžine)
#  - Opis: odstrani podvajanje (npr. "X ... X ...")
#  - Dobava: DA/NE na osnovi zaloge po centrih (če jo najdemo)
# ============================================================

SHOP_NAME = "Merkur"
BASE_URL = "https://www.merkur.si"
DDV_RATE = 0.22

# Vedno Excel (1x na koncu)
EXPORT_EXCEL = True

# Tempo (prilagodljivo prek env; privzeto ~5s na request)
SLEEP_MIN = float(os.environ.get("SCRAPE_SLEEP_MIN", "4.0"))
SLEEP_MAX = float(os.environ.get("SCRAPE_SLEEP_MAX", "6.0"))

# Start jitter (da CI ne udari vedno ob isti sekundi)
START_JITTER_CI = (0.5, 3.0)
START_JITTER_LOCAL = (2.0, 12.0)

# checkpoint (JSON) – da ne izgubimo vsega ob prekinitvi
FLUSH_JSON_EVERY = int(os.environ.get("FLUSH_JSON_EVERY", "50"))

# varovalke
MAX_PAGES = int(os.environ.get("MAX_PAGES", "250"))
MAX_BLOCK_RETRIES = int(os.environ.get("MAX_BLOCK_RETRIES", "3"))

# občasni "človeški" počitek
BREAK_EVERY_PRODUCTS = int(os.environ.get("BREAK_EVERY_PRODUCTS", "140"))
BREAK_SLEEP_MIN = float(os.environ.get("BREAK_SLEEP_MIN", "20"))
BREAK_SLEEP_MAX = float(os.environ.get("BREAK_SLEEP_MAX", "90"))

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
]
# stabilen UA na en run
_RUN_UA = random.choice(USER_AGENTS)

# Kategorije za Merkur
MERKUR_CATEGORIES = {
    "Osnovni gradbeni izdelki in les": [
        "https://www.merkur.si/gradnja/osnovni-gradbeni-izdelki-in-les/gradbene-surovine/",
        "https://www.merkur.si/gradnja/osnovni-gradbeni-izdelki-in-les/opazne-plosce-in-elementi/",
        "https://www.merkur.si/gradnja/osnovni-gradbeni-izdelki-in-les/osb-in-lsb-plosce/",
        "https://www.merkur.si/gradnja/osnovni-gradbeni-izdelki-in-les/opeka-prizme/",
        "https://www.merkur.si/gradnja/osnovni-gradbeni-izdelki-in-les/malte-in-ometi/",
        "https://www.merkur.si/gradnja/osnovni-gradbeni-izdelki-in-les/zagan-les-in-letve/",
        "https://www.merkur.si/gradnja/osnovni-gradbeni-izdelki-in-les/lepljenci/",
    ],
    "Termoizolacije": [
        "https://www.merkur.si/gradnja/termoizolacije/stiropor/",
        "https://www.merkur.si/gradnja/termoizolacije/estrudirani-polistiren-xps/",
        "https://www.merkur.si/gradnja/termoizolacije/steklena-volna/",
        "https://www.merkur.si/gradnja/termoizolacije/kamena-volna/",
        "https://www.merkur.si/gradnja/termoizolacije/folije/",
        "https://www.merkur.si/gradnja/termoizolacije/ostalo/",
    ],
    "Hidroizolacije": [
        "https://www.merkur.si/gradnja/hidroizolacije/bitumenski-trakovi-in-premazi/bitumenski-premazi/",
        "https://www.merkur.si/gradnja/hidroizolacije/bitumenski-trakovi-in-premazi/bitumenski-trakovi/",
        "https://www.merkur.si/gradnja/hidroizolacije/cementna-hidroizolacija/mrezica/",
    ],
}

# EM whitelist (če ni v whitelist -> kos)
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
    "os", "os d", "x", "delež", "oc", "op",
}

BASE_EXCEL_COLS = [
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
    "Zaloga po centrih",
]

# Merkur nima fiksnega seznama "centrov" v vseh kategorijah, zato so stolpci dinamični (ostanejo v JSON).
# Excel ima vseeno fiksne osnovne stolpce; zalogo po centrih damo v "Zaloga po centrih" (JSON string).

_log_file = None
_debug_dir = None
_global_item_counter = 0


# -----------------------------
# Logging / sleeps
# -----------------------------
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


# -----------------------------
# Output paths
# -----------------------------
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
    """Shrani HTML, ko naletimo na challenge (da vidiš, kaj server vrača)."""
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


# -----------------------------
# Block detector (FIX: no false-positive on "reCAPTCHA")
# -----------------------------
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
    """
    GET + backoff (429/5xx) + challenge detekcija.
    Če dobimo "block page", počakamo in poskusimo še.
    """
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

    return None


# -----------------------------
# Price / EM helpers
# -----------------------------
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


# -----------------------------
# Data extraction helpers
# -----------------------------
def clean_title_duplicate(title: str) -> str:
    """Če je naslov podvojen (npr. 'X ... X ...'), poskusi očistit."""
    if not title:
        return ""
    t = re.sub(r"\s+", " ", title).strip()
    half = len(t) // 2
    if half > 10 and t[:half].strip() == t[half:].strip():
        return t[:half].strip()
    return t


def extract_ean_raw(soup: BeautifulSoup) -> str:
    txt = soup.get_text(" ", strip=True)
    m = re.search(r"(EAN|GTIN)\s*[:#]?\s*([0-9]{6,20})", txt, flags=re.IGNORECASE)
    return m.group(2).strip() if m else ""


def extract_image_url(soup: BeautifulSoup) -> str:
    og = soup.find("meta", attrs={"property": "og:image"})
    if og and og.get("content"):
        return og["content"].strip()
    img = soup.select_one("img[src]")
    return img.get("src", "").strip() if img else ""


def extract_long_description(soup: BeautifulSoup) -> str:
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        return meta_desc.get("content").strip()

    # poskusi najti daljši opis v tipičnih blokih
    for sel in (".product.attribute.description", "#description", ".product-info-main", ".product.info.detailed"):
        d = soup.select_one(sel)
        if d:
            txt = d.get_text("\n", strip=True)
            if txt and len(txt) > 30:
                return (txt[:8000].rstrip() + "…") if len(txt) > 8000 else txt
    return ""


def extract_price_and_unit(soup: BeautifulSoup) -> Tuple[str, str]:
    """Cena in enota (best-effort)."""
    # različni templati; vzemi prvo najdeno ceno
    for sel in ("span.price", ".price-wrapper .price", ".product-info-price .price"):
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            price = round_price_2dec(el.get_text(" ", strip=True))
            if price:
                # enota iz teksta (€/m2 ipd)
                txt = soup.get_text(" ", strip=True)
                munit = re.search(r"€\s*/\s*([A-Za-z0-9²³]+)", txt)
                unit = normalize_em(munit.group(1)) if munit else "kos"
                return price, unit
    return "", "kos"


def extract_stock_like_merkur(soup: BeautifulSoup) -> Dict[str, int]:
    """
    Merkur ima včasih zalogo po poslovalnicah/prevzemu; v HTML zna biti tekstovno.
    Best-effort: poišče vrstice 'Merkur <lokacija> <številka> kos' ipd.
    """
    txt = soup.get_text("\n", strip=True).replace("\xa0", " ")
    matches = re.findall(r"(Merkur[^\n\r]+?)\s+(\d+)\s+kos", txt, flags=re.IGNORECASE)
    stock = {}
    for name, qty in matches:
        name = re.sub(r"\s+", " ", name).strip()
        try:
            stock[name] = int(qty)
        except Exception:
            pass
    return stock


def extract_delivery_short(soup: BeautifulSoup) -> str:
    txt = soup.get_text(" ", strip=True).replace("\xa0", " ")
    tl = txt.lower()
    if "ni na zalogi" in tl:
        return "NE"
    if "na zalogi" in tl:
        return "DA"
    m = re.search(r"dobavni\s+rok\s*[:\-]?\s*([0-9]+\s*[-–]\s*[0-9]+\s*\w+)", tl)
    return m.group(1).strip() if m else ""


# -----------------------------
# Save helpers
# -----------------------------
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
        df = pd.DataFrame([], columns=BASE_EXCEL_COLS)
        df.to_excel(excel_path, index=False)
        return

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    df = pd.DataFrame(data)
    for c in BASE_EXCEL_COLS:
        if c not in df.columns:
            df[c] = ""
    df[BASE_EXCEL_COLS].to_excel(excel_path, index=False)
    log_and_print("Shranjen Excel (na koncu).")


# -----------------------------
# URL helpers
# -----------------------------
def add_or_replace_query(url: str, params: Dict[str, str]) -> str:
    """Doda/posodobi query parametre v URL."""
    parts = urlparse(url)
    q = dict(parse_qsl(parts.query))
    q.update(params)
    new_query = urlencode(q)
    return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, new_query, parts.fragment))


# -----------------------------
# Listing -> product URLs
# -----------------------------
def get_product_links_from_category(session: requests.Session, category_url: str) -> List[str]:
    links: List[str] = []
    last_first = None

    for page in range(1, MAX_PAGES + 1):
        page_url = add_or_replace_query(category_url, {"p": str(page)}) + "#section-products"
        log_and_print(f"  Stran {page}: {page_url}")

        html = get_page_content(session, page_url, referer=category_url)
        if not html:
            break

        soup = BeautifulSoup(html, "lxml")
        item_container = soup.find("div", class_="list-items")
        if not item_container:
            break

        items = item_container.find_all("div", class_="item")
        if not items:
            break

        first_title = items[0].get_text(" ", strip=True)[:80]
        if page > 1 and last_first and first_title == last_first:
            log_and_print("  Stran se ponavlja. Konec kategorije.")
            break
        last_first = first_title

        for it in items:
            a = it.find("a", href=True)
            if not a:
                continue
            href = a["href"]
            full = href if href.startswith("http") else urljoin(BASE_URL, href)
            # Merkur produkti so praviloma /<slug>/
            if full.startswith(BASE_URL):
                links.append(full)

        # "a.next" včasih obstaja
        if not soup.select_one("a.next"):
            # če ni next, smo verjetno na zadnji strani
            break

    return list(dict.fromkeys(links))


# -----------------------------
# Product details
# -----------------------------
def extract_product_details(session: requests.Session, product_url: str, group_name: str, date_str: str, referer: str) -> Optional[Dict[str, Any]]:
    global _global_item_counter

    html = get_page_content(session, product_url, referer=referer)
    if not html:
        return None

    soup = BeautifulSoup(html, "lxml")
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
        "URL": product_url,
        "SLIKA URL": "",
        "Zaloga po centrih": "",
    }

    h1 = soup.select_one("h1")
    if h1:
        data["Opis"] = clean_title_duplicate(h1.get_text(" ", strip=True))

    data["Opis izdelka"] = extract_long_description(soup)
    data["SLIKA URL"] = extract_image_url(soup)
    data["EAN"] = extract_ean_raw(soup)

    # Cena + enota
    price, unit = extract_price_and_unit(soup)
    if price:
        data["Cena / EM (z DDV)"] = price
        data["Cena / EM (brez DDV)"] = convert_price_to_without_vat(price, DDV_RATE)
    data["EM"] = normalize_em(unit)

    # Koda artikla: pogosto se pojavi v URL kot zadnja številka (npr. ...-134410/)
    m = re.search(r"-(\d{4,})/?$", product_url.strip("/"))
    if m:
        data["Oznaka / naziv"] = m.group(1)

    # Zaloga po centrih (best-effort)
    stock = extract_stock_like_merkur(soup)
    if stock:
        data["Zaloga po centrih"] = json.dumps(stock, ensure_ascii=False)
        data["Dobava"] = "DA" if any(qty > 0 for qty in stock.values()) else "NE"
    else:
        data["Dobava"] = extract_delivery_short(soup)

    # Proizvajalec best-effort
    txt = soup.get_text("\n", strip=True).replace("\xa0", " ")
    mman = re.search(r"Proizvajalec\s*:\s*([^\n\r]+)", txt, flags=re.IGNORECASE)
    if mman:
        data["Proizvajalec"] = mman.group(1).strip()[:250]

    data["EM"] = normalize_em(data.get("EM") or "kos")
    return data


# -----------------------------
# Main
# -----------------------------
def main():
    global _log_file, _global_item_counter, _debug_dir

    if is_ci():
        human_sleep(*START_JITTER_CI)
    else:
        human_sleep(*START_JITTER_LOCAL)

    json_path, excel_path, log_path, daily_dir = create_output_paths(SHOP_NAME)
    _debug_dir = daily_dir

    try:
        _log_file = open(log_path, "w", encoding="utf-8")
    except Exception:
        return

    session = build_session()



    # Warm-up: pridobi osnovne piškotke (cookie consent / session)

    try:

        session.get(BASE_URL, headers={'User-Agent': (_RUN_UA if '_RUN_UA' in globals() else HEADERS.get('User-Agent', 'Mozilla/5.0'))}, timeout=20)

    except Exception:

        pass
    # Preflight: test en seznam (če je challenge, ne kurimo časa po vseh kategorijah)
    test_url = list(MERKUR_CATEGORIES.values())[0][0] + "?p=1#section-products"
    test_html = get_page_content(session, test_url, referer=BASE_URL)
    
