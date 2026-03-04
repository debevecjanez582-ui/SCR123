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
# SLOVENIJALES scraper (GitHub/CI friendly) - refactor v6
# ------------------------------------------------------------
# V6 vključuje:
#  - dobavljivost / zaloga po poslovalnicah (best-effort)
#    * prebere seznam prodajnih centrov iz /prodajni-centri
#    * iz produktne strani poskusi razbrati status/qty za vsako poslovalnico
#    * v JSON/Excel doda:
#        - "Zaloga po poslovalnicah" (JSON string)
#        - stolpce "Zaloga - <poslovalnica>"
#        - "Dobava" = DA/NE (če karkoli >0), sicer fallback
#  - stabilen User-Agent na run
#  - retry/backoff (429/5xx) + BLOCK detekcija (FIXED - brez false positive na "reCAPTCHA")
#  - "polite scraping": jitter sleep + občasni počitek
#  - JSON checkpoint v batchih, Excel 1x na koncu
#  - cene vedno 2 decimalki
#  - EM normalizacija: če ni v whitelist -> kos
#  - EAN pobere (ne validira dolžine)
# ============================================================

SHOP_NAME = "Slovenijales"
BASE_URL = "https://trgovina.slovenijales.si"
DDV_RATE = 0.22

EXPORT_EXCEL = True

# tempo (lahko overridaš z env)
SLEEP_MIN = float(os.environ.get("SCRAPE_SLEEP_MIN", "4.0"))
SLEEP_MAX = float(os.environ.get("SCRAPE_SLEEP_MAX", "6.0"))
BUFFER_FLUSH = int(os.environ.get("BUFFER_FLUSH", "50"))
MAX_PAGES = int(os.environ.get("MAX_PAGES", "250"))

# občasni “počitek”
BREAK_EVERY_PRODUCTS = int(os.environ.get("BREAK_EVERY_PRODUCTS", "140"))
BREAK_SLEEP_RANGE = (
    float(os.environ.get("BREAK_SLEEP_MIN", "20")),
    float(os.environ.get("BREAK_SLEEP_MAX", "90")),
)

# če zaznamo block/captcha večkrat, raje preskočimo URL
MAX_BLOCK_RETRIES = int(os.environ.get("MAX_BLOCK_RETRIES", "3"))

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
]
UA_THIS_RUN = random.choice(USER_AGENTS)

HEADERS = {
    "User-Agent": UA_THIS_RUN,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "sl-SI,sl;q=0.9,en-US;q=0.7,en;q=0.5",
    "Connection": "keep-alive",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
}

# Kategorije za Slovenijales
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

# --- EM whitelist (če ni na seznamu -> kos) ---
_ALLOWED_EM = {
    "ar", "ha",
    "kam", "kg", "km",
    "kwh", "kw", "wat",
    "kpl", "kos", "kos dan", "kos mes",
    "m", "m2", "m3",
    "cm", "kN",
    "km2", "kg/m3", "kg/h", "kg/l",
    "m/dan", "m/h", "m/min", "m/s",
    "m2 dan", "m2 mes",
    "m3/dan", "m3/h", "m3/min", "m3/s", "m3 d",
    "t", "tm",
    "t/dan", "t/h", "t/let",
    "h", "min", "s",
    "lit/dan", "lit/h", "lit/min", "lit/s",
    "L",
    "par", "pal", "sto", "skl", "del", "ključ",
    "os", "os d",
    "x",
    "delež",
    "oc", "op",
}

# ====== Poslovalnice ======
SLOV_STORE_FALLBACK = [
    "Slovenijales Maribor / Hoče",
    "Slovenijales Celje",
    "Hobby Ljubljana Črnuče",
    "Hobby Ljubljana Vižmarje",
    "Slovenijales Murska Sobota",
    "Slovenijales Nova Gorica",
    "Slovenijales Koper",
    "Slovenijales Kranj",
    "Slovenijales Novo mesto",
]

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
    "Zaloga po poslovalnicah",
]

_log_file = None
_global_item_counter = 0
_debug_dir = None


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


def human_sleep(min_s: float = None, max_s: float = None) -> None:
    mn = SLEEP_MIN if min_s is None else min_s
    mx = SLEEP_MAX if max_s is None else max_s
    time.sleep(random.uniform(mn, mx))


# ==========================
# BLOCK DETECTOR (FIXED)
# ==========================
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


def write_empty_outputs(json_path: str, excel_path: str, store_cols: List[str]) -> None:
    try:
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump([], f, ensure_ascii=False, indent=2)
    except Exception:
        pass
    try:
        cols = BASE_EXCEL_COLS + store_cols
        df = pd.DataFrame([], columns=cols)
        df.to_excel(excel_path, index=False)
    except Exception:
        pass


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
        human_sleep()
        try:
            resp = session.get(url, headers=headers, timeout=25)

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
                log_and_print(f"[BLOCK] challenge @ {url} -> sleep {wait:.1f}s")
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


# -----------------------------
# Parsing helpers
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


def parse_price_any(text: str) -> str:
    if not text:
        return ""
    m = re.search(r"(\d{1,3}(?:\.\d{3})*,\d{2}|\d+(?:[.,]\d+)?)", text)
    return m.group(1).strip() if m else ""


# -----------------------------
# Save/load helpers
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


def write_excel_from_json(json_path: str, excel_path: str, store_cols: List[str]) -> None:
    if not os.path.exists(json_path):
        return

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    df = pd.DataFrame(data)
    cols = BASE_EXCEL_COLS + store_cols
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    df[cols].to_excel(excel_path, index=False)
    log_and_print("Shranjen Excel (na koncu).")


# -----------------------------
# Stores (prodajni centri)
# -----------------------------
def fetch_store_order(session: requests.Session) -> List[str]:
    url = f"{BASE_URL}/prodajni-centri"
    html = get_page_content(session, url, referer=BASE_URL)
    if not html:
        return SLOV_STORE_FALLBACK

    soup = BeautifulSoup(html, "html.parser")

    candidates: List[str] = []
    for tag in soup.find_all(["h2", "h3", "h4", "strong"]):
        t = tag.get_text(" ", strip=True)
        if not t:
            continue
        tl = t.lower()
        if tl.startswith("slovenijales") or tl.startswith("hobby") or "jelovica" in tl:
            if len(t) <= 80:
                candidates.append(t)

    out: List[str] = []
    for t in candidates:
        if t not in out:
            out.append(t)

    return out or SLOV_STORE_FALLBACK


def _normalize_text(s: str) -> str:
    s = (s or "").lower()
    s = s.replace("č", "c").replace("š", "s").replace("ž", "z")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _store_aliases(store_name: str) -> List[str]:
    s = store_name.strip()
    aliases = [s]
    parts = re.split(r"[/,]", s)
    for p in parts:
        p = p.strip()
        if p and p not in aliases:
            aliases.append(p)
    no_pref = re.sub(r"^(Slovenijales|Hobby)\s+", "", s, flags=re.IGNORECASE).strip()
    if no_pref and no_pref not in aliases:
        aliases.append(no_pref)
    for w in re.split(r"\s+", no_pref):
        if len(w) >= 4 and w[0].isalpha() and w not in aliases:
            aliases.append(w)
    return aliases


def extract_store_stock_from_product_page(soup: BeautifulSoup, store_order: List[str]) -> Dict[str, int]:
    txt = soup.get_text("\n", strip=True).replace("\xa0", " ")
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in txt.splitlines() if ln.strip()]
    big = "\n".join(lines)
    big_norm = _normalize_text(big)

    stock: Dict[str, int] = {}

    for store in store_order:
        aliases = _store_aliases(store)
        matched = False

        for a in aliases:
            a_norm = _normalize_text(a)
            if not a_norm or len(a_norm) < 4:
                continue
            if a_norm not in big_norm:
                continue

            idx = big_norm.find(a_norm)
            if idx < 0:
                continue
            win = big_norm[max(0, idx - 120): idx + 220]

            if "ni na zalogi" in win or "ni zalogi" in win or "ni na voljo" in win:
                stock[store] = 0
                matched = True
                break

            if "na zalogi" in win or "na voljo" in win:
                mqty = re.search(r"(\d{1,4})\s*(kos|kom|komad|m2|m3)?", win)
                if mqty:
                    try:
                        q = int(mqty.group(1))
                        stock[store] = max(stock.get(store, 0), q if 0 <= q <= 999 else 1)
                    except Exception:
                        stock[store] = max(stock.get(store, 0), 1)
                else:
                    stock[store] = max(stock.get(store, 0), 1)
                matched = True
                break

            matched = True

        if not matched:
            continue

    return {k: v for k, v in stock.items() if isinstance(v, int) and v >= 0}


# -----------------------------
# Listing -> product URLs
# -----------------------------
def get_product_links_from_category(session: requests.Session, category_url: str) -> List[str]:
    links: List[str] = []
    last_first_url = None

    for page in range(1, MAX_PAGES + 1):
        page_url = f"{category_url}?page={page}"
        log_and_print(f"  Stran {page}: {page_url}")

        html = get_page_content(session, page_url, referer=category_url)
        if not html:
            break

        soup = BeautifulSoup(html, "html.parser")
        products = soup.select('div.single-product.border-left[itemscope]')
        if not products:
            break

        first_a = products[0].select_one(".product-img a")
        first_url = urljoin(BASE_URL, first_a.get("href")) if first_a and first_a.get("href") else None
        if page > 1 and first_url and last_first_url and first_url == last_first_url:
            log_and_print("  Stran se ponavlja. Konec kategorije.")
            break
        last_first_url = first_url

        for p in products:
            a = p.select_one(".product-img a")
            if a and a.get("href"):
                links.append(urljoin(BASE_URL, a["href"]))

        if not soup.select_one('ul.pagination a[aria-label="Naprej"]'):
            break

    return list(dict.fromkeys(links))


# -----------------------------
# Product details
# -----------------------------
def extract_delivery_short(soup: BeautifulSoup) -> str:
    txt = soup.get_text(" ", strip=True).replace("\xa0", " ").lower()
    if "na zalogi" in txt:
        return "DA"
    if "ni na zalogi" in txt or "ni zaloge" in txt:
        return "NE"
    return ""


def extract_long_description(soup: BeautifulSoup) -> str:
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        return meta_desc.get("content").strip()
    return ""


def extract_ean_raw(soup: BeautifulSoup) -> str:
    for prop in ("gtin13", "gtin14", "gtin12", "gtin", "ean"):
        m = soup.find("meta", attrs={"itemprop": prop})
        if m and m.get("content"):
            return m.get("content").strip()
    txt = soup.get_text(" ", strip=True)
    m2 = re.search(r"(EAN|GTIN)\s*[:#]?\s*([0-9]{6,20})", txt, flags=re.IGNORECASE)
    return m2.group(2).strip() if m2 else ""


def extract_product_details(
    session: requests.Session,
    product_url: str,
    category_name: str,
    date_str: str,
    referer: str,
    store_order: List[str],
) -> Optional[Dict[str, Any]]:
    global _global_item_counter

    html = get_page_content(session, product_url, referer=referer)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    _global_item_counter += 1

    data: Dict[str, Any] = {
        "Skupina": category_name,
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
        "Zaloga po poslovalnicah": "",
    }

    h1 = soup.select_one('h1[itemprop="name"]') or soup.select_one("h1")
    if h1:
        data["Opis"] = h1.get_text(" ", strip=True)

    sku = soup.select_one('meta[itemprop="sku"]')
    if sku and sku.get("content"):
        data["Oznaka / naziv"] = sku.get("content").strip()

    data["EAN"] = extract_ean_raw(soup)
    data["Opis izdelka"] = extract_long_description(soup)

    new_p = soup.select_one(".product-info-price span.new")
    old_p = soup.select_one(".product-info-price span.old")

    if new_p:
        p_new = parse_price_any(new_p.get_text(" ", strip=True))
        if old_p:
            p_old = parse_price_any(old_p.get_text(" ", strip=True))
            data["Cena / EM (z DDV)"] = round_price_2dec(p_old)
            data["Akcijska cena / EM (z DDV)"] = round_price_2dec(p_new)
        else:
            data["Cena / EM (z DDV)"] = round_price_2dec(p_new)

    data["Cena / EM (brez DDV)"] = convert_price_to_without_vat(data.get("Cena / EM (z DDV)"), DDV_RATE)
    data["Akcijska cena / EM (brez DDV)"] = convert_price_to_without_vat(data.get("Akcijska cena / EM (z DDV)"), DDV_RATE)
    data["EM"] = normalize_em(data.get("EM") or "kos")

    img = soup.select_one(".flexslider .slides img") or soup.select_one('img[itemprop="image"]') or soup.select_one("img[src]")
    if img and img.get("src"):
        data["SLIKA URL"] = urljoin(BASE_URL, img.get("src"))

    stock = extract_store_stock_from_product_page(soup, store_order)
    if stock:
        data["Zaloga po poslovalnicah"] = json.dumps(stock, ensure_ascii=False)
        data["Dobava"] = "DA" if any(q > 0 for q in stock.values()) else "NE"
        for s in store_order:
            data[f"Zaloga - {s}"] = stock.get(s, 0)
    else:
        data["Dobava"] = extract_delivery_short(soup)
        for s in store_order:
            data[f"Zaloga - {s}"] = ""

    return data


def main():
    global _log_file, _global_item_counter, _debug_dir

    if os.environ.get("GITHUB_ACTIONS", "").lower() == "true":
        time.sleep(random.uniform(0.5, 2.5))
    else:
        time.sleep(random.uniform(2.0, 12.0))

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
    # Preflight
    test_url = f"{list(SLOVENIJALES_CATEGORIES.values())[0][0]}?page=1"
    test_html = get_page_content(session, test_url, referer=BASE_URL)
    
