import os
import re
import json
import time
import random
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
from urllib.parse import urljoin

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ============================================================
# TEHNOLES scraper (GitHub/CI friendly) - refactor v4
# ------------------------------------------------------------
# Usklajeno z logiko OBI/Kalcer:
#  - stabilen User-Agent na run
#  - retry/backoff (429/5xx) + captcha/verification detekcija
#  - "polite scraping": jitter sleep + občasni počitek
#  - JSON checkpoint v batchih, Excel 1x na koncu
#  - cene vedno 2 decimalki
#  - EM normalizacija: če ni v whitelist -> kos
#  - EAN pobere (ne validira dolžine)
#  - Dobava: best-effort (DA/NE) iz "na zalogi/ni na zalogi"/dobavni rok
# ============================================================

SHOP_NAME = "Tehnoles"
BASE_URL = "https://www.tehnoles.si"
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

# Kategorije za Tehnoles
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
]

_log_file = None
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


def human_sleep(min_s: float = None, max_s: float = None) -> None:
    mn = SLEEP_MIN if min_s is None else min_s
    mx = SLEEP_MAX if max_s is None else max_s
    time.sleep(random.uniform(mn, mx))


def is_block_page(html: str) -> bool:
    if not html:
        return False
    t = html.lower()
    needles = [
        "captcha",
        "verifying you are human",
        "request is being verified",
        "access denied",
        "cloudflare",
        "one moment, please",
    ]
    return any(n in t for n in needles)


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
    return json_path, excel_path, log_path


# -----------------------------
# Networking
# -----------------------------
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

            if resp.status_code in (500, 502, 503, 504):
                wait = random.randint(10, 60)
                log_and_print(f"HTTP {resp.status_code} -> backoff {wait}s: {url}")
                time.sleep(wait)

            if not resp.ok:
                log_and_print(f"HTTP {resp.status_code} @ {url}")
                return None

            html = resp.text or ""
            if is_block_page(html):
                wait = min(300, 30 * attempt + random.uniform(0, 30))
                log_and_print(f"[BLOCK] verification/captcha @ {url} -> sleep {wait:.1f}s")
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
# Parsing helpers (cena / EM)
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


def extract_brand_from_text(soup: BeautifulSoup) -> str:
    """Best-effort proizvajalec iz besedila (npr. 'Proizvajalec: AUSTROTHERM')."""
    txt = soup.get_text("\n", strip=True).replace("\xa0", " ")
    m = re.search(r"Proizvajalec\s*:\s*([^\n\r]+)", txt, flags=re.IGNORECASE)
    if m:
        val = m.group(1).strip()
        # odreži, če se nadaljuje z 'Enota mere' ipd.
        val = re.split(r"\b(Enota\s+mere|Zaloga)\b", val, flags=re.IGNORECASE)[0].strip()
        return val
    return ""


def extract_prices_from_text(soup: BeautifulSoup) -> Dict[str, str]:
    """Best-effort: prebere cene iz tekstovnih vrstic na produktni strani.

    Tehnoles pogosto izpisuje:
      - 'Vaša cena z DDV:3,3896 €/M2'
      - 'Najnižja cena zadnjih 30 dni:6,4562 €/M2'
      - ali 'Prejšnja cena: ...' + 'Vaša cena: ...'
    Vrne dict z ključi:
      - price (redna) in special (akcijska), oba že zaokrožena na 2 dec.
    """
    txt = soup.get_text("\n", strip=True).replace("\xa0", " ")
    # primarne tarče
    your = re.search(r"Va\s*ša\s*cena\s*(?:z\s*DDV)?\s*:\s*([0-9]+[\.,][0-9]+)", txt, flags=re.IGNORECASE)
    lowest30 = re.search(r"Najni\s*žja\s*cena\s+zadnjih\s+30\s+dni\s*:\s*([0-9]+[\.,][0-9]+)", txt, flags=re.IGNORECASE)
    prev = re.search(r"Prej\s*šnja\s*cena\s*:\s*([0-9]+[\.,][0-9]+)", txt, flags=re.IGNORECASE)
    # včasih: 'Cena z DDV:'
    price_ddv = re.search(r"Cena\s*z\s*DDV\s*:\s*([0-9]+[\.,][0-9]+)", txt, flags=re.IGNORECASE)

    your_p = round_price_2dec(your.group(1)) if your else ""
    prev_p = round_price_2dec(prev.group(1)) if prev else ""
    low_p = round_price_2dec(lowest30.group(1)) if lowest30 else ""
    ddv_p = round_price_2dec(price_ddv.group(1)) if price_ddv else ""

    # odločitev:
    # - če imamo your in (prev ali lowest30) in je večje -> your = akcijska, drugo = redna
    def fval(s: str):
        try:
            return float(s.replace(".", "").replace(",", "."))
        except Exception:
            return None

    out = {"price": "", "special": ""}

    cand_regular = prev_p or low_p or ddv_p
    if your_p and cand_regular:
        vy = fval(your_p); vr = fval(cand_regular)
        if vy is not None and vr is not None and vr > vy:
            out["price"] = cand_regular
            out["special"] = your_p
            return out

    # če je samo your -> vzemi kot redno (brez akcije)
    if your_p:
        out["price"] = your_p
        return out

    # fallback: redna
    if cand_regular:
        out["price"] = cand_regular
    return out


# -----------------------------
# Save/load helpers
# -----------------------------
def _item_key(item: dict) -> str:
    return str(item.get("URL") or "")


def save_data_append(new_data: List[Dict[str, Any]], json_path: str) -> None:
    """JSON shranjujemo v batchih; dedupe po URL."""
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
    """Excel naredimo 1x na koncu."""
    if not os.path.exists(json_path):
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
# Product link collection (category pages)
# -----------------------------
def get_product_links_from_category(session: requests.Session, category_url: str) -> List[str]:
    """
    Tehnoles uporablja pagenum=N.
    Vrnemo unique seznam produktnih URL (polni URL).
    """
    links: List[str] = []
    last_first_url = None

    for page in range(1, MAX_PAGES + 1):
        page_url = f"{category_url}?pagenum={page}"
        log_and_print(f"  Stran {page}: {page_url}")

        html = get_page_content(session, page_url, referer=category_url)
        if not html:
            break

        soup = BeautifulSoup(html, "html.parser")
        products = soup.select("li.wrapper_prods.category")
        if not products:
            break

        first_a = products[0].select_one(".name a")
        first_url = urljoin(BASE_URL, first_a.get("href")) if first_a and first_a.get("href") else None
        if page > 1 and first_url and last_first_url and first_url == last_first_url:
            log_and_print("  Stran se ponavlja. Konec kategorije.")
            break
        last_first_url = first_url

        for item in products:
            a = item.select_one(".name a")
            if a and a.get("href"):
                links.append(urljoin(BASE_URL, a["href"]))

        # če ni "next" linka, pogosto pomeni konec; ampak strani včasih imajo vedno pager,
        # zato vseeno raje gledamo "ponavljanje" zgoraj
        if not soup.select_one("a.PagerPrevNextLink"):
            break

    return list(dict.fromkeys(links))


def extract_delivery_short(soup: BeautifulSoup) -> str:
    """Dobava: best-effort (DA/NE ali dobavni rok)."""
    txt = soup.get_text("\n", strip=True).replace("\xa0", " ")
    tl = txt.lower()

    if "na zalogi" in tl:
        return "DA"
    if "ni na zalogi" in tl or "trenutno ni na zalogi" in tl:
        return "NE"

    m = re.search(r"dobavni\s+rok\s*[:\-]?\s*([0-9]+\s*[-–]\s*[0-9]+\s*\w+)", tl, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()

    return ""


def extract_long_description(soup: BeautifulSoup) -> str:
    """Opis izdelka: poskusi iz metadescription ali iz bloka opisa (heuristika)."""
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        return meta_desc.get("content").strip()

    # fallback: celoten tekst brez nav
    return ""


def extract_ean_raw(soup: BeautifulSoup) -> str:
    """EAN/GTIN: best-effort, brez validacije dolžine."""
    for prop in ("gtin13", "gtin14", "gtin12", "gtin", "ean"):
        m = soup.find("meta", attrs={"itemprop": prop})
        if m and m.get("content"):
            return m.get("content").strip()

    txt = soup.get_text(" ", strip=True)
    m2 = re.search(r"(EAN|GTIN)\s*[:#]?\s*([0-9]{6,20})", txt, flags=re.IGNORECASE)
    return m2.group(2).strip() if m2 else ""


def extract_product_details(session: requests.Session, product_url: str, group_name: str, date_str: str, referer: str) -> Optional[Dict[str, Any]]:
    """Produktna stran -> 1 zapis."""
    global _global_item_counter

    html = get_page_content(session, product_url, referer=referer)
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
        "URL": product_url,
        "SLIKA URL": "",
    }

    # naslov
    h1 = soup.select_one("h1.productInfo") or soup.select_one("h1")
    if h1:
        data["Opis"] = h1.get_text(" ", strip=True)

    # opis izdelka
    data["Opis izdelka"] = extract_long_description(soup)

    # šifra / ident / enota (Tehnoles ima podobno tabelo kot Kalcer)
    for row in soup.select(".listing.stockMargin tr"):
        tds = row.select("td")
        if len(tds) != 2:
            continue
        k = tds[0].get_text(" ", strip=True).lower()
        v = tds[1].get_text(" ", strip=True).strip()
        if "ident" in k:
            data["Oznaka / naziv"] = v
        elif "enota mere" in k:
            data["EM"] = normalize_em(v)
        elif "ean" in k or "gtin" in k:
            data["EAN"] = v

    # ean fallback
    if not data["EAN"]:
        data["EAN"] = extract_ean_raw(soup)

    # dobava
    data["Dobava"] = extract_delivery_short(soup)

    # proizvajalec (če ga ni v tabeli, ga poberemo iz tekstovne vrstice 'Proizvajalec: ...')
    if not data.get("Proizvajalec"):
        data["Proizvajalec"] = extract_brand_from_text(soup)

    # cene:
    # 1) najprej poskusi iz eksplicitnih vrstic ('Vaša cena z DDV', 'Prejšnja cena', 'Najnižja cena zadnjih 30 dni', ...)
    prices = extract_prices_from_text(soup)
    if prices.get("price"):
        data["Cena / EM (z DDV)"] = prices["price"]
        data["Cena / EM (brez DDV)"] = convert_price_to_without_vat(data["Cena / EM (z DDV)"], DDV_RATE)
    if prices.get("special"):
        data["Akcijska cena / EM (z DDV)"] = prices["special"]
        data["Akcijska cena / EM (brez DDV)"] = convert_price_to_without_vat(data["Akcijska cena / EM (z DDV)"], DDV_RATE)

    # 2) fallback: stari selektorji (če zgornje ne najde nič)
    if not data.get("Cena / EM (z DDV)"):
        p = soup.select_one("span.productSpecialPrice") or soup.select_one("span.priceColor") or soup.select_one(".priceColor")
        if p:
            price_raw = parse_price_any(p.get_text(" ", strip=True))
            data["Cena / EM (z DDV)"] = round_price_2dec(price_raw)
            data["Cena / EM (brez DDV)"] = convert_price_to_without_vat(data["Cena / EM (z DDV)"], DDV_RATE)

    # slika

    # na Tehnoles je pogosto: https://www.tehnoles.si/images/Product/large/xxxx.jpg
    img = soup.select_one('img[src*="/images/Product/large/"]') or soup.select_one("img[src]")
    if img and img.get("src"):
        data["SLIKA URL"] = urljoin(BASE_URL, img.get("src"))

    # proizvajalec (best-effort): pogosto je v opisu/nazivu; če obstaja meta og:brand?
    # (pustimo prazno, ker ni konsistentno)

    # EM normalizacija fallback
    data["EM"] = normalize_em(data.get("EM") or "kos")

    return data


# -----------------------------
# Main
# -----------------------------
def main():
    global _log_file, _global_item_counter

    # start jitter (CI manj, lokalno več)
    if os.environ.get("GITHUB_ACTIONS", "").lower() == "true":
        time.sleep(random.uniform(0.5, 2.5))
    else:
        time.sleep(random.uniform(2.0, 12.0))

    json_path, excel_path, log_path = create_output_paths(SHOP_NAME)
    try:
        _log_file = open(log_path, "w", encoding="utf-8")
    except Exception:
        return

    log_and_print(f"--- Zagon {SHOP_NAME} ---")
    log_and_print(f"UA: {UA_THIS_RUN}")
    log_and_print(f"SLEEP=[{SLEEP_MIN},{SLEEP_MAX}] BUFFER_FLUSH={BUFFER_FLUSH} EXCEL(end)={EXPORT_EXCEL} MAX_PAGES={MAX_PAGES}")

    session = build_session()

    # resume: če json že obstaja, nadaljuj števec in preskoči URL-je
    existing_urls = set()
    if os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                d = json.load(f)
            if isinstance(d, list) and d:
                for x in d:
                    if isinstance(x, dict) and x.get("URL"):
                        existing_urls.add(x["URL"])
                _global_item_counter = max((int(x.get("Zap", 0)) for x in d if isinstance(x, dict)), default=0)
        except Exception:
            pass

    date_str = datetime.now().strftime("%d/%m/%Y")
    buffer: List[Dict[str, Any]] = []
    processed_in_run = 0

    try:
        for cat, urls in TEHNOLES_CATEGORIES.items():
            log_and_print(f"\n=== {cat} ===")

            for category_url in urls:
                # podkategorija (bolj uporabno kot samo "Gradbeni material")
                sub_name = category_url.split("/")[-1].split("-c-")[0].strip()
                group_name = sub_name if sub_name else cat
                log_and_print(f"\n-- Podkategorija: {group_name}")

                product_urls = get_product_links_from_category(session, category_url)

                for product_url in product_urls:
                    if product_url in existing_urls:
                        continue

                    log_and_print(f"    Izdelek: {product_url}")
                    human_sleep()  # "think time"

                    details = extract_product_details(
                        session=session,
                        product_url=product_url,
                        group_name=group_name,
                        date_str=date_str,
                        referer=category_url,
                    )
                    if details:
                        buffer.append(details)
                        existing_urls.add(product_url)
                        processed_in_run += 1

                    if len(buffer) >= BUFFER_FLUSH:
                        save_data_append(buffer, json_path)
                        buffer = []

                    if processed_in_run > 0 and (processed_in_run % BREAK_EVERY_PRODUCTS == 0):
                        bmin, bmax = BREAK_SLEEP_RANGE
                        wait = random.uniform(bmin, bmax)
                        log_and_print(f"PAUSE: {processed_in_run} izdelkov -> počitek {wait:.1f}s")
                        time.sleep(wait)

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
