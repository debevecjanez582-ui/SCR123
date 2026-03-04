import pandas as pd
import os
from datetime import datetime
import time
import random
import re
import json
from urllib.parse import urljoin
from typing import Optional, Dict, Any, List, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ============================================================
# OBI scraper (GitHub/CI friendly)
# ------------------------------------------------------------
# Dodatne funkcionalnosti (po dogovoru):
#  - Oznaka/naziv: samo številka (brez "Št. art.:")
#  - EAN: pobere (ne validira dolžine; ohrani tudi 12-mestne ipd.)
#  - Dobava: DA/NE na osnovi zaloge po centrih
#  - Cene: vedno zaokrožene na 2 decimalki
#  - Akcijska cena: če so za isti EM 2 ceni -> min=akcijska, max=redna
#  - Proizvajalec: best-effort iz JSON-LD / itemprop brand
#  - Anti-bot: zazna captcha/verification strani in naredi backoff
#  - Polite scraping: stabilen UA na run, jitter sleep, občasni "počitek"
#  - EM normalizacija: če ni v whitelist -> kos
# ============================================================

# --- Konfiguracija ---
SHOP_NAME = "OBI"
BASE_URL = "https://www.obi.si"
DDV_RATE = 0.22

# Kategorije za OBI
OBI_CATEGORIES = {
    "Ploščice": [
        "https://www.obi.si/c/gradnja-877/ploscice-308/talne-ploscice-1150",
        "https://www.obi.si/c/gradnja-877/ploscice-308/stenske-ploscice-786",
        "https://www.obi.si/c/gradnja-877/ploscice-308/stenske-obrobe-1850",
        "https://www.obi.si/c/gradnja-877/ploscice-308/okrasne-ploscice-1849",
        "https://www.obi.si/c/gradnja-877/ploscice-308/ploscice-iz-naravnega-kamna-1151",
        "https://www.obi.si/c/gradnja-877/ploscice-308/obzidniki-in-koticki-481",
        "https://www.obi.si/c/gradnja-877/ploscice-308/mozaiki-572",
        "https://www.obi.si/c/gradnja-877/ploscice-308/robne-ploscice-1152",
    ],
    "Ureditev okolice": [
        "https://www.obi.si/c/gradnja-877/ureditev-okolice-336/pohodne-plosce-914",
        "https://www.obi.si/c/gradnja-877/ureditev-okolice-336/tlakovci-608",
        "https://www.obi.si/c/gradnja-877/ureditev-okolice-336/obrobe-stopnice-in-zidni-sistemi-1281",
        "https://www.obi.si/c/gradnja-877/ureditev-okolice-336/terasne-deske-1464",
        "https://www.obi.si/c/gradnja-877/ureditev-okolice-336/terasne-in-pohodne-plosce-1279",
        "https://www.obi.si/c/gradnja-877/ureditev-okolice-336/okrasni-prod-in-okrasni-drobljenec-1382",
    ],
    "Gradbeni materiali": [
        "https://www.obi.si/c/gradnja-877/gradbeni-materiali-175/omet-malta-in-cement-619",
        "https://www.obi.si/c/gradnja-877/gradbeni-materiali-175/suha-gradnja-764",
        "https://www.obi.si/c/gradnja-877/gradbeni-materiali-175/kamni-in-pesek-720",
        "https://www.obi.si/c/gradnja-877/gradbeni-materiali-175/izolacijski-material-233",
    ],
}

# Centri (za stabilne stolpce v Excelu)
OBI_STORES_ORDER = [
    "OBI Spletna trgovina",
    "OBI Celje",
    "OBI Koper",
    "OBI Kranj",
    "OBI Ljubljana",
    "OBI Maribor",
    "OBI Murska Sobota",
    "OBI Nova Gorica",
    "OBI Ptuj",
]

# Excel stolpci
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
STORE_EXCEL_COLS = [f"Zaloga - {s}" for s in OBI_STORES_ORDER]

# “Polite scraping” tempo (jitter)
DETAIL_SLEEP_RANGE = (2.0, 6.0)      # pred/okoli detail strani
BETWEEN_PRODUCTS_RANGE = (0.8, 2.2)  # dodatni jitter med izdelki
BETWEEN_PAGES_RANGE = (2.0, 6.0)     # pavza med list stranmi
BETWEEN_SUBCATS_RANGE = (8.0, 25.0)  # med podkategorijami
STARTUP_JITTER_CI = (0.5, 3.0)
STARTUP_JITTER_LOCAL = (2.0, 12.0)

# checkpoint (JSON)
FLUSH_JSON_EVERY = int(os.environ.get("FLUSH_JSON_EVERY", "50"))

# občasni “počitek” (da ni preveč enakomerno)
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
_RUN_UA = random.choice(USER_AGENTS)

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

_log_file = None
_global_item_counter = 0


# -----------------------------
# Logging / sleeps
# -----------------------------
def log_and_print(message: str, to_file: bool = True) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_message = f"[{timestamp}] {message}"
    print(full_message)
    if to_file and _log_file:
        try:
            _log_file.write(full_message + "\n")
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

    today_date_folder = datetime.now().strftime("%Y-%m-%d")
    daily_dir = os.path.join(output_root, "Ceniki_Scraping", shop_name, today_date_folder)
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
    """Session z retry/backoff (manj napak, manj ‘burst’ prometa ob težavah)."""
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
    """Stabilen UA (izbran na začetku), referer nastavljen glede na navigacijo."""
    return {
        "User-Agent": _RUN_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "sl-SI,sl;q=0.9,en-US;q=0.7,en;q=0.5",
        "Connection": "keep-alive",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
        "Referer": referer or BASE_URL,
    }


def is_block_page(html: str) -> bool:
    """Bolj varna detekcija block/challenge strani.

    Prejšnja verzija je iskala substring "captcha", kar je povzročilo lažne alarme
    (npr. "reCAPTCHA" scripti na normalnih straneh). Ta verzija išče bolj specifične
    indikatorje (Cloudflare/PerimeterX/DataDome/Incapsula ipd.).
    """
    if not html:
        return False

    t = html.lower()

    # zelo močni indikatorji zaščite (challenge stran)
    strong_needles = [
        "/cdn-cgi/challenge-platform",   # Cloudflare
        "cf-chl-",                        # Cloudflare challenge
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
        "geo.captcha",
        "incapsula",
        "sucuri website firewall",
        "ddos-guard",
        "akamai bot manager",
    ]
    if any(n in t for n in strong_needles):
        return True

    # captcha widget indikatorji (specifični – ne "captcha" na splošno!)
    if re.search(r"\b(hcaptcha|cf-turnstile|g-recaptcha|px-captcha|data-sitekey)\b", t):
        # g-recaptcha sam po sebi lahko obstaja tudi na normalnih straneh,
        # zato preverimo še, da je prisotna tudi kakšna "challenge" fraza
        if ("verify" in t) or ("verifying" in t) or ("access denied" in t) or ("blocked" in t):
            return True

    # "soft" indikatorji – če se pojavijo skupaj
    soft = 0
    for n in ("captcha", "challenge", "bot", "blocked", "verify", "verification"):
        if n in t:
            soft += 1
    return soft >= 4

def get_page_content(session: requests.Session, url: str, referer: Optional[str] = None) -> Optional[str]:
    """
    GET + soft retry pri 403.
    Če zaznamo captcha/verification HTML, naredimo backoff in poskusimo še.
    """
    headers = get_headers(referer=referer)

    for attempt in range(1, MAX_BLOCK_RETRIES + 1):
        try:
            resp = session.get(url, headers=headers, timeout=25)

            # 403 pogosto pomeni zaščito -> upočasni
            if resp.status_code == 403:
                wait = min(180, 15 * attempt + random.uniform(0, 15))
                log_and_print(f"HTTP 403 @ {url} -> sleep {wait:.1f}s (poskus {attempt}/{MAX_BLOCK_RETRIES})")
                time.sleep(wait)
                continue

            if resp.status_code >= 400:
                log_and_print(f"HTTP {resp.status_code} @ {url}")

            if not resp.ok:
                return None

            html = resp.text or ""

            # captcha/verification stran -> backoff, ne parsamo kot produkt!
            if is_block_page(html):
                wait = min(300, 30 * attempt + random.uniform(0, 30))
                log_and_print(f"[BLOCK] verification/captcha @ {url} -> sleep {wait:.1f}s (poskus {attempt}/{MAX_BLOCK_RETRIES})")
                time.sleep(wait)
                continue

            return html

        except requests.RequestException as e:
            wait = min(90, 5 * attempt + random.uniform(0, 10))
            log_and_print(f"Request error @ {url}: {e} -> sleep {wait:.1f}s (poskus {attempt}/{MAX_BLOCK_RETRIES})")
            time.sleep(wait)

    # preveč block poskusov
    log_and_print(f"[BLOCK] Preveč poskusov, preskakujem URL: {url}")
    return None


# -----------------------------
# Parsing helpers (cena / EM)
# -----------------------------
def _parse_float_any(price_str: str) -> Optional[float]:
    """Robusten parser za '1.234,56' ali '1234.56' ali '1234,56'."""
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
    """Vedno 2 decimalki, decimalna vejica."""
    v = _parse_float_any(price_str) if price_str else None
    return fmt_2dec(v)


def convert_price_to_without_vat(price_str: Optional[str], vat_rate: float) -> str:
    """Iz cene z DDV izračuna ceno brez DDV (2 decimalki)."""
    v = _parse_float_any(price_str) if price_str else None
    if v is None:
        return ""
    return fmt_2dec(v / (1 + vat_rate))


def normalize_url(href: str) -> str:
    return urljoin(BASE_URL, href)


def parse_price_unit_matches(text: str) -> List[Tuple[str, str]]:
    """Najde pare '12,34 € / m2'."""
    t = text.replace("\xa0", " ")
    return re.findall(r"(\d{1,3}(?:\.\d{3})*,\d{2}|\d+[\.,]\d+)\s*€\s*/\s*([\w²³]+)", t)


def normalize_em(unit: str) -> str:
    """Če EM ni na seznamu dovoljenih -> 'kos'."""
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
# Product field extraction
# -----------------------------
def extract_product_id_numbers(soup: BeautifulSoup) -> str:
    """Št. art. -> samo številke (brez 'Št. art.:')."""
    txt = soup.get_text("\n", strip=True)
    ids = re.findall(r"Št\.?\s*art\.?\s*:\s*([0-9]+)", txt, flags=re.IGNORECASE)
    if not ids:
        ids = re.findall(r"\bart\.?\s*:\s*([0-9]+)", txt, flags=re.IGNORECASE)

    uniq: List[str] = []
    for i in ids:
        i = i.strip()
        if i and i not in uniq:
            uniq.append(i)
    return ";".join(uniq)


def extract_ean_raw(soup: BeautifulSoup) -> str:
    """
    EAN/GTIN: NE validiramo dolžine (po tvojem navodilu).
    Poskusimo meta/JSON-LD, nato regex.
    """
    for prop in ("gtin13", "gtin14", "gtin12", "gtin", "ean"):
        m = soup.find("meta", attrs={"itemprop": prop})
        if m and m.get("content"):
            v = re.sub(r"\s+", "", m["content"]).strip()
            if v:
                return v

    # JSON-LD
    for s in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            payload = json.loads(s.get_text(strip=True))
        except Exception:
            continue

        def walk(obj):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    lk = str(k).lower()
                    if lk in ("gtin13", "gtin14", "gtin12", "gtin", "ean"):
                        if isinstance(v, str) and v.strip():
                            return v.strip()
                    res = walk(v)
                    if res:
                        return res
            elif isinstance(obj, list):
                for it in obj:
                    res = walk(it)
                    if res:
                        return res
            return ""

        found = walk(payload)
        if found:
            return found

    txt = soup.get_text(" ", strip=True)
    m = re.search(r"(EAN|GTIN)\s*[:#]?\s*([0-9]{6,20})", txt, flags=re.IGNORECASE)
    if m:
        return m.group(2).strip()

    return ""


def extract_manufacturer(soup: BeautifulSoup) -> str:
    """Proizvajalec/brand (best-effort)."""
    b = soup.select_one('[itemprop="brand"]')
    if b:
        t = b.get_text(" ", strip=True)
        if t:
            return t

    m = soup.find("meta", attrs={"itemprop": "brand"})
    if m and m.get("content"):
        return m["content"].strip()

    for s in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            payload = json.loads(s.get_text(strip=True))
        except Exception:
            continue

        def walk(obj):
            if isinstance(obj, dict):
                if "brand" in obj:
                    br = obj.get("brand")
                    if isinstance(br, dict):
                        name = br.get("name")
                        if isinstance(name, str) and name.strip():
                            return name.strip()
                    if isinstance(br, str) and br.strip():
                        return br.strip()
                for v in obj.values():
                    r = walk(v)
                    if r:
                        return r
            elif isinstance(obj, list):
                for it in obj:
                    r = walk(it)
                    if r:
                        return r
            return ""

        found = walk(payload)
        if found:
            return found

    return ""


def extract_product_title(soup: BeautifulSoup) -> str:
    h1 = soup.select_one("div.product-basics-info.part-1 h1") or soup.select_one("h1")
    if h1:
        return h1.get_text(strip=True)
    t = soup.select_one("title")
    return t.get_text(strip=True) if t else ""


def extract_product_long_description(soup: BeautifulSoup) -> str:
    """Dolg opis iz sekcije 'Opis' (ne H1)."""
    txt = soup.get_text("\n", strip=True).replace("\xa0", " ")
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in txt.splitlines()]
    lines = [ln for ln in lines if ln]

    idx = None
    for i, ln in enumerate(lines):
        if ln.lower().strip(": ") == "opis":
            idx = i
            break
    if idx is None:
        return ""

    stop_headers = {
        "podatki proizvajalca",
        "tehnične lastnosti",
        "ocene",
        "nazadnje ogledani izdelki",
        "prijava na spletne novice",
        "4 razlogi za nakup brez skrbi",
    }

    out: List[str] = []
    for ln in lines[idx + 1:]:
        low = ln.lower().strip()
        if low in stop_headers or any(low.startswith(h) for h in stop_headers):
            break
        out.append(ln)

    desc = "\n".join(out).strip()
    if len(desc) > 8000:
        desc = desc[:8000].rstrip() + "…"
    return desc


def extract_image_url(soup: BeautifulSoup) -> str:
    """Glavna slika (meta og:image je navadno najbolj zanesljiv)."""
    og = soup.find("meta", attrs={"property": "og:image"})
    if og and og.get("content"):
        return og["content"].strip()
    tw = soup.find("meta", attrs={"name": "twitter:image"})
    if tw and tw.get("content"):
        return tw["content"].strip()
    img = soup.select_one("img[src]")
    if img and img.get("src"):
        return img["src"].strip()
    return ""


def extract_store_stock(soup: BeautifulSoup) -> Dict[str, int]:
    """Prebere zalogo po centrih iz sekcije 'Stanje zaloge' (če je v HTML)."""
    txt = soup.get_text("\n", strip=True).replace("\xa0", " ")
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in txt.splitlines() if ln.strip()]

    start = None
    for i, ln in enumerate(lines[:600]):
        if ln.lower().strip(": ") == "stanje zaloge":
            start = i
            break
    if start is None:
        return {}

    segment = lines[start + 1:start + 180]
    stock: Dict[str, int] = {}

    for ln in segment:
        if not ln.startswith("OBI"):
            continue

        m = re.match(r"^(OBI.+?)\s+(\d+)\s+kos", ln, flags=re.IGNORECASE)
        if m:
            name = m.group(1).strip()
            qty = int(m.group(2))
            stock[name] = qty
            continue

        if re.search(r"\bna zalogi\b", ln, flags=re.IGNORECASE):
            name = re.sub(r"\bna zalogi\b.*$", "", ln, flags=re.IGNORECASE).strip()
            if name and name.startswith("OBI"):
                stock[name] = max(stock.get(name, 0), 1)
            continue

        if re.search(r"\bni na zalogi\b", ln, flags=re.IGNORECASE):
            name = re.sub(r"\bni na zalogi\b.*$", "", ln, flags=re.IGNORECASE).strip()
            if name and name.startswith("OBI"):
                stock[name] = 0
            continue

    return stock


# -----------------------------
# Core: product parsing
# -----------------------------
def extract_product_details(
    session: requests.Session,
    product_url: str,
    category_name: str,
    date_str: str,
    referer: str
) -> Optional[Dict[str, Any]]:
    """Parsanje produktne strani -> 1 zapis (flat)."""
    global _global_item_counter

    html = get_page_content(session, product_url, referer=referer)
    if not html:
        return None

    soup = BeautifulSoup(html, "lxml")
    _global_item_counter += 1

    data: Dict[str, Any] = {
        "Skupina": category_name,
        "Zap": _global_item_counter,
        "Veljavnost od": date_str,
        "Valuta": "EUR",
        "DDV": "22",
        "EM": "kos",
        "URL": product_url,
        "EAN": "",
        "Proizvajalec": "",
        "Opis": "",
        "Opis izdelka": "",
        "Oznaka / naziv": "",
        "Cena / EM (z DDV)": "",
        "Akcijska cena / EM (z DDV)": "",
        "Cena / EM (brez DDV)": "",
        "Akcijska cena / EM (brez DDV)": "",
        "SLIKA URL": "",
        "Dobava": "NE",  # po dogovoru: DA/NE po centrih
        "Zaloga po centrih": "",
    }

    # --- osnovna polja ---
    data["Opis"] = extract_product_title(soup)
    data["Opis izdelka"] = extract_product_long_description(soup)
    data["Oznaka / naziv"] = extract_product_id_numbers(soup)     # brez "Št. art.:"
    data["EAN"] = extract_ean_raw(soup)                           # brez validacije dolžine
    data["Proizvajalec"] = extract_manufacturer(soup)             # brand/proizvajalec (best-effort)
    data["SLIKA URL"] = extract_image_url(soup)

    # --- cena + akcijska cena (če sta 2 ceni za isti EM) ---
    page_txt = soup.get_text(" ", strip=True)
    matches = parse_price_unit_matches(page_txt)

    chosen_unit = ""
    if matches:
        # izberi EM: prefer kos, sicer prva najdena EM
        for _, u in matches:
            if str(u).lower().startswith("kos"):
                chosen_unit = u
                break
        if not chosen_unit:
            chosen_unit = matches[0][1]

    data["EM"] = normalize_em(chosen_unit) if chosen_unit else normalize_em(data.get("EM", "kos"))

    # poberi vse cene za izbran EM, nato:
    #   max = redna, min = akcijska (če sta 2 različni)
    vals: List[float] = []
    if matches:
        for p, u in matches:
            if normalize_em(u) == data["EM"]:
                fv = _parse_float_any(p)
                if fv is not None:
                    vals.append(fv)

    vals = sorted(set(vals))
    if len(vals) >= 2:
        data["Cena / EM (z DDV)"] = fmt_2dec(vals[-1])
        data["Akcijska cena / EM (z DDV)"] = fmt_2dec(vals[0])
    elif len(vals) == 1:
        data["Cena / EM (z DDV)"] = fmt_2dec(vals[0])

    # brez DDV (oba)
    data["Cena / EM (brez DDV)"] = convert_price_to_without_vat(data.get("Cena / EM (z DDV)", ""), DDV_RATE)
    data["Akcijska cena / EM (brez DDV)"] = convert_price_to_without_vat(data.get("Akcijska cena / EM (z DDV)", ""), DDV_RATE)

    # --- dobava + zaloga po centrih ---
    stock = extract_store_stock(soup)
    if stock:
        data["Zaloga po centrih"] = json.dumps(stock, ensure_ascii=False)
        data["Dobava"] = "DA" if any(qty > 0 for qty in stock.values()) else "NE"
        for store in OBI_STORES_ORDER:
            data[f"Zaloga - {store}"] = int(stock.get(store, 0))
    else:
        # če nimamo podatkov o zalogi, ostane NE (konzervativno)
        data["Dobava"] = "NE"
        for store in OBI_STORES_ORDER:
            data[f"Zaloga - {store}"] = 0

    return data


# -----------------------------
# Save/load helpers
# -----------------------------
def load_existing_data(json_path: str, excel_path: str) -> List[Dict[str, Any]]:
    """Prednost: JSON. Če ga ni, poskusi Excel."""
    if os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                d = json.load(f)
            if isinstance(d, list):
                return d
        except Exception:
            pass

    if os.path.exists(excel_path):
        try:
            df = pd.read_excel(excel_path)
            return df.to_dict(orient="records")
        except Exception:
            pass

    return []


def merge_by_url(existing: List[Dict[str, Any]], new_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Dedup po URL (zadnji zapis zmaga)."""
    d: Dict[str, Dict[str, Any]] = {x.get("URL"): x for x in existing if x.get("URL")}
    for x in new_items:
        if x.get("URL"):
            d[x["URL"]] = x
    out = list(d.values())
    try:
        out.sort(key=lambda x: int(x.get("Zap", 0)))
    except Exception:
        pass
    return out


def write_json(data: List[Dict[str, Any]], json_path: str) -> None:
    """Zapis kanoničnega JSON (flat)."""
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def write_excel(data: List[Dict[str, Any]], excel_path: str) -> None:
    """Zapis Excela (flat) + fiksni stolpci za zalogo."""
    df = pd.DataFrame(data)
    cols = BASE_EXCEL_COLS + STORE_EXCEL_COLS
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    df[cols].to_excel(excel_path, index=False)


# -----------------------------
# Main scraping loop
# -----------------------------
def main():
    global _log_file, _global_item_counter

    # start jitter: da se CI ne zažene vedno točno ob isti sekundi
    if is_ci():
        human_sleep(*STARTUP_JITTER_CI)
    else:
        human_sleep(*STARTUP_JITTER_LOCAL)

    json_path, excel_path, log_path = create_output_paths(SHOP_NAME)
    try:
        _log_file = open(log_path, "w", encoding="utf-8")
    except Exception:
        return

    session = build_session()
    log_and_print(f"--- Zagon {SHOP_NAME} ---")
    log_and_print(f"User-Agent (stabilen za ta zagon): {_RUN_UA}")

    # resume: če že obstaja json/excel, nadaljuj števec in ne pobiraj istih URL
    all_data = load_existing_data(json_path, excel_path)
    if all_data:
        try:
            _global_item_counter = max((int(x.get("Zap", 0)) for x in all_data), default=0)
        except Exception:
            _global_item_counter = 0

    existing_urls = {x.get("URL") for x in all_data if x.get("URL")}

    date_str = datetime.now().strftime("%d/%m/%Y")
    buffer: List[Dict[str, Any]] = []
    processed_in_run = 0

    try:
        for cat, urls in OBI_CATEGORIES.items():
            log_and_print(f"\n--- {cat} ---")

            for category_url in urls:
                sub_name = category_url.strip("/").split("/")[-1]
                log_and_print(f"  Podkategorija: {sub_name}")

                n = 1
                last_first_title = None
                MAX_PAGES_LOCAL = 250  # varovalo proti neskončni zanki

                while n <= MAX_PAGES_LOCAL:
                    page_url = f"{category_url}?p={n}"
                    log_and_print(f"    Stran {n}: {page_url}")

                    html = get_page_content(session, page_url, referer=category_url)
                    if not html:
                        break

                    soup = BeautifulSoup(html, "lxml")
                    container = soup.find("div", class_="list-items list-category-products")

                    # 1) primarni selector (OBI list page)
                    if container:
                        items = container.find_all("div", class_="item")
                        if not items:
                            break

                        # anti-loop: OBI včasih vrti isto stran
                        first_title_el = items[0].find("h4") if items else None
                        first_title = first_title_el.get_text(strip=True) if first_title_el else None
                        if n > 1 and first_title and first_title == last_first_title:
                            log_and_print("    Stran se ponavlja. Konec kategorije.")
                            break
                        last_first_title = first_title

                        product_urls = []
                        for it in items:
                            a = it.find("a", href=re.compile(r"^/p/")) or it.find("a")
                            if not a or not a.get("href"):
                                continue
                            product_urls.append(normalize_url(a["href"]))
                    else:
                        # 2) fallback selector
                        product_urls = []
                        for a in soup.select('a[href^="/p/"], a[href*="/p/"]'):
                            href = a.get("href")
                            if href and "/p/" in href:
                                product_urls.append(normalize_url(href))

                    # dedupe URL-jev na strani
                    seen = set()
                    product_urls = [u for u in product_urls if not (u in seen or seen.add(u))]
                    if not product_urls:
                        break

                    for product_url in product_urls:
                        # resume: preskoči že zajete
                        if product_url in existing_urls:
                            continue

                        log_and_print(f"      Izdelek: {product_url}")

                        # glavna pavza pred detail requestom
                        human_sleep(*DETAIL_SLEEP_RANGE)

                        details = extract_product_details(
                            session=session,
                            product_url=product_url,
                            category_name=cat,
                            date_str=date_str,
                            referer=page_url,
                        )
                        if details:
                            buffer.append(details)
                            existing_urls.add(product_url)
                            processed_in_run += 1

                        # checkpoint zapis (da ob prekinitvi ne izgubiš vsega)
                        if len(buffer) >= FLUSH_JSON_EVERY:
                            all_data = merge_by_url(all_data, buffer)
                            buffer = []
                            write_json(all_data, json_path)
                            log_and_print("Shranjen JSON (checkpoint).")

                        # občasni "počitek"
                        if processed_in_run > 0 and (processed_in_run % BREAK_EVERY_PRODUCTS == 0):
                            bmin, bmax = BREAK_SLEEP_RANGE
                            wait = random.uniform(bmin, bmax)
                            log_and_print(f"PAUSE: {processed_in_run} izdelkov -> počitek {wait:.1f}s")
                            time.sleep(wait)

                        # manjši jitter med izdelki
                        human_sleep(*BETWEEN_PRODUCTS_RANGE)

                    # pavza med list stranmi
                    human_sleep(*BETWEEN_PAGES_RANGE)
                    n += 1

                if n > MAX_PAGES_LOCAL:
                    log_and_print(f"    OPOZORILO: dosežen MAX_PAGES={MAX_PAGES_LOCAL} za {category_url}")

                # checkpoint po podkategoriji
                if buffer:
                    all_data = merge_by_url(all_data, buffer)
                    buffer = []
                    write_json(all_data, json_path)
                    log_and_print("Shranjen JSON (podkategorija).")

                # pavza med podkategorijami
                human_sleep(*BETWEEN_SUBCATS_RANGE)

    except Exception as e:
        log_and_print(f"NAPAKA: {e}")
    finally:
        try:
            if buffer:
                all_data = merge_by_url(all_data, buffer)
                buffer = []

            write_json(all_data, json_path)
            log_and_print("Shranjen JSON (final).")

            write_excel(all_data, excel_path)
            log_and_print("Shranjen Excel (final).")
        except Exception as e:
            log_and_print(f"NAPAKA pri finalnem shranjevanju: {e}")
        try:
            if _log_file:
                _log_file.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
