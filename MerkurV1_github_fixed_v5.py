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
START_JITTER_CI = (0.5, 2.5)
START_JITTER_LOCAL = (2.0, 12.0)

# Koliko zapisov naenkrat shranimo v JSON (batch)
BUFFER_FLUSH = int(os.environ.get("BUFFER_FLUSH", "50"))

# Varovalo proti neskončnim zankam pri paginaciji
MAX_PAGES = int(os.environ.get("MAX_PAGES", "250"))

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
UA_THIS_RUN = random.choice(USER_AGENTS)

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

# Merkur centri (fiksni stolpci v Excelu; če se pojavi nov center, bo vsaj v JSON "Zaloga po centrih")
MERKUR_STORES_ORDER = [
    "MERKUR SPLETNA TRGOVINA",
    "MERKUR BEŽIGRAD",
    "MERKUR CELJE",
    "MERKUR DRAVOGRAD",
    "MERKUR DOMŽALE",
    "MERKUR GORICA",
    "MERKUR IZOLA",
    "MERKUR KRANJ PRIMSKOVO",
    "MERKUR KRŠKO",
    "MERKUR MURSKA SOBOTA",
    "MERKUR NOVO MESTO",
    "MERKUR PTUJ",
    "MERKUR SEŽANA",
    "MERKUR SLOVENSKA BISTRICA",
    "MERKUR TRBOVLJE",
    "MERKUR TRŽIČ",
    "MERKUR VELENJE",
    "MERKUR VIČ",
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
    "Zaloga po centrih",
]
STORE_EXCEL_COLS = [f"Zaloga - {s}" for s in MERKUR_STORES_ORDER]

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
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"[{ts}] {message}"
    print(msg)
    if to_file and _log_file:
        try:
            _log_file.write(msg + "\n")
            _log_file.flush()
        except Exception:
            pass


def is_ci() -> bool:
    return os.environ.get("GITHUB_ACTIONS", "").lower() == "true"


def human_sleep(min_s: float = None, max_s: float = None) -> None:
    mn = SLEEP_MIN if min_s is None else min_s
    mx = SLEEP_MAX if max_s is None else max_s
    time.sleep(random.uniform(mn, mx))


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
# Networking (session + page fetch)
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


def get_headers(referer: Optional[str] = None) -> Dict[str, str]:
    return {
        "User-Agent": UA_THIS_RUN,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "sl-SI,sl;q=0.9,en-US;q=0.7,en;q=0.5",
        "Connection": "keep-alive",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
        "Referer": referer or BASE_URL,
    }


def is_block_page(html: str) -> bool:
    """Detekcija bot-protection/captcha strani."""
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


def get_page_content(session: requests.Session, url: str, referer: Optional[str] = None) -> Optional[str]:
    """
    GET + backoff (429/5xx) + captcha detection.
    Če dobimo "block page", počakamo in poskusimo še.
    """
    headers = get_headers(referer=referer)

    for attempt in range(1, MAX_BLOCK_RETRIES + 1):
        try:
            resp = session.get(url, headers=headers, timeout=25)

            if resp.status_code == 403:
                wait = min(180, 15 * attempt + random.uniform(0, 15))
                log_and_print(f"HTTP 403 @ {url} -> sleep {wait:.1f}s (poskus {attempt}/{MAX_BLOCK_RETRIES})")
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
# URL helpers
# -----------------------------
def add_query_param(url: str, key: str, value: str) -> str:
    """Dodaj/posodobi query param (npr. p=2)."""
    u = urlparse(url)
    q = dict(parse_qsl(u.query))
    q[key] = value
    new_query = urlencode(q)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_query, u.fragment))


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
    v = _parse_float_any(price_str) if price_str else None
    return fmt_2dec(v)


def convert_price_to_without_vat(price_str: Optional[str], vat_rate: float) -> str:
    v = _parse_float_any(price_str) if price_str else None
    if v is None:
        return ""
    return fmt_2dec(v / (1 + vat_rate))


def normalize_em(unit: str) -> str:
    """Če EM ni v whitelist -> kos."""
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
    """Ujame npr. 46,34 ali 1.234,56 ali 46.34."""
    if not text:
        return ""
    m = re.search(r"(\d{1,3}(?:\.\d{3})*,\d{2}|\d+(?:[.,]\d+)?)", text)
    return m.group(1).strip() if m else ""


# -----------------------------
# Content extraction
# -----------------------------
def clean_duplicated_title(s: str) -> str:
    """Če je naslov podvojen (A A), obdrži A."""
    if not s:
        return ""
    t = re.sub(r"\s+", " ", s).strip()
    mid = len(t) // 2
    # če je natančno ponovljen niz
    if len(t) > 20 and t[:mid].strip() == t[mid:].strip():
        return t[:mid].strip()
    # če se ponavlja celoten string po separatorjih (npr. 'X ... X ...')
    m = re.match(r"^(.+?)\s+\1$", t)
    if m:
        return m.group(1).strip()
    return t


def extract_long_description(soup: BeautifulSoup) -> str:
    """Izlušči daljši opis iz sekcije 'Opis' do 'Tehnične podrobnosti'."""
    txt = soup.get_text("\n", strip=True).replace("\xa0", " ")
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in txt.splitlines()]
    lines = [ln for ln in lines if ln]

    start = None
    for i, ln in enumerate(lines):
        if ln.strip(": ").lower() == "opis":
            start = i
            break
    if start is None:
        return ""

    stop_headers = {
        "tehnične podrobnosti",
        "mnenja",
        "ocene",
        "dodatne informacije",
        "podobni izdelki",
        "nazadnje ogledani izdelki",
        "plačilna sredstva",
        "pridruži se nam",
    }

    out = []
    for ln in lines[start + 1:]:
        l = ln.lower().strip()
        if l in stop_headers or any(l.startswith(h) for h in stop_headers):
            break
        out.append(ln)

    desc = "\n".join(out).strip()
    if len(desc) > 8000:
        desc = desc[:8000].rstrip() + "…"
    return desc


def extract_specs(soup: BeautifulSoup) -> Dict[str, str]:
    """Grobo izlušči tehnične podrobnosti v dict (label -> value)."""
    txt = soup.get_text("\n", strip=True).replace("\xa0", " ")
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in txt.splitlines()]
    lines = [ln for ln in lines if ln]

    start = None
    for i, ln in enumerate(lines):
        if ln.strip(": ").lower() == "tehnične podrobnosti":
            start = i
            break
    if start is None:
        return {}

    specs: Dict[str, str] = {}
    for ln in lines[start + 1:]:
        l = ln.lower()
        if l in {"mnenja", "opis", "plačilna sredstva", "pridruži se nam"}:
            break

        m = re.match(r"^(EAN\s+koda|EAN|Blagovna\s+znamka|Proizvajalec)\s+(.+)$", ln, flags=re.I)
        if m:
            key = m.group(1).strip()
            val = m.group(2).strip()
            specs[key] = val
            continue

    return specs


def extract_product_id(text: str) -> str:
    """Izlušči šifro izdelka (številke) iz strani."""
    # pogosti vzorci: "Šifra: 134410", "ID: 134410"
    m = re.search(r"(Šifra|ID|Artikel)\s*[:#]?\s*([0-9]{4,})", text, flags=re.I)
    if m:
        return m.group(2).strip()

    # fallback: "product-id"
    m2 = re.search(r"\b([0-9]{6,})\b", text)
    return m2.group(1) if m2 else ""


def extract_price_and_unit(soup: BeautifulSoup) -> Tuple[str, str, str]:
    """
    Vrne (price, unit, special_price_if_any).
    Vsi outputi so "raw" stringi; rounding/normalization naredimo kasneje.
    """
    txt = soup.get_text(" ", strip=True).replace("\xa0", " ")

    special = ""
    m_spec = re.search(r"Akcijska\s+cena\s*([0-9]{1,3}(?:\.[0-9]{3})*,[0-9]{2})", txt, flags=re.I)
    if m_spec:
        special = m_spec.group(1)

    m = re.search(r"([0-9]{1,3}(?:\.[0-9]{3})*,[0-9]{2})\s*€\s*/\s*([A-Za-zŠŽČšžč0-9²³]+)", txt)
    if m:
        return m.group(1), m.group(2), special

    p = parse_price_any(txt)
    return p, "", special


def extract_store_availability(soup: BeautifulSoup) -> Dict[str, str]:
    """
    Best-effort: izlušči zalogo/razpoložljivost po centrih iz HTML.
    Ker se layout lahko spreminja, je to heuristika (SSR).
    """
    txt = soup.get_text("\n", strip=True).replace("\xa0", " ")
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in txt.splitlines() if ln.strip()]

    # poišči blok "Razpoložljivost v trgovinah" (ali podobno)
    start = None
    for i, ln in enumerate(lines[:800]):
        if ln.lower().strip(": ") in {"razpoložljivost v trgovinah", "razpoložljivost v trgovini", "zaloga v trgovinah"}:
            start = i
            break
    if start is None:
        return {}

    segment = lines[start + 1:start + 260]
    stock: Dict[str, str] = {}

    # tipično: "MERKUR CELJE Na zalogi"
    for ln in segment:
        if not ln.upper().startswith("MERKUR"):
            continue
        m = re.match(r"^(MERKUR.+?)\s+(Na\s+zalogi|Ni\s+na\s+zalogi|Ni\s+zaloge|Zadnji\s+kos|Zadnji\s+izdelki|Na\s+voljo.*)$", ln, flags=re.I)
        if m:
            name = m.group(1).strip().upper()
            status = m.group(2).strip()
            stock[name] = status
    return stock


def extract_product_urls_from_listing(html: str) -> List[str]:
    """Iz list strani potegni URL-je produktov."""
    soup = BeautifulSoup(html, "lxml")

    urls: List[str] = []
    # Merkur listing običajno vsebuje <a href="..."> okoli produkta
    for a in soup.select('a[href^="/"], a[href^="https://www.merkur.si/"]'):
        href = a.get("href") or ""
        if "/p/" in href or href.startswith("https://www.merkur.si/"):
            # filtriraj tipične produktne linke (heuristika)
            if "/gradnja/" in href or "/izdelki/" in href or href.endswith("/"):
                full = urljoin(BASE_URL, href)
                # odfiltriraj kategorijske linke
                if "/c/" in full:
                    continue
                urls.append(full)

    # fallback: v originalni implementaciji so bili že zanesljivi selektorji
    # zato še dedupe:
    urls = list(dict.fromkeys(urls))

    # dodatno: filtriraj samo tiste, ki izgledajo kot produkt (Merkur ima pogosto trailing slash)
    filtered = []
    for u in urls:
        # izloči očitne navigacijske
        if any(x in u for x in ["/search", "/cart", "/checkout", "/account"]):
            continue
        filtered.append(u)
    return filtered


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
    cols = BASE_EXCEL_COLS + STORE_EXCEL_COLS
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    df[cols].to_excel(excel_path, index=False)
    log_and_print("Shranjen Excel (na koncu).")


# -----------------------------
# Product details
# -----------------------------
def extract_product_details(
    session: requests.Session,
    product_url: str,
    group_name: str,
    date_str: str,
    referer: str,
) -> Optional[Dict[str, Any]]:
    """Parsanje produktne strani -> 1 zapis."""
    global _global_item_counter

    html = get_page_content(session, product_url, referer=referer)
    if not html:
        return None

    soup = BeautifulSoup(html, "lxml")
    page_txt = soup.get_text("\n", strip=True).replace("\xa0", " ")

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

    # --- naslov (očisti podvajanje) ---
    h1 = soup.select_one("h1")
    if h1:
        data["Opis"] = clean_duplicated_title(h1.get_text(" ", strip=True))

    # --- daljši opis ---
    data["Opis izdelka"] = extract_long_description(soup)

    # --- šifra izdelka (številke) ---
    data["Oznaka / naziv"] = extract_product_id(page_txt)

    # --- specifikacije: EAN + proizvajalec ---
    specs = extract_specs(soup)

    for k in ("EAN koda", "EAN"):
        if k in specs and specs[k]:
            data["EAN"] = specs[k]  # NE validiramo dolžine
            break

    if "Blagovna znamka" in specs and specs["Blagovna znamka"]:
        data["Proizvajalec"] = specs["Blagovna znamka"]
    elif "Proizvajalec" in specs and specs["Proizvajalec"]:
        data["Proizvajalec"] = specs["Proizvajalec"]

    # --- slika ---
    og = soup.select_one('meta[property="og:image"]')
    if og and og.get("content"):
        data["SLIKA URL"] = og.get("content").strip()

    # --- cena + enota + akcijska ---
    price_raw, unit_raw, special_raw = extract_price_and_unit(soup)

    if price_raw:
        data["Cena / EM (z DDV)"] = round_price_2dec(price_raw)
        data["Cena / EM (brez DDV)"] = convert_price_to_without_vat(data["Cena / EM (z DDV)"], DDV_RATE)

    if special_raw:
        data["Akcijska cena / EM (z DDV)"] = round_price_2dec(special_raw)
        data["Akcijska cena / EM (brez DDV)"] = convert_price_to_without_vat(data["Akcijska cena / EM (z DDV)"], DDV_RATE)

    if unit_raw:
        data["EM"] = normalize_em(unit_raw)
    else:
        data["EM"] = normalize_em(data.get("EM", "kos"))

    # --- dobavljivost / zaloga po centrih ---
    store_stock = extract_store_availability(soup)
    if store_stock:
        data["Zaloga po centrih"] = json.dumps(store_stock, ensure_ascii=False)

        any_available = any(s.lower().startswith("na") or "zadnji" in s.lower() for s in store_stock.values())
        data["Dobava"] = "DA" if any_available else "NE"

        for store in MERKUR_STORES_ORDER:
            data[f"Zaloga - {store}"] = store_stock.get(store, "")
    else:
        # fallback: samo "Na zalogi / Ni zaloge" na strani
        if "Na zalogi" in page_txt:
            data["Dobava"] = "DA"
        elif "Ni zaloge" in page_txt or "Ni na zalogi" in page_txt:
            data["Dobava"] = "NE"

        for store in MERKUR_STORES_ORDER:
            data[f"Zaloga - {store}"] = ""

    return data


# -----------------------------
# Main
# -----------------------------
def main():
    global _log_file, _global_item_counter

    # start jitter
    if is_ci():
        human_sleep(*START_JITTER_CI)
    else:
        human_sleep(*START_JITTER_LOCAL)

    json_path, excel_path, log_path = create_output_paths(SHOP_NAME)
    try:
        _log_file = open(log_path, "w", encoding="utf-8")
    except Exception:
        return

    log_and_print(f"--- Zagon {SHOP_NAME} ---")
    log_and_print(f"UA: {UA_THIS_RUN}")
    log_and_print(f"SLEEP=[{SLEEP_MIN},{SLEEP_MAX}] BUFFER_FLUSH={BUFFER_FLUSH} EXCEL(end)={EXPORT_EXCEL} MAX_PAGES={MAX_PAGES}")

    session = build_session()

    # resume: če json že obstaja, nadaljuj števec in ne pobiraj istih URL
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
    buffer: List[dict] = []
    processed_in_run = 0

    try:
        for main_cat, sub_urls in MERKUR_CATEGORIES.items():
            log_and_print(f"\n=== {main_cat} ===")

            for sub_cat_url in sub_urls:
                group_name = sub_cat_url.strip("/").split("/")[-1].replace("-", " ").capitalize()
                log_and_print(f"\n-- Podkategorija: {group_name}")
                last_first_url = None

                for page in range(1, MAX_PAGES + 1):
                    page_url = add_query_param(sub_cat_url, "p", str(page))
                    log_and_print(f"  Stran {page}: {page_url}")

                    html = get_page_content(session, page_url, referer=sub_cat_url)
                    if not html:
                        break

                    product_urls = extract_product_urls_from_listing(html)
                    if not product_urls:
                        break

                    # zaščita proti ponavljanju strani
                    first_url = product_urls[0]
                    if page > 1 and last_first_url and first_url == last_first_url:
                        log_and_print("  Stran se ponavlja. Konec te podkategorije.")
                        break
                    last_first_url = first_url

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
                            referer=page_url,
                        )
                        if details:
                            buffer.append(details)
                            existing_urls.add(product_url)
                            processed_in_run += 1

                        if len(buffer) >= BUFFER_FLUSH:
                            save_data_append(buffer, json_path)
                            buffer = []

                        # občasni počitek (da ni preveč enakomerno)
                        if processed_in_run > 0 and (processed_in_run % BREAK_EVERY_PRODUCTS == 0):
                            bmin, bmax = BREAK_SLEEP_RANGE
                            wait = random.uniform(bmin, bmax)
                            log_and_print(f"PAUSE: {processed_in_run} izdelkov -> počitek {wait:.1f}s")
                            time.sleep(wait)

                        # majhen jitter med izdelki
                        human_sleep(0.8, 2.2)

                    # pavza med stranmi
                    human_sleep(2.0, 6.0)

                # flush po podkategoriji
                if buffer:
                    save_data_append(buffer, json_path)
                    buffer = []

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
