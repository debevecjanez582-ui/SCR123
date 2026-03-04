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
# OBI scraper (GitHub/CI friendly) - stable
# ============================================================

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

# Fiksni centri za Excel (JSON lahko vsebuje tudi nove)
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

# Tempo / varnost (lahko overridaš z env)
SLEEP_MIN = float(os.environ.get("SCRAPE_SLEEP_MIN", "4.0"))
SLEEP_MAX = float(os.environ.get("SCRAPE_SLEEP_MAX", "6.0"))
DETAIL_SLEEP_RANGE = (2.0, 6.0)
BETWEEN_PRODUCTS_RANGE = (0.8, 2.2)
BETWEEN_PAGES_RANGE = (2.0, 6.0)
BETWEEN_SUBCATS_RANGE = (8.0, 25.0)
STARTUP_JITTER_CI = (0.5, 3.0)
STARTUP_JITTER_LOCAL = (2.0, 12.0)

FLUSH_JSON_EVERY = int(os.environ.get("FLUSH_JSON_EVERY", "50"))
MAX_PAGES = int(os.environ.get("MAX_PAGES", "250"))
MAX_BLOCK_RETRIES = int(os.environ.get("MAX_BLOCK_RETRIES", "3"))

# EM whitelist
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
# stabilen UA na en run (bolj “naravno”)
_RUN_UA = os.environ.get("SCRAPE_UA") or random.choice(USER_AGENTS)

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
    """Shrani HTML, ko naletimo na challenge (za diagnozo)."""
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
        cols = BASE_EXCEL_COLS + STORE_EXCEL_COLS
        df = pd.DataFrame([], columns=cols)
        df.to_excel(excel_path, index=False)
    except Exception:
        pass


def is_block_page(html: str) -> bool:
    """Detekcija bot-protection/challenge strani (brez lažnih zadetkov na 'reCAPTCHA')."""
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

    # captcha widget indikatorji: samo, če je zraven še 'challenge' kontekst
    if re.search(r"\b(hcaptcha|cf-turnstile|g-recaptcha|px-captcha|data-sitekey)\b", t):
        if any(x in t for x in ("verify", "verifying", "blocked", "access denied", "challenge")):
            return True

    # soft heuristika: več signalov skupaj
    soft = 0
    for n in ("captcha", "challenge", "bot", "blocked", "verify", "verification"):
        if n in t:
            soft += 1
    return soft >= 4


def build_session() -> requests.Session:
    """Session z retry/backoff (manj napak, manj burst-a)."""
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
    """GET + backoff (429/5xx) + challenge detekcija."""
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


def normalize_url(href: str) -> str:
    return urljoin(BASE_URL, href)


def extract_product_ids_only_number(soup: BeautifulSoup) -> str:
    """Vrne samo številke artikla (brez 'Št. art.:' prefiksa)."""
    # OBI ima pogosto div.product-id
    sid = soup.select_one("div.product-id")
    txt = sid.get_text(" ", strip=True) if sid else soup.get_text(" ", strip=True)

    ids = re.findall(r"Št\.?\s*art\.?\s*:\s*([0-9]+)", txt, flags=re.IGNORECASE)
    if not ids:
        ids = re.findall(r"\bart\.?\s*:\s*([0-9]+)", txt, flags=re.IGNORECASE)
    # fallback: če nič, poberi vsaj prvo večjo številko
    if not ids:
        ids = re.findall(r"\b([0-9]{6,})\b", txt)

    uniq = []
    for i in ids:
        if i not in uniq:
            uniq.append(i)
    return ";".join(uniq)


def extract_ean_raw(soup: BeautifulSoup) -> str:
    """EAN raw, brez validacije dolžine."""
    txt = soup.get_text(" ", strip=True)
    m = re.search(r"(EAN|GTIN)\s*[:#]?\s*([0-9]{6,20})", txt, flags=re.IGNORECASE)
    return m.group(2).strip() if m else ""


def extract_product_title(soup: BeautifulSoup) -> str:
    h1 = soup.select_one("div.product-basics-info.part-1 h1") or soup.select_one("h1")
    if h1:
        return h1.get_text(" ", strip=True)
    t = soup.select_one("title")
    return t.get_text(" ", strip=True) if t else ""


def extract_product_long_description(soup: BeautifulSoup) -> str:
    """Dolg opis iz sekcije 'Opis' (best-effort)."""
    # meta description
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        return meta_desc.get("content").strip()

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
    og = soup.find("meta", attrs={"property": "og:image"})
    if og and og.get("content"):
        return og.get("content").strip()
    img = soup.select_one("img[src]")
    return img.get("src", "").strip() if img else ""


def parse_price_unit_matches(text: str) -> List[tuple]:
    """Najde pare '12,34 € / m2'."""
    t = text.replace("\xa0", " ")
    return re.findall(r"(\d{1,3}(?:\.\d{3})*,\d{2}|\d+[\.,]\d+)\s*€\s*/\s*([\w²³]+)", t)


def extract_store_stock(soup: BeautifulSoup) -> Dict[str, int]:
    """Prebere zalogo po centrih (best-effort iz vidnega HTML)."""
    txt = soup.get_text("\n", strip=True).replace("\xa0", " ")
    # najpogostejši pattern: "OBI Ljubljana 17 kosov"
    matches = re.findall(r"(OBI[^\n\r]+?)\s+(\d+)\s+kosov", txt)
    stock: Dict[str, int] = {}
    for name, qty in matches:
        name = re.sub(r"\s+", " ", name).strip()
        try:
            stock[name] = int(qty)
        except Exception:
            pass
    return stock


def extract_product_details(
    session: requests.Session,
    product_url: str,
    category_name: str,
    date_str: str,
    referer: str
) -> Optional[Dict[str, Any]]:
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
        "Dobava": "NE",
        "Zaloga po centrih": "",
    }

    data["Opis"] = extract_product_title(soup)
    data["Opis izdelka"] = extract_product_long_description(soup)

    # Oznaka/naziv = samo številka (brez 'Št. art')
    data["Oznaka / naziv"] = extract_product_ids_only_number(soup)

    # EAN raw
    data["EAN"] = extract_ean_raw(soup)

    # slika
    data["SLIKA URL"] = extract_image_url(soup)

    # cena/EM: preferiraj kos, sicer prva najdena
    page_txt = soup.get_text(" ", strip=True)
    matches = parse_price_unit_matches(page_txt)
    chosen_price, chosen_unit = "", ""
    if matches:
        for p, u in matches:
            if u.lower().startswith("kos"):
                chosen_price, chosen_unit = p, u
                break
        if not chosen_price:
            chosen_price, chosen_unit = matches[0]

    if chosen_price:
        data["Cena / EM (z DDV)"] = round_price_2dec(chosen_price)
        data["Cena / EM (brez DDV)"] = convert_price_to_without_vat(data["Cena / EM (z DDV)"], DDV_RATE)
    if chosen_unit:
        data["EM"] = normalize_em(chosen_unit)

    # zaloga po centrih
    stock = extract_store_stock(soup)
    if stock:
        data["Zaloga po centrih"] = json.dumps(stock, ensure_ascii=False)
        data["Dobava"] = "DA" if any(qty > 0 for qty in stock.values()) else "NE"
        for store in OBI_STORES_ORDER:
            data[f"Zaloga - {store}"] = stock.get(store, 0)
    else:
        data["Dobava"] = "NE"
        for store in OBI_STORES_ORDER:
            data[f"Zaloga - {store}"] = 0

    data["EM"] = normalize_em(data.get("EM") or "kos")
    return data


def load_existing_data(json_path: str, excel_path: str) -> List[Dict[str, Any]]:
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
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def write_excel(data: List[Dict[str, Any]], excel_path: str) -> None:
    df = pd.DataFrame(data)
    cols = BASE_EXCEL_COLS + STORE_EXCEL_COLS
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    df[cols].to_excel(excel_path, index=False)


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
    log_and_print(f"SLEEP=[{SLEEP_MIN},{SLEEP_MAX}] FLUSH_JSON_EVERY={FLUSH_JSON_EVERY} MAX_PAGES={MAX_PAGES}")

    # Preflight (da ne kuri časa, če je res challenge)
    first_cat = list(OBI_CATEGORIES.values())[0][0]
    test_url = f"{first_cat}?p=1"
    test_html = get_page_content(session, test_url, referer=BASE_URL)
    if not test_html:
        log_and_print("OBI vrača challenge/verification že na prvem testu. Končujem (debug HTML je v output mapi).")
        write_empty_outputs(json_path, excel_path)
        try:
            if _log_file:
                _log_file.close()
        except Exception:
            pass
        return

    all_data = load_existing_data(json_path, excel_path)
    if all_data:
        try:
            _global_item_counter = max((int(x.get("Zap", 0)) for x in all_data), default=0)
        except Exception:
            _global_item_counter = 0

    date_str = datetime.now().strftime("%d/%m/%Y")
    buffer: List[Dict[str, Any]] = []

    try:
        for cat, urls in OBI_CATEGORIES.items():
            log_and_print(f"\n--- {cat} ---")

            for category_url in urls:
                sub_name = category_url.strip("/").split("/")[-1]
                log_and_print(f"  Podkategorija: {sub_name}")

                n = 1
                last_first_title = None

                while n <= MAX_PAGES:
                    page_url = f"{category_url}?p={n}"
                    log_and_print(f"    Stran {n}: {page_url}")

                    html = get_page_content(session, page_url, referer=category_url)
                    if not html:
                        break

                    soup = BeautifulSoup(html, "lxml")
                    container = soup.find("div", class_="list-items list-category-products")
                    if not container:
                        # fallback selector
                        container = soup.find("div", class_="list-items")
                        if not container:
                            break

                    items = container.find_all("div", class_="item")
                    if not items:
                        break

                    first_title_el = items[0].find("h4")
                    first_title = first_title_el.get_text(strip=True) if first_title_el else None
                    if n > 1 and first_title and first_title == last_first_title:
                        log_and_print("    Stran se ponavlja. Konec kategorije.")
                        break
                    last_first_title = first_title

                    product_urls = []
                    for it in items:
                        a = it.find("a", href=True)
                        if not a:
                            continue
                        href = a["href"]
                        if "/p/" not in href:
                            continue
                        product_urls.append(normalize_url(href))

                    # dedupe
                    seen = set()
                    product_urls = [u for u in product_urls if not (u in seen or seen.add(u))]

                    for product_url in product_urls:
                        log_and_print(f"      Izdelek: {product_url}")

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

                        if len(buffer) >= FLUSH_JSON_EVERY:
                            all_data = merge_by_url(all_data, buffer)
                            buffer = []
                            write_json(all_data, json_path)
                            log_and_print("Shranjen JSON (checkpoint).")

                        human_sleep(*BETWEEN_PRODUCTS_RANGE)

                    human_sleep(*BETWEEN_PAGES_RANGE)
                    n += 1

                if buffer:
                    all_data = merge_by_url(all_data, buffer)
                    buffer = []
                    write_json(all_data, json_path)
                    log_and_print("Shranjen JSON (podkategorija).")

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
