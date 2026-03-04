import os
import re
import json
import time
import random
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
from urllib.parse import urljoin
from itertools import product as cart_product

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ============================================================
# KALCER scraper (GitHub/CI friendly) - v5 (stable)
# ------------------------------------------------------------
# Glavne lastnosti:
#  - OUTPUT_DIR/Ceniki_Scraping/<SHOP>/<YYYY-MM-DD>/...
#  - JSON checkpoint v batchih + Excel 1x na koncu
#  - cene round na 2 decimalki
#  - EM normalizacija (če ni whitelist -> kos)
#  - EAN raw (brez validacije dolžine)
#  - varnejši "BLOCK" detector (ne lažno na reCAPTCHA)
#  - retry/backoff pri 429/5xx + continue
#  - variantni izdelki (select/radio)
# ============================================================

SHOP_NAME = "Kalcer"
BASE_URL = "https://www.trgovina-kalcer.si"
DDV_RATE = 0.22

# Vedno Excel (kot želiš)
EXPORT_EXCEL = True

# Tempo (lahko overridaš v GitHub Actions env)
SLEEP_MIN = float(os.environ.get("SCRAPE_SLEEP_MIN", "4.0"))
SLEEP_MAX = float(os.environ.get("SCRAPE_SLEEP_MAX", "6.0"))

BUFFER_FLUSH = int(os.environ.get("BUFFER_FLUSH", "50"))
MAX_PAGES = int(os.environ.get("MAX_PAGES", "250"))

# Zaščita proti preveč variantam (kombinacijam opcij)
MAX_VARIANTS_PER_PRODUCT = int(os.environ.get("MAX_VARIANTS_PER_PRODUCT", "20"))

# Občasni počitek (bolj “naravno”)
BREAK_EVERY_PRODUCTS = int(os.environ.get("BREAK_EVERY_PRODUCTS", "140"))
BREAK_SLEEP_MIN = float(os.environ.get("BREAK_SLEEP_MIN", "20"))
BREAK_SLEEP_MAX = float(os.environ.get("BREAK_SLEEP_MAX", "90"))

# Kolikokrat retry-a, če zazna block/challenge
MAX_BLOCK_RETRIES = int(os.environ.get("MAX_BLOCK_RETRIES", "3"))

# Variante: cart-probe je “najbolj sumljiv”, zato privzeto OFF
ENABLE_CART_PROBE = os.environ.get("KALCER_ENABLE_CART_PROBE", "0").strip().lower() in ("1", "true", "yes", "y")
MAX_CART_PROBES_PER_PRODUCT = int(os.environ.get("MAX_CART_PROBES_PER_PRODUCT", "3"))

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
]

UA_THIS_RUN = random.choice(USER_AGENTS)

HEADERS = {
    "User-Agent": UA_THIS_RUN,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "sl-SI,sl;q=0.9,en;q=0.7,en-US;q=0.5",
    "Connection": "keep-alive",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
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

# --- Kategorije (tvoj seznam) ---
KALCER_CATEGORIES = {
    "Gradnja": [
        "https://www.trgovina-kalcer.si/gradnja/izolacije/fasadni-izdelki/fasadne-izolacije",
        "https://www.trgovina-kalcer.si/gradnja/izolacije/fasadni-izdelki/fasadna-lepila-in-malte",
        "https://www.trgovina-kalcer.si/gradnja/izolacije/fasadni-izdelki/fasadne-barve-in-zakljucni-sloji",
        "https://www.trgovina-kalcer.si/gradnja/izolacije/fasadni-izdelki/fasadna-sidra",
        "https://www.trgovina-kalcer.si/gradnja/izolacije/fasadni-izdelki/fasadne-mrezice-in-profili",
        "https://www.trgovina-kalcer.si/gradnja/izolacije/fasadni-izdelki/fasadne-stukature",
        "https://www.trgovina-kalcer.si/gradnja/izolacije/toplotne-izolacije/steklena-izolacija",
        "https://www.trgovina-kalcer.si/gradnja/izolacije/toplotne-izolacije/kamena-izolacija",
        "https://www.trgovina-kalcer.si/gradnja/izolacije/toplotne-izolacije/izolacijske-plosce",
        "https://www.trgovina-kalcer.si/gradnja/izolacije/toplotne-izolacije/izolacijska-folija",
        "https://www.trgovina-kalcer.si/gradnja/izolacije/toplotne-izolacije/izolacijsko-nasutje",
        "https://www.trgovina-kalcer.si/gradnja/izolacije/folije-za-izolacijo",
        "https://www.trgovina-kalcer.si/gradnja/izolacije/izolacijski-lepilni-trakovi",
        "https://www.trgovina-kalcer.si/gradnja/izolacije/izolacijska-tesnila",
        "https://www.trgovina-kalcer.si/gradnja/izolacije/pozarni-izdelki-plosce",
        "https://www.trgovina-kalcer.si/gradnja/suhomontazni-sistemi/gradbene-plosce-gradnja",
        "https://www.trgovina-kalcer.si/gradnja/suhomontazni-sistemi/konstrukcija",
        "https://www.trgovina-kalcer.si/gradnja/suhomontazni-sistemi/pribor-za-suhi-estrih",
        "https://www.trgovina-kalcer.si/gradnja/suhomontazni-sistemi/suhi-estrihi",
        "https://www.trgovina-kalcer.si/gradnja/suhomontazni-sistemi/podlage-za-suhi-estrih",
        "https://www.trgovina-kalcer.si/gradnja/suhomontazni-sistemi/ogrevanje-hlajenje/talno-ogrevanje-hlajenje",
        "https://www.trgovina-kalcer.si/gradnja/suhomontazni-sistemi/ogrevanje-hlajenje/stensko-in-stropno-ogrevanje-hlajenje",
        "https://www.trgovina-kalcer.si/gradnja/pripomocki-suha-gradnja/svetila",
        "https://www.trgovina-kalcer.si/gradnja/pripomocki-suha-gradnja/pripomocki-pritrjevanje-suha-gradnja",
        "https://www.trgovina-kalcer.si/gradnja/pripomocki-suha-gradnja/fugiranje-armiranje/mase",
        "https://www.trgovina-kalcer.si/gradnja/pripomocki-suha-gradnja/fugiranje-armiranje/trakovi",
        "https://www.trgovina-kalcer.si/gradnja/pripomocki-suha-gradnja/fugiranje-armiranje/vogalniki",
        "https://www.trgovina-kalcer.si/gradnja/pripomocki-suha-gradnja/revizijske-odprtine",
        "https://www.trgovina-kalcer.si/gradnja/pripomocki-suha-gradnja/ciscenje",
        "https://www.trgovina-kalcer.si/gradnja/pripomocki-suha-gradnja/barvanje",
        "https://www.trgovina-kalcer.si/gradnja/lepljenje-tesnenje/hidroizolacije/stresne-folije",
        "https://www.trgovina-kalcer.si/gradnja/lepljenje-tesnenje/hidroizolacije/tekoce-brezsivne-folije",
        "https://www.trgovina-kalcer.si/gradnja/lepljenje-tesnenje/hidroizolacije/bitumenske-hidroizolacije",
        "https://www.trgovina-kalcer.si/gradnja/lepljenje-tesnenje/hidroizolacije/cementne-hidroizolacije",
        "https://www.trgovina-kalcer.si/gradnja/lepljenje-tesnenje/izravnalne-mase",
        "https://www.trgovina-kalcer.si/gradnja/lepljenje-tesnenje/radonska-zascita",
        "https://www.trgovina-kalcer.si/gradnja/lepljenje-tesnenje/tesnilne-mase",
        "https://www.trgovina-kalcer.si/gradnja/lepljenje-tesnenje/tesnilni-trakovi",
        "https://www.trgovina-kalcer.si/gradnja/lepljenje-tesnenje/lepila",
        "https://www.trgovina-kalcer.si/gradnja/gradbena-akustika/zvocni-absorberji",
        "https://www.trgovina-kalcer.si/gradnja/gradbena-akustika/zvocne-izolacije",
        "https://www.trgovina-kalcer.si/gradnja/gradbena-akustika/modularni-stropi",
        "https://www.trgovina-kalcer.si/gradnja/gradbena-akustika/akusticni-pribor",
    ]
}

# --- Output columns ---
EXCEL_COLS = [
    "Skupina",
    "Zap",
    "Oznaka / naziv",
    "EAN",
    "Opis",
    "Opis izdelka",
    "Varianta",
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

_session = requests.Session()
_session.headers.update(HEADERS)


# -----------------------------
# Helpers: logging / sleep
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


def save_debug_html(kind: str, url: str, html: str) -> None:
    """Shrani HTML, ko naletimo na BLOCK/challenge, za diagnozo."""
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
    return json_path, excel_path, log_path, daily_dir


# -----------------------------
# Block detector (NO false-positive on "reCAPTCHA")
# -----------------------------
def is_block_page(html: str) -> bool:
    if not html:
        return False
    t = html.lower()

    strong_needles = [
        "/cdn-cgi/challenge-platform",  # Cloudflare
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

    # captcha widget indikatorji: samo, če je zraven še “challenge” kontekst
    if re.search(r"\b(hcaptcha|cf-turnstile|g-recaptcha|px-captcha|data-sitekey)\b", t):
        if any(x in t for x in ("verify", "verifying", "blocked", "access denied", "challenge")):
            return True

    # soft heuristika: več signalov skupaj
    soft = 0
    for n in ("captcha", "challenge", "bot", "blocked", "verify", "verification"):
        if n in t:
            soft += 1
    return soft >= 4


# -----------------------------
# Networking (retry/backoff)
# -----------------------------
def build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=2,  # mi delamo še svoj loop -> naj bo zmeren
        connect=2,
        read=2,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update(HEADERS)
    return s


def get_page_content(session: requests.Session, url: str, referer: Optional[str] = None) -> Optional[str]:
    headers = dict(HEADERS)
    if referer:
        headers["Referer"] = referer

    for attempt in range(1, MAX_BLOCK_RETRIES + 1):
        human_sleep()

        try:
            resp = session.get(url, headers=headers, timeout=30)

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

            if resp.status_code == 403:
                wait = min(180, 15 * attempt + random.uniform(0, 20))
                log_and_print(f"HTTP 403 @ {url} -> sleep {wait:.1f}s")
                time.sleep(wait)
                continue

            if not resp.ok:
                log_and_print(f"HTTP {resp.status_code} @ {url}")
                return None

            html = resp.text or ""
            if is_block_page(html):
                wait = min(300, 30 * attempt + random.uniform(0, 30))
                log_and_print(f"[BLOCK] verification/challenge @ {url} -> sleep {wait:.1f}s (poskus {attempt}/{MAX_BLOCK_RETRIES})")
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


def post_form(session: requests.Session, url: str, data: dict, referer: Optional[str] = None) -> Tuple[Optional[str], Optional[int]]:
    headers = dict(HEADERS)
    if referer:
        headers["Referer"] = referer

    for attempt in range(1, MAX_BLOCK_RETRIES + 1):
        human_sleep()
        try:
            resp = session.post(url, data=data, headers=headers, timeout=30)

            if resp.status_code == 429:
                ra = resp.headers.get("Retry-After")
                wait = int(ra) if (ra and ra.isdigit()) else random.randint(30, 120)
                log_and_print(f"HTTP 429 (POST) -> backoff {wait}s: {url}")
                time.sleep(wait)
                continue

            if resp.status_code in (500, 502, 503, 504):
                wait = random.randint(10, 60)
                log_and_print(f"HTTP {resp.status_code} (POST) -> backoff {wait}s: {url}")
                time.sleep(wait)
                continue

            if not resp.ok:
                log_and_print(f"HTTP {resp.status_code} (POST) @ {url}")
                return None, resp.status_code

            html = resp.text or ""
            if is_block_page(html):
                wait = min(300, 30 * attempt + random.uniform(0, 30))
                log_and_print(f"[BLOCK] challenge (POST) @ {url} -> sleep {wait:.1f}s")
                save_debug_html("block_post", url, html)
                time.sleep(wait)
                continue

            return html, resp.status_code

        except requests.RequestException as e:
            wait = min(90, 5 * attempt + random.uniform(0, 10))
            log_and_print(f"POST error @ {url}: {e} -> sleep {wait:.1f}s")
            time.sleep(wait)

    return None, None


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


def parse_price_any(text: str) -> str:
    if not text:
        return ""
    m = re.search(r"(\d{1,3}(?:\.\d{3})*,\d{2}|\d+(?:[.,]\d+)?)", text)
    return m.group(1).strip() if m else ""


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
    """EAN raw (brez validacije dolžine)."""
    # poskusi meta
    for prop in ("gtin13", "gtin14", "gtin12", "gtin", "ean"):
        m = soup.find("meta", attrs={"itemprop": prop})
        if m and m.get("content"):
            return m.get("content").strip()

    txt = soup.get_text(" ", strip=True)
    m2 = re.search(r"(EAN|GTIN)\s*[:#]?\s*([0-9]{6,20})", txt, flags=re.IGNORECASE)
    return m2.group(2).strip() if m2 else ""


def extract_long_description(soup: BeautifulSoup) -> str:
    """Opis izdelka (daljši) – best-effort."""
    # meta description
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        return meta_desc.get("content").strip()

    # opisni blok
    cand = soup.select_one(".product-info .description") or soup.select_one("#tab-description") or soup.select_one(".tab-content")
    if cand:
        txt = cand.get_text("\n", strip=True)
        if txt and len(txt) > 20:
            return (txt[:8000].rstrip() + "…") if len(txt) > 8000 else txt
    return ""


def extract_delivery_short(soup: BeautifulSoup) -> str:
    """Dobava (kratko): DA/NE ali '1-5 dni' če najde."""
    txt = soup.get_text(" ", strip=True).replace("\xa0", " ")
    tl = txt.lower()

    if "ni na zalogi" in tl or "trenutno ni na zalogi" in tl:
        return "NE"
    if "na zalogi" in tl:
        return "DA"

    # npr. "1-10 delovnih dni"
    m = re.search(r"(\d+\s*[-–]\s*\d+\s*(?:delovnih\s+)?dni)", tl)
    if m:
        return m.group(1).strip()

    return ""


def extract_option_groups(soup: BeautifulSoup) -> List[dict]:
    """
    OpenCart-style opcije:
    Vrne:
      [{"label":"Debelina", "name":"option[123]", "values":[{"id":"456","text":"50 mm"}, ...]}, ...]
    """
    groups: List[dict] = []

    # SELECT opcije
    for sel in soup.select('select[name^="option["]'):
        name = sel.get("name")
        label = ""
        fg = sel.find_parent(class_="form-group")
        if fg:
            lab = fg.select_one("label")
            if lab:
                label = lab.get_text(" ", strip=True)

        values = []
        for opt in sel.select("option"):
            vid = (opt.get("value") or "").strip()
            txt = opt.get_text(" ", strip=True)
            if vid and vid != "0":
                values.append({"id": vid, "text": txt})
        if name and values:
            groups.append({"label": label or name, "name": name, "values": values})

    # RADIO opcije
    if not groups:
        radios = soup.select('input[type="radio"][name^="option["]')
        bucket: Dict[str, dict] = {}
        for r in radios:
            name = r.get("name")
            vid = (r.get("value") or "").strip()
            if not name or not vid:
                continue
            labtxt = ""
            pl = r.find_parent("label")
            if pl:
                labtxt = pl.get_text(" ", strip=True)
            bucket.setdefault(name, {"label": name, "name": name, "values": []})
            bucket[name]["values"].append({"id": vid, "text": labtxt or vid})
        groups = list(bucket.values())

    return [g for g in groups if g.get("values")]


def extract_product_id(html: str, soup: BeautifulSoup) -> str:
    pid = ""
    el = soup.select_one('input[name="product_id"]')
    if el and el.get("value"):
        pid = el.get("value").strip()
    if pid:
        return pid

    m = re.search(r'name="product_id"\s+value="(\d+)"', html)
    if m:
        return m.group(1)

    m = re.search(r"product_id\s*[:=]\s*['\"](\d+)['\"]", html)
    if m:
        return m.group(1)

    return ""


def try_price_from_option_text(base_price_str: str, option_text: str) -> str:
    """Če option_text vsebuje (+10,00€) ali (-5,00€), izračunamo varianto brez dodatnih requestov."""
    base_v = _parse_float_any(base_price_str)
    if base_v is None:
        return ""

    m = re.search(r"([+-]\s*\d{1,3}(?:\.\d{3})*,\d{2})\s*€", option_text)
    if not m:
        return ""

    try:
        mod_v = _parse_float_any(m.group(1))
        if mod_v is None:
            return ""
        return fmt_2dec(base_v + mod_v)
    except Exception:
        return ""


def try_ajax_variant_price(session: requests.Session, product_url: str, product_id: str, options_payload: dict) -> str:
    """Poskusi tipične OpenCart endpoint-e za dinamično ceno."""
    candidates = [
        f"{BASE_URL}/index.php?route=product/product/getPrice",
        f"{BASE_URL}/index.php?route=product/product/getprice",
        f"{BASE_URL}/index.php?route=product/product/price",
    ]

    payload = {"product_id": product_id, "quantity": "1"}
    payload.update(options_payload)

    for url in candidates:
        txt, status = post_form(session, url, payload, referer=product_url)
        if not txt or status is None:
            continue

        # JSON?
        try:
            js = json.loads(txt)
            for k in ("special", "price"):
                if k in js and js[k]:
                    p = parse_price_any(str(js[k]))
                    if p:
                        return round_price_2dec(p)
        except Exception:
            pass

        # HTML?
        p = parse_price_any(txt)
        if p:
            return round_price_2dec(p)

    return ""


def cart_probe_variant_price(product_url: str, product_id: str, options_payload: dict, product_name_hint: str) -> str:
    """
    Fallback: doda varianto v košarico in prebere ceno iz košarice.
    To je bolj “sumljivo”, zato je privzeto izklopljeno.
    """
    s = build_session()

    add_url = f"{BASE_URL}/index.php?route=checkout/cart/add"
    cart_url = f"{BASE_URL}/checkout/cart"

    payload = {"product_id": product_id, "quantity": "1"}
    payload.update(options_payload)

    txt, status = post_form(s, add_url, payload, referer=product_url)
    if not txt:
        return ""

    try:
        js = json.loads(txt)
        if js.get("error"):
            return ""
    except Exception:
        pass

    cart_html = get_page_content(s, cart_url, referer=product_url)
    if not cart_html:
        return ""

    soup = BeautifulSoup(cart_html, "html.parser")
    rows = soup.select("table tbody tr")
    if not rows:
        return round_price_2dec(parse_price_any(soup.get_text(" ", strip=True)))

    ph = (product_name_hint or "").strip().lower()
    target = None
    for r in rows:
        if ph and ph in r.get_text(" ", strip=True).lower():
            target = r
            break
    if not target:
        target = rows[0]

    rowtxt = target.get_text(" ", strip=True)
    m = re.search(r"(\d{1,3}(?:\.\d{3})*,\d{2})\s*€", rowtxt)
    if m:
        return round_price_2dec(m.group(1))

    return round_price_2dec(parse_price_any(rowtxt))


# -----------------------------
# Save helpers (JSON batch + Excel end)
# -----------------------------
def _item_key(item: dict) -> str:
    # URL ni več unikat (varianta!)
    return f"{item.get('URL','')}|{item.get('Varianta','')}".strip("|")


def save_data_append(new_data: List[Dict[str, Any]], json_path: str) -> None:
    """JSON shranjujemo sproti (v batchih)."""
    if not new_data:
        return

    all_data: List[Dict[str, Any]] = []
    if os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                all_data = json.load(f)
        except Exception:
            all_data = []

    data_dict = {_item_key(x): x for x in all_data if isinstance(x, dict)}
    for x in new_data:
        if isinstance(x, dict):
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
    """Excel delamo 1x na koncu (hitreje + manj disk IO)."""
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


# -----------------------------
# Listing -> product URLs
# -----------------------------
def get_product_links_from_category(session: requests.Session, category_url: str) -> List[str]:
    """Pobere produktne linke iz kategorije (stran po stran)."""
    all_links: List[str] = []
    last_first_href = None

    for page in range(1, MAX_PAGES + 1):
        sep = "&" if "?" in category_url else "?"
        url = f"{category_url}{sep}page={page}"

        log_and_print(f"  Stran {page}: {url}")
        html = get_page_content(session, url, referer=category_url)
        if not html:
            break

        soup = BeautifulSoup(html, "html.parser")
        products = soup.select(".product-list > div, .product-grid .product")
        if not products:
            break

        first_a = products[0].select_one(".name a")
        first_href = first_a.get("href") if first_a else None
        if page > 1 and first_href and last_first_href and first_href == last_first_href:
            log_and_print("  Stran se ponavlja. Konec kategorije.")
            break
        last_first_href = first_href

        for item in products:
            a = item.select_one(".name a")
            if a and a.get("href"):
                href = a["href"]
                full = href if href.startswith("http") else urljoin(BASE_URL, href)
                all_links.append(full)

        text = soup.select_one(".pagination-results .text-right")
        if not text or "Prikazujem" not in text.get_text():
            break

    # unique, ohrani vrstni red
    return list(dict.fromkeys(all_links))


# -----------------------------
# Product details (returns list: [base] or [variants...])
# -----------------------------
def get_product_details(session: requests.Session, url: str, sub_name: str, date_str: str) -> List[Dict[str, Any]]:
    global _global_item_counter

    log_and_print(f"    - Detajli: {url}")
    html = get_page_content(session, url, referer=url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")

    base: Dict[str, Any] = {
        "Skupina": sub_name,
        "Zap": 0,
        "Oznaka / naziv": "",
        "EAN": "",
        "Opis": "",
        "Opis izdelka": "",
        "Varianta": "",
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

    # Naziv
    h1 = soup.select_one("h1.product-name") or soup.select_one("h1.productInfo") or soup.select_one("h1")
    if h1:
        base["Opis"] = h1.get_text(" ", strip=True)

    # Opis izdelka (daljši)
    base["Opis izdelka"] = extract_long_description(soup)

    # Ident / enota mere (tabela)
    for row in soup.select(".listing.stockMargin tr"):
        cells = row.select("td")
        if len(cells) != 2:
            continue
        k = cells[0].get_text(" ", strip=True).lower()
        v = cells[1].get_text(" ", strip=True).strip()
        if "ident" in k:
            base["Oznaka / naziv"] = v  # brez "Št. art" prefiksa, OK
        elif "enota mere" in k:
            base["EM"] = normalize_em(v)
        elif ("ean" in k or "gtin" in k) and not base["EAN"]:
            base["EAN"] = v  # raw

    # EAN fallback (raw)
    if not base["EAN"]:
        base["EAN"] = extract_ean_raw(soup)

    # Proizvajalec
    brand = soup.select_one(".product-info .description a[href*='/m-']")
    if brand:
        base["Proizvajalec"] = brand.get_text(" ", strip=True)[:250]

    # Dobava (kratko)
    base["Dobava"] = extract_delivery_short(soup)

    # Slika
    img = soup.select_one("a.lightbox-image")
    if img and img.get("href"):
        base["SLIKA URL"] = img["href"] if img["href"].startswith("http") else urljoin(BASE_URL, img["href"])

    # Cena (base)
    p = soup.select_one("span.productSpecialPrice") or soup.select_one(".price-new, .price")
    if p:
        base_price = parse_price_any(p.get_text(" ", strip=True))
        base["Cena / EM (z DDV)"] = round_price_2dec(base_price)

    base["Cena / EM (brez DDV)"] = convert_price_to_without_vat(base.get("Cena / EM (z DDV)"), DDV_RATE)

    # Opcije (variante)
    option_groups = extract_option_groups(soup)
    if not option_groups:
        _global_item_counter += 1
        one = dict(base)
        one["Zap"] = _global_item_counter
        # EM final normalize
        one["EM"] = normalize_em(one.get("EM") or "kos")
        return [one]

    # zaščita proti eksploziji kombinacij
    combos = 1
    for g in option_groups:
        combos *= len(g["values"])
    if combos > MAX_VARIANTS_PER_PRODUCT:
        log_and_print(f"      [WARN] preveč kombinacij ({combos}) -> shranim samo base")
        _global_item_counter += 1
        one = dict(base)
        one["Zap"] = _global_item_counter
        one["EM"] = normalize_em(one.get("EM") or "kos")
        return [one]

    product_id = extract_product_id(html, soup)

    value_lists = []
    for g in option_groups:
        value_lists.append([(g["name"], v["id"], g["label"], v["text"]) for v in g["values"]])

    results: List[Dict[str, Any]] = []
    cart_probes_used = 0

    for combo in cart_product(*value_lists):
        options_payload = {name: vid for (name, vid, _, _) in combo}
        variant_label = ", ".join([f"{lab}: {txt}".strip(": ") for (_, _, lab, txt) in combo])

        d = dict(base)
        d["Varianta"] = variant_label

        # Cena variante – prioriteta: modifier v tekstu -> AJAX -> (opcijsko) cart
        variant_price = ""

        if base.get("Cena / EM (z DDV)"):
            for (_, _, _, txt) in combo:
                variant_price = try_price_from_option_text(base["Cena / EM (z DDV)"], txt)
                if variant_price:
                    break

        if not variant_price and product_id:
            variant_price = try_ajax_variant_price(session, url, product_id, options_payload)

        if not variant_price and product_id and ENABLE_CART_PROBE and cart_probes_used < MAX_CART_PROBES_PER_PRODUCT:
            cart_probes_used += 1
            variant_price = cart_probe_variant_price(url, product_id, options_payload, base.get("Opis", ""))

        if variant_price:
            d["Cena / EM (z DDV)"] = round_price_2dec(variant_price)
            d["Cena / EM (brez DDV)"] = convert_price_to_without_vat(d["Cena / EM (z DDV)"], DDV_RATE)

        _global_item_counter += 1
        d["Zap"] = _global_item_counter
        d["EM"] = normalize_em(d.get("EM") or "kos")
        results.append(d)

    return results


# -----------------------------
# Main
# -----------------------------
def main():
    global _log_file, _debug_dir, _global_item_counter

    # start jitter (da CI ne udari vedno v isti sekundi)
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
    log_and_print(f"SLEEP=[{SLEEP_MIN},{SLEEP_MAX}] BUFFER_FLUSH={BUFFER_FLUSH} EXCEL(end)={EXPORT_EXCEL} MAX_PAGES={MAX_PAGES} CART_PROBE={ENABLE_CART_PROBE}")

    session = build_session()

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
    processed_products = 0

    try:
        for cat, urls in KALCER_CATEGORIES.items():
            log_and_print(f"\n=== {cat} ===")

            for category_url in urls:
                sub_name = category_url.strip("/").split("/")[-1]
                log_and_print(f"\n-- Podkategorija: {sub_name}")

                product_links = get_product_links_from_category(session, category_url)

                for link in product_links:
                    recs = get_product_details(session, link, sub_name, date_str)
                    if recs:
                        buffer.extend(recs)
                        processed_products += 1

                    if len(buffer) >= BUFFER_FLUSH:
                        save_data_append(buffer, json_path)
                        buffer = []

                    # občasni počitek
                    if processed_products > 0 and processed_products % BREAK_EVERY_PRODUCTS == 0:
                        wait = random.uniform(BREAK_SLEEP_MIN, BREAK_SLEEP_MAX)
                        log_and_print(f"PAUSE: {processed_products} izdelkov -> počitek {wait:.1f}s")
                        time.sleep(wait)

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
