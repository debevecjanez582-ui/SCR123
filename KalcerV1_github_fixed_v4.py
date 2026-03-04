import pandas as pd
import os
from datetime import datetime
import time
import random
import re
import json
from typing import Optional, Dict, Any, List, Tuple
from itertools import product as cart_product
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ============================================================
# KALCER scraper (GitHub/CI friendly)
# ------------------------------------------------------------
# Dodatne funkcionalnosti (po dogovoru):
#  - Cene: vedno 2 decimalki
#  - EM normalizacija: če ni v whitelist -> kos
#  - EAN: pobere (ne validira dolžine)
#  - Dobava: "po dobavitelju" (Kalcer: best-effort kratka vrednost)
#  - Variants: opcije (npr. debeline) -> dodatne vrstice
#    * varno: cart-probe je privzeto IZKLOPLJEN (da ni sumljivo)
#  - Anti-bot: zazna captcha/verification strani in naredi backoff
#  - Polite scraping: stabilen UA na run, jitter sleep, checkpoint JSON, Excel 1x na koncu
# ============================================================

# --- Konfiguracija ---
SHOP_NAME = "Kalcer"
BASE_URL = "https://www.trgovina-kalcer.si"
DDV_RATE = 0.22

# Vedno Excel (kot želiš)
EXPORT_EXCEL = True

# varnost / "normalen" tempo (lahko overridaš z env)
SLEEP_MIN = float(os.environ.get("SCRAPE_SLEEP_MIN", "4.0"))      # ~5s povprečno
SLEEP_MAX = float(os.environ.get("SCRAPE_SLEEP_MAX", "6.0"))
START_JITTER_MIN = float(os.environ.get("SCRAPE_START_JITTER_MIN", "0.5"))
START_JITTER_MAX = float(os.environ.get("SCRAPE_START_JITTER_MAX", "3.0"))
BUFFER_FLUSH = int(os.environ.get("BUFFER_FLUSH", "50"))          # JSON pišemo v batchih
MAX_VARIANTS_PER_PRODUCT = int(os.environ.get("MAX_VARIANTS_PER_PRODUCT", "20"))

# opcijsko: če res rabiš 100% variantne cene in AJAX ne dela, lahko vklopiš cart-probe
USE_CART_PROBE = os.environ.get("KALCER_USE_CART_PROBE", "false").lower() == "true"

# če zaznamo block/captcha večkrat, raje preskočimo URL
MAX_BLOCK_RETRIES = int(os.environ.get("MAX_BLOCK_RETRIES", "3"))

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
]
_RUN_UA = random.choice(USER_AGENTS)

HEADERS = {
    "User-Agent": _RUN_UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "sl-SI,sl;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
}

# Celoten seznam kategorij
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

# --- EM whitelist (če ni na seznamu -> kos) ---
_ALLOWED_EM = {
    "ar","CAD","CHF","CZK","clet","dd","dlet","dan","EUR","GBP","ha","HRK","JPY","kam","kg","km","kwh","kw",
    "kpl","kos","kos dan","m3","m2","let","lit/dan","lit/h","lit/min","lit/s","L","m dan","m/dan","m/h","m/min","m/s",
    "m2 dan","m3/dan","m3/h","m3/min","m3/s","mes","min","oc","op","pal","par","%","s","SIT","SKK","slet",
    "t/dan","t/h","t/let","ted","m","tlet","tm","t","h","USD","wat","x","zvr","sto","skl","del","ključ","os",
    "cm","kN","km2","kg/m3","kg/h","kpl d","kpl h","m2 mes","m3 d","kg/l","os d","delež","kos mes","cu"
}

_log_file = None
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


def polite_sleep(min_s: float = None, max_s: float = None) -> None:
    lo = SLEEP_MIN if min_s is None else min_s
    hi = SLEEP_MAX if max_s is None else max_s
    time.sleep(random.uniform(lo, hi))


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

def create_output_paths(shop_name: str):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_root = os.environ.get("OUTPUT_DIR") or script_dir

    today = datetime.now().strftime("%Y-%m-%d")
    daily_dir = os.path.join(output_root, "Ceniki_Scraping", shop_name, today)
    os.makedirs(daily_dir, exist_ok=True)

    filename_date = datetime.now().strftime("%d_%m_%Y")
    json_path = os.path.join(daily_dir, f"{shop_name}_Podatki_{filename_date}.json")
    excel_path = os.path.join(daily_dir, f"{shop_name}_Podatki_{filename_date}.xlsx")
    log_path = os.path.join(daily_dir, f"{shop_name}_Scraping_Log_{datetime.now().strftime('%H-%M-%S')}.txt")
    return json_path, excel_path, log_path


_session = requests.Session()
_retry = Retry(
    total=3,
    connect=3,
    read=3,
    backoff_factor=1.2,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET", "POST"]),
    raise_on_status=False,
)
_adapter = HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=10)
_session.mount("https://", _adapter)
_session.mount("http://", _adapter)
_session.headers.update(HEADERS)


def safe_get(url: str, referer: Optional[str] = None, timeout: int = 30) -> Optional[str]:
    headers = dict(HEADERS)
    if referer:
        headers["Referer"] = referer

    for attempt in range(1, MAX_BLOCK_RETRIES + 1):
        polite_sleep()
        try:
            r = _session.get(url, headers=headers, timeout=timeout)

            if r.status_code == 403:
                wait = min(180, 15 * attempt + random.uniform(0, 15))
                log_and_print(f"HTTP 403 @ {url} -> sleep {wait:.1f}s")
                time.sleep(wait)
                continue

            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                wait = int(ra) if (ra and ra.isdigit()) else random.randint(30, 120)
                log_and_print(f"429 Too Many Requests -> backoff {wait}s ({url})")
                time.sleep(wait)

            if r.status_code in (500, 502, 503, 504):
                wait = random.randint(10, 60)
                log_and_print(f"{r.status_code} Server error -> backoff {wait}s ({url})")
                time.sleep(wait)

            if not r.ok:
                return None

            html = r.text or ""
            if is_block_page(html):
                wait = min(300, 30 * attempt + random.uniform(0, 30))
                log_and_print(f"[BLOCK] verification/captcha @ {url} -> sleep {wait:.1f}s")
                time.sleep(wait)
                continue

            return html
        except Exception as e:
            wait = min(90, 5 * attempt + random.uniform(0, 10))
            log_and_print(f"GET napaka: {e} ({url}) -> sleep {wait:.1f}s")
            time.sleep(wait)

    log_and_print(f"[BLOCK] Preveč poskusov, preskakujem URL: {url}")
    return None


def safe_post(url: str, data: dict, referer: Optional[str] = None, timeout: int = 30) -> Optional[str]:
    headers = dict(HEADERS)
    if referer:
        headers["Referer"] = referer

    for attempt in range(1, MAX_BLOCK_RETRIES + 1):
        polite_sleep()
        try:
            r = _session.post(url, headers=headers, data=data, timeout=timeout)

            if r.status_code == 403:
                wait = min(180, 15 * attempt + random.uniform(0, 15))
                log_and_print(f"HTTP 403 (POST) @ {url} -> sleep {wait:.1f}s")
                time.sleep(wait)
                continue

            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                wait = int(ra) if (ra and ra.isdigit()) else random.randint(30, 120)
                log_and_print(f"429 Too Many Requests (POST) -> backoff {wait}s ({url})")
                time.sleep(wait)

            if r.status_code in (500, 502, 503, 504):
                wait = random.randint(10, 60)
                log_and_print(f"{r.status_code} Server error (POST) -> backoff {wait}s ({url})")
                time.sleep(wait)

            if not r.ok:
                return None

            txt = r.text or ""
            if is_block_page(txt):
                wait = min(300, 30 * attempt + random.uniform(0, 30))
                log_and_print(f"[BLOCK] verification/captcha (POST) @ {url} -> sleep {wait:.1f}s")
                time.sleep(wait)
                continue

            return txt
        except Exception as e:
            wait = min(90, 5 * attempt + random.uniform(0, 10))
            log_and_print(f"POST napaka: {e} ({url}) -> sleep {wait:.1f}s")
            time.sleep(wait)

    log_and_print(f"[BLOCK] Preveč poskusov POST, preskakujem URL: {url}")
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


def price_without_vat(price_str: Optional[str]) -> str:
    v = _parse_float_any(price_str) if price_str else None
    if v is None:
        return ""
    return fmt_2dec(v / (1 + DDV_RATE))


def parse_price_any(text: str) -> str:
    if not text:
        return ""
    m = re.search(r"(\d{1,3}(?:\.\d{3})*,\d{2}|\d+(?:[.,]\d+)?)", text)
    return m.group(1).strip() if m else ""


def normalize_em(raw: str) -> str:
    if not raw:
        return "kos"
    u = str(raw).strip()
    u = u.replace("m²", "m2").replace("m³", "m3").replace("²", "2").replace("³", "3")
    u = re.sub(r"\s+", " ", u).strip()
    ul = u.lower()
    if ul in ("kos", "kosov", "kom", "komad", "pcs", "pc"):
        return "kos"
    if ul in _ALLOWED_EM:
        return ul
    if u in _ALLOWED_EM:
        return u
    return "kos"


def _item_key(item: dict) -> str:
    return f"{item.get('URL','')}|{item.get('Varianta','')}".strip("|")


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
    if not os.path.exists(json_path):
        return
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    df = pd.DataFrame(data)
    cols = [
        "Skupina", "Zap", "Oznaka / naziv", "EAN", "Opis", "Opis izdelka", "Varianta",
        "EM", "Valuta", "DDV", "Proizvajalec", "Veljavnost od", "Dobava",
        "Cena / EM (z DDV)", "Akcijska cena / EM (z DDV)",
        "Cena / EM (brez DDV)", "Akcijska cena / EM (brez DDV)",
        "URL", "SLIKA URL"
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    df[cols].to_excel(excel_path, index=False)
    log_and_print("Shranjen Excel (na koncu).")


def get_product_links_from_category(category_url: str) -> List[str]:
    all_links: List[str] = []
    page = 1
    while True:
        sep = "&" if "?" in category_url else "?"
        url = f"{category_url}{sep}page={page}"

        log_and_print(f"  Stran {page}: {url}")
        html = safe_get(url, referer=category_url)
        if not html:
            break

        soup = BeautifulSoup(html, "html.parser")
        products = soup.select(".product-list > div, .product-grid .product")
        if not products:
            break

        for item in products:
            a = item.select_one(".name a")
            if a and a.get("href"):
                all_links.append(urljoin(BASE_URL, a["href"]))

        text = soup.select_one(".pagination-results .text-right")
        if not text or "Prikazujem" not in text.get_text():
            break

        page += 1

    return list(dict.fromkeys(all_links))


def extract_option_groups(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    groups: List[Dict[str, Any]] = []

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

    if not groups:
        radios = soup.select('input[type="radio"][name^="option["]')
        bucket: Dict[str, Dict[str, Any]] = {}
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
    el = soup.select_one('input[name="product_id"]')
    if el and el.get("value"):
        return el.get("value").strip()
    m = re.search(r'name="product_id"\s+value="(\d+)"', html)
    return m.group(1) if m else ""


def try_price_from_option_text(base_price_str: str, option_text: str) -> str:
    base = _parse_float_any(base_price_str)
    if base is None:
        return ""
    m = re.search(r"([+-]\s*\d{1,3}(?:\.\d{3})*,\d{2})\s*€", option_text)
    if not m:
        return ""
    try:
        mod_val = float(m.group(1).replace(" ", "").replace(".", "").replace(",", "."))
        out = base + mod_val
        return fmt_2dec(out)
    except Exception:
        return ""


def try_ajax_variant_price(product_url: str, product_id: str, options_payload: dict) -> str:
    candidates = [
        f"{BASE_URL}/index.php?route=product/product/getPrice",
        f"{BASE_URL}/index.php?route=product/product/getprice",
        f"{BASE_URL}/index.php?route=product/product/price",
    ]
    payload = {"product_id": product_id, "quantity": "1"}
    payload.update(options_payload)

    for url in candidates:
        txt = safe_post(url, payload, referer=product_url)
        if not txt:
            continue
        try:
            js = json.loads(txt)
            for k in ("special", "price"):
                if k in js and js[k]:
                    v = _parse_float_any(parse_price_any(str(js[k])))
                    if v is not None:
                        return fmt_2dec(v)
        except Exception:
            pass
        v = _parse_float_any(parse_price_any(txt))
        if v is not None:
            return fmt_2dec(v)
    return ""


def cart_probe_variant_price(product_url: str, product_id: str, options_payload: dict, product_name_hint: str) -> str:
    if not USE_CART_PROBE:
        return ""
    s = requests.Session()
    s.headers.update(HEADERS)
    add_url = f"{BASE_URL}/index.php?route=checkout/cart/add"
    cart_url = f"{BASE_URL}/checkout/cart"
    payload = {"product_id": product_id, "quantity": "1"}
    payload.update(options_payload)

    try:
        time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
        r = s.post(add_url, data=payload, timeout=30, headers={"Referer": product_url, "User-Agent": _RUN_UA})
        if not r.ok:
            return ""
        txt = r.text
        try:
            js = json.loads(txt)
            if js.get("error"):
                return ""
        except Exception:
            pass
    except Exception:
        return ""

    try:
        time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
        r2 = s.get(cart_url, timeout=30, headers={"Referer": product_url, "User-Agent": _RUN_UA})
        if not r2.ok:
            return ""
        cart_html = r2.text
    except Exception:
        return ""

    soup = BeautifulSoup(cart_html, "html.parser")
    rowtxt = soup.get_text(" ", strip=True)
    v = _parse_float_any(parse_price_any(rowtxt))
    return fmt_2dec(v)


def extract_delivery_short(soup: BeautifulSoup) -> str:
    txt = soup.get_text("\n", strip=True).replace("\xa0", " ")
    tl = txt.lower()
    if "na zalogi" in tl:
        return "DA"
    if "ni na zalogi" in tl or "trenutno ni na zalogi" in tl:
        return "NE"
    m = re.search(r"dobava\s*[:\-]\s*([0-9]+\s*[-–]\s*[0-9]+\s*\w+)", tl, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return ""


def extract_ean_raw(soup: BeautifulSoup) -> str:
    m = soup.find("meta", attrs={"itemprop": "gtin13"})
    if m and m.get("content"):
        return m["content"].strip()
    for row in soup.select(".listing.stockMargin tr"):
        tds = row.select("td")
        if len(tds) != 2:
            continue
        k = tds[0].get_text(" ", strip=True).lower()
        v = tds[1].get_text(" ", strip=True).strip()
        if "ean" in k or "gtin" in k:
            return v
    txt = soup.get_text(" ", strip=True)
    mm = re.search(r"(EAN|GTIN)\s*[:#]?\s*([0-9]{6,20})", txt, flags=re.IGNORECASE)
    return mm.group(2).strip() if mm else ""


def get_product_details(product_url: str, subcat: str, date: str) -> List[Dict[str, Any]]:
    global _global_item_counter

    log_and_print(f"    - Detajli: {product_url}")
    html = safe_get(product_url, referer=product_url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")

    base: Dict[str, Any] = {
        "Skupina": subcat,
        "Veljavnost od": date,
        "Valuta": "EUR",
        "DDV": "22",
        "URL": product_url,
        "SLIKA URL": "",
        "Opis": "",
        "Opis izdelka": "",
        "Varianta": "",
        "Oznaka / naziv": "",
        "EM": "kos",
        "Cena / EM (z DDV)": "",
        "Cena / EM (brez DDV)": "",
        "Akcijska cena / EM (z DDV)": "",
        "Akcijska cena / EM (brez DDV)": "",
        "Proizvajalec": "",
        "EAN": "",
        "Dobava": "",
    }

    h1 = soup.select_one("h1.product-name") or soup.select_one("h1.productInfo") or soup.select_one("h1")
    if h1:
        base["Opis"] = h1.get_text(strip=True)

    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        base["Opis izdelka"] = meta_desc.get("content", "").strip()

    for row in soup.select(".listing.stockMargin tr"):
        cells = row.select("td")
        if len(cells) != 2:
            continue
        k = cells[0].get_text(" ", strip=True).lower()
        v = cells[1].get_text(" ", strip=True).strip()
        if "ident" in k:
            base["Oznaka / naziv"] = v
        elif "enota mere" in k:
            base["EM"] = normalize_em(v)
        elif "ean" in k or "gtin" in k:
            base["EAN"] = v

    brand = soup.select_one(".product-info .description a[href*='/m-']")
    if brand:
        base["Proizvajalec"] = brand.get_text(" ", strip=True)

    img = soup.select_one("a.lightbox-image")
    if img and img.get("href"):
        base["SLIKA URL"] = urljoin(BASE_URL, img["href"])

    base["Dobava"] = extract_delivery_short(soup)

    if not base["EAN"]:
        base["EAN"] = extract_ean_raw(soup)

    p = soup.select_one("span.productSpecialPrice") or soup.select_one(".price-new, .price")
    if p:
        base_price_raw = parse_price_any(p.get_text(" ", strip=True))
        base["Cena / EM (z DDV)"] = round_price_2dec(base_price_raw)
        base["Cena / EM (brez DDV)"] = price_without_vat(base["Cena / EM (z DDV)"])

    option_groups = extract_option_groups(soup)
    if not option_groups:
        _global_item_counter += 1
        one = dict(base)
        one["Zap"] = _global_item_counter
        return [one]

    combos = 1
    for g in option_groups:
        combos *= len(g["values"])
    if combos > MAX_VARIANTS_PER_PRODUCT:
        log_and_print(f"      [WARN] preveč kombinacij ({combos}) -> shranim samo base")
        _global_item_counter += 1
        one = dict(base)
        one["Zap"] = _global_item_counter
        return [one]

    product_id = extract_product_id(html, soup)

    value_lists = []
    for g in option_groups:
        value_lists.append([(g["name"], v["id"], g["label"], v["text"]) for v in g["values"]])

    results: List[Dict[str, Any]] = []
    for combo in cart_product(*value_lists):
        options_payload = {name: vid for (name, vid, _, _) in combo}
        variant_label = ", ".join([f"{lab}: {txt}".strip(": ") for (_, _, lab, txt) in combo])

        d = dict(base)
        d["Varianta"] = variant_label
        if d.get("Opis"):
            d["Opis"] = f"{d['Opis']} ({variant_label})"

        variant_price = ""

        if base.get("Cena / EM (z DDV)"):
            for (_, _, _, txt) in combo:
                variant_price = try_price_from_option_text(base["Cena / EM (z DDV)"], txt)
                if variant_price:
                    break

        if not variant_price and product_id:
            variant_price = try_ajax_variant_price(product_url, product_id, options_payload)

        if not variant_price and USE_CART_PROBE and product_id:
            variant_price = cart_probe_variant_price(product_url, product_id, options_payload, base.get("Opis", ""))

        if variant_price:
            d["Cena / EM (z DDV)"] = round_price_2dec(variant_price)
            d["Cena / EM (brez DDV)"] = price_without_vat(d["Cena / EM (z DDV)"])

        _global_item_counter += 1
        d["Zap"] = _global_item_counter
        results.append(d)

    return results


def main():
    global _log_file, _global_item_counter

    time.sleep(random.uniform(START_JITTER_MIN, START_JITTER_MAX))

    json_path, excel_path, log_path = create_output_paths(SHOP_NAME)
    try:
        _log_file = open(log_path, "w", encoding="utf-8")
    except Exception:
        return

    log_and_print(f"--- Zagon {SHOP_NAME} ---")
    log_and_print(f"User-Agent (stabilen za ta zagon): {_RUN_UA}")
    log_and_print(f"SLEEP=[{SLEEP_MIN},{SLEEP_MAX}] BUFFER_FLUSH={BUFFER_FLUSH} EXCEL(end)={EXPORT_EXCEL} USE_CART_PROBE={USE_CART_PROBE}")

    if os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                d = json.load(f)
            if d:
                _global_item_counter = max((int(x.get("Zap", 0)) for x in d if isinstance(x, dict)), default=0)
        except Exception:
            pass

    date = datetime.now().strftime("%d/%m/%Y")
    buffer: List[Dict[str, Any]] = []

    try:
        for _, urls in KALCER_CATEGORIES.items():
            for category_url in urls:
                sub_name = category_url.strip("/").split("/")[-1]
                log_and_print(f"\n--- Podkategorija: {sub_name} ---")

                links = get_product_links_from_category(category_url)

                for link in links:
                    recs = get_product_details(link, sub_name, date)
                    if recs:
                        buffer.extend(recs)

                    if len(buffer) >= BUFFER_FLUSH:
                        save_data_append(buffer, json_path)
                        buffer = []

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
