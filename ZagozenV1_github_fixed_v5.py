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
# ZAGOŽEN scraper (GitHub/CI friendly) - refactor v5
# ------------------------------------------------------------
# Usklajeno z logiko OBI/Kalcer/Tehnoles/Slovenijales:
#  - stabilen User-Agent na run
#  - retry/backoff (429/5xx) + captcha/verification detekcija
#  - "polite scraping": jitter sleep + občasni počitek
#  - JSON checkpoint v batchih, Excel 1x na koncu
#  - cene vedno 2 decimalki
#  - EM normalizacija: če ni v whitelist -> kos
#  - EAN pobere (ne validira dolžine)
#  - Dobava: "kratko" (npr. '1-10 delovnih dni' ali DA/NE), brez dolgih razlag
#  - Opis izdelka: meta description ali blok opisa (heuristika)
# ============================================================

SHOP_NAME = "Zagozen"
BASE_URL = "https://eshop-zagozen.si/"
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

# Kategorije (slugi) – prevzeto iz tvoje skripte
CATEGORIES = {
    "vodovod": [
        "zbiralniki-za-vodo-aquastay-in-oprema",
        "vodomerni-termo-jaski-in-oprema",
        "vodovodne-pe-cevi-in-spojke",
        "spojke-za-popravila",
        "pocinkani-fitingi-protipovratni-in-krogelni-ventili",
        "hisni-prikljucki-za-vodovod",
        "ventili-za-redukcijo-tlaka",
        "ploscata-tesnila",
        "dodatno",
    ],
    "kanalizacija": [
        "kanalizacijske-cevi-in-fazoni",
        "kanalizacijski-jaski-in-oprema",
        "lovilci-olj-in-mascob",
        "cistilne-naprave-in-oprema",
        "ponikovalna-polja",
        "drenazne-cevi",
        "greznice",
        "kanalizacijski-pokrovi-resetke-in-oprema",
        "opozorilni-trakovi",
        "crpalni-jaski",
    ],
    "zascita-in-energetika": [
        "pe-cevi-za-zascito-aflex-in-spojke",
        "pvc-energetske-cevi",
        "opozorilni-trakovi",
    ],
}

# --- EM whitelist (če ni na seznamu -> kos) ---
_ALLOWED_EM = {
    "ar","CAD","CHF","CZK","clet","dd","dlet","dan","EUR","GBP","ha","HRK","JPY","kam","kg","km","kwh","kw",
    "kpl","kos","kos dan","m3","m2","let","lit/dan","lit/h","lit/min","lit/s","L","m dan","m/dan","m/h","m/min","m/s",
    "m2 dan","m3/dan","m3/h","m3/min","m3/s","mes","min","oc","op","pal","par","%","s","SIT","SKK","slet",
    "t/dan","t/h","t/let","ted","m","tlet","tm","t","h","USD","wat","x","zvr","sto","skl","del","ključ","os",
    "cm","kN","km2","kg/m3","kg/h","kpl d","kpl h","m2 mes","m3 d","kg/l","os d","delež","kos mes","cu"
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

    # pogoste variante
    if ul in ("kosov", "kos", "kom", "komad", "pcs", "pc"):
        return "kos"

    if u in _ALLOWED_EM:
        return u
    if ul in _ALLOWED_EM:
        return ul

    return "kos"


def parse_price_any(text: str) -> str:
    """Ujame npr. 46,34 ali 1.234,56 ali 46.34."""
    if not text:
        return ""
    m = re.search(r"(\d{1,3}(?:\.\d{3})*,\d{2}|\d+(?:[.,]\d+)?)", text)
    return m.group(1).strip() if m else ""


# -----------------------------
# Save/load helpers
# -----------------------------
def _item_key(item: dict) -> str:
    """Zagožen: preferiramo šifro (Oznaka/naziv), sicer URL."""
    sku = str(item.get("Oznaka / naziv") or "").strip()
    if sku:
        return f"ID_{sku}"
    return f"URL_{item.get('URL') or ''}"


def save_data_append(new_data: List[Dict[str, Any]], json_path: str) -> None:
    """JSON shranjujemo v batchih; dedupe po key (ID ali URL)."""
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
# Category listing -> product URLs
# -----------------------------
def get_product_links_from_subcategory(session: requests.Session, category_slug: str, subcategory_slug: str) -> List[str]:
    links: List[str] = []
    last_first = None

    for page in range(1, MAX_PAGES + 1):
        if page == 1:
            url = f"{BASE_URL}{category_slug}/{subcategory_slug}"
        else:
            url = f"{BASE_URL}{category_slug}/{subcategory_slug}?p={page}"

        log_and_print(f"  Stran {page}: {url}")
        html = get_page_content(session, url, referer=f"{BASE_URL}{category_slug}/{subcategory_slug}")
        if not html:
            break

        soup = BeautifulSoup(html, "html.parser")

        no_products = soup.find("p", class_="note-msg")
        if no_products and "ni izdelkov" in no_products.get_text(" ", strip=True).lower():
            break

        product_grid = soup.find("ul", class_="products-grid")
        if not product_grid:
            break

        product_items = product_grid.find_all("li", class_="item")
        if not product_items:
            break

        first_a = product_items[0].find("a", class_="product-image")
        first_url = first_a.get("href") if first_a and first_a.get("href") else None
        if page > 1 and first_url and last_first and first_url == last_first:
            log_and_print("  Stran se ponavlja. Konec podkategorije.")
            break
        last_first = first_url

        for li in product_items:
            a = li.find("a", class_="product-image")
            if a and a.get("href"):
                links.append(a["href"])

        next_page = soup.select_one("div.pages a.next, div.pages a.i-next")
        if not next_page:
            break

    return list(dict.fromkeys(links))


# -----------------------------
# Product details
# -----------------------------
def extract_long_description(soup: BeautifulSoup) -> str:
    """Opis izdelka za klasifikacijo: meta description ali opisni blok."""
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        return meta_desc.get("content").strip()

    # pogosto Magento tema: div.short-description / div.std
    desc = soup.select_one("div.short-description div.std, div.product-collateral div.std, div#description, div.std")
    if desc:
        text = desc.get_text("\n", strip=True)
        if len(text) > 8000:
            text = text[:8000].rstrip() + "…"
        return text
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


def extract_delivery_short_from_text(raw: str) -> str:
    """Dobava: skrajšaj dolg tekst (npr. '1-10 delovnih dni...')."""
    if not raw:
        return ""

    t = raw.replace("\xa0", " ")
    t = re.sub(r"\s+", " ", t).strip()

    # odsekuj tipičen 'Dobavni rok predvidevamo ...'
    t = re.split(r"Dobavni rok", t, flags=re.IGNORECASE)[0].strip()

    # če vsebuje range dni, ga izvleci
    m = re.search(r"(\d+\s*[-–]\s*\d+)\s*(delovnih\s+)?dni", t, flags=re.IGNORECASE)
    if m:
        rng = m.group(1).replace("–", "-").replace(" ", "")
        return f"{rng} delovnih dni" if m.group(2) else f"{rng} dni"

    m2 = re.search(r"(\d+)\s*(delovnih\s+)?dni", t, flags=re.IGNORECASE)
    if m2:
        return f"{m2.group(1)} delovnih dni" if m2.group(2) else f"{m2.group(1)} dni"

    tl = t.lower()
    if "na zalogi" in tl:
        return "DA"
    if "ni na zalogi" in tl or "ni zaloge" in tl:
        return "NE"

    # varovalka: naj bo kratko
    return t[:80].strip()





def _extract_sku_and_dobava_from_text(page_text: str) -> tuple[str, str, bool]:
    """Best-effort: iz navadnega teksta pobere Šifra artikla + Dobava in zazna 'ni na zalogi'."""
    if not page_text:
        return "", "", False

    txt = page_text.replace("\xa0", " ")
    out_of_stock = "izdelka ni na zalogi" in txt.lower()

    sku = ""
    msku = re.search(r"Šifra\s+artikla\s*:\s*([0-9]+)", txt, flags=re.IGNORECASE)
    if msku:
        sku = msku.group(1).strip()

    dobava_raw = ""
    # npr. "Dobava: 1-5 delovnih dni"
    md = re.search(r"Dobava\s*:\s*([^\n\r]+)", txt, flags=re.IGNORECASE)
    if md:
        dobava_raw = md.group(1).strip()

    # odreži, če je preveč (včasih pride cela razlaga v isti vrstici)
    if dobava_raw:
        dobava_raw = extract_delivery_short_from_text(dobava_raw)

    return sku, dobava_raw, out_of_stock


def _extract_prices_from_text(page_text: str) -> tuple[str, str]:
    """Vrne (regular_price, special_price) kot string (2 dec)."""
    if not page_text:
        return "", ""

    txt = page_text.replace("\xa0", " ")

    # "Cena:  54,68 €"
    reg = ""
    m_reg = re.search(r"\bCena\s*:\s*([0-9\.,]+)", txt, flags=re.IGNORECASE)
    if m_reg:
        reg = round_price_2dec(m_reg.group(1))

    # "Akcijska cena  31,72 €"
    spec = ""
    m_spec = re.search(r"\bAkcijska\s+cena\s*([0-9\.,]+)", txt, flags=re.IGNORECASE)
    if m_spec:
        spec = round_price_2dec(m_spec.group(1))

    # fallback: če ni "Cena:" ampak je samo ena cena z € (npr. brez akcije)
    if not reg and not spec:
        m_any = re.search(r"(\d{1,3}(?:\.\d{3})*,\d{2})\s*€", txt)
        if m_any:
            reg = round_price_2dec(m_any.group(1))

    return reg, spec


def _extract_em_from_text(page_text: str) -> str:
    """Najde 'Cena je na KOS.' ali 'Cena je za KOS.' -> EM."""
    if not page_text:
        return "kos"
    txt = page_text.replace("\xa0", " ")
    m = re.search(r"Cena\s+je\s+(?:na|za)\s*([A-Za-z0-9/ ]+)", txt, flags=re.IGNORECASE)
    if m:
        em_raw = m.group(1).strip().strip(".")
        # pogosto je "KOS" ali "KOS."
        return normalize_em(em_raw)
    return "kos"


def _extract_image_best_effort(soup: BeautifulSoup) -> str:
    """Poskusi najti glavno sliko (og:image / product image / prva img)."""
    og = soup.find("meta", attrs={"property": "og:image"})
    if og and og.get("content"):
        return og.get("content").strip()

    tw = soup.find("meta", attrs={"name": "twitter:image"})
    if tw and tw.get("content"):
        return tw.get("content").strip()

    img = soup.select_one("img#image-main, img.gallery-image, img.product-image-photo, img[src]")
    if img and img.get("src"):
        src = img.get("src").strip()
        return urljoin(BASE_URL, src)

    return ""


def extract_product_details(session: requests.Session, product_url: str, group_name: str, date_str: str, referer: str) -> Optional[Dict[str, Any]]:
    """Produktna stran -> 1 zapis.

    Popravek (v5):
      - cene/akcije beremo tudi iz navadnega teksta ("Cena:", "Akcijska cena"),
        ker nekateri template-i nimajo div.price-box.
      - Šifra artikla + Dobava se pogosto pojavi kot navaden tekst
        ("Šifra artikla: ... Dobava: ...").
      - zazna "Izdelka ni na zalogi" in Dobava nastavi na "NE".
      - EM prebere iz "Cena je na/za KOS".
    """
    global _global_item_counter

    html = get_page_content(session, product_url, referer=referer)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text("\n", strip=True)

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

    # Opis (H1) – fallback na prvi h1
    h1 = soup.select_one("h1")
    if h1:
        data["Opis"] = h1.get_text(" ", strip=True)
    else:
        name_tag = soup.find("div", class_="product-name")
        if name_tag and name_tag.find("h1"):
            data["Opis"] = name_tag.find("h1").get_text(" ", strip=True)

    # Opis izdelka (za klasifikacijo)
    data["Opis izdelka"] = extract_long_description(soup)

    # SKU + dobava (kratko) – najprej poskusi strukturo, potem tekst
    sku = ""
    dobava = ""
    out_of_stock = False

    sku_div = soup.find("div", class_="sku")
    if sku_div:
        sifra_strong = sku_div.find("strong")
        if sifra_strong:
            sku = sifra_strong.get_text(" ", strip=True).strip()

        dobava_span = sku_div.find("span", class_="dobava")
        if dobava_span:
            raw = dobava_span.get_text(" ", strip=True).replace("Dobava:", "").strip()
            dobava = extract_delivery_short_from_text(raw)

    if not sku or not dobava:
        sku2, dobava2, oos2 = _extract_sku_and_dobava_from_text(page_text)
        sku = sku or sku2
        dobava = dobava or dobava2
        out_of_stock = out_of_stock or oos2
    else:
        out_of_stock = "izdelka ni na zalogi" in page_text.lower()

    if sku:
        data["Oznaka / naziv"] = sku

    # EAN (brez validacije dolžine)
    data["EAN"] = extract_ean_raw(soup)

    # Cene – najprej struktura, potem tekst
    reg_price = ""
    spec_price = ""

    price_box = soup.find("div", class_="price-box")
    if price_box:
        special_p = price_box.find("p", class_="special-price")
        old_p = price_box.find("p", class_="old-price")
        regular_span = price_box.find("span", class_="regular-price")

        if special_p:
            p_val = special_p.find("span", class_="price")
            if p_val:
                spec_price = round_price_2dec(parse_price_any(p_val.get_text(" ", strip=True)))

            if old_p:
                p_old = old_p.find("span", class_="price")
                if p_old:
                    reg_price = round_price_2dec(parse_price_any(p_old.get_text(" ", strip=True)))
        elif regular_span:
            p_val = regular_span.find("span", class_="price")
            if p_val:
                reg_price = round_price_2dec(parse_price_any(p_val.get_text(" ", strip=True)))

    if not reg_price and not spec_price:
        reg_price, spec_price = _extract_prices_from_text(page_text)

    # nastavitev cen
    data["Cena / EM (z DDV)"] = reg_price
    data["Akcijska cena / EM (z DDV)"] = spec_price

    # brez DDV
    data["Cena / EM (brez DDV)"] = convert_price_to_without_vat(data.get("Cena / EM (z DDV)"), DDV_RATE)
    data["Akcijska cena / EM (brez DDV)"] = convert_price_to_without_vat(data.get("Akcijska cena / EM (z DDV)"), DDV_RATE)

    # EM – najprej struktura, potem tekst ("Cena je na KOS")
    em_div = soup.find("div", class_="em")
    if em_div:
        em_text = em_div.get_text(" ", strip=True)
        m = re.search(r"Cena je na\s*([^\.]+)", em_text, flags=re.IGNORECASE)
        if m:
            data["EM"] = normalize_em(m.group(1).strip())
        else:
            data["EM"] = normalize_em(data.get("EM") or "kos")
    else:
        data["EM"] = _extract_em_from_text(page_text) or normalize_em(data.get("EM") or "kos")

    # Slika – bolj robustno
    data["SLIKA URL"] = _extract_image_best_effort(soup)

    # Dobava – prioriteta:
    #   1) če je explicitno "ni na zalogi" -> NE
    #   2) če imamo dobavni rok -> to (kratko)
    #   3) če je "na zalogi" v tekstu -> DA
    if out_of_stock:
        data["Dobava"] = "NE"
    elif dobava:
        data["Dobava"] = dobava
    else:
        data["Dobava"] = extract_delivery_short_from_text(page_text)

    return data


# -----------------------------
# Main
# -----------------------------
def main():
    global _log_file, _global_item_counter

    # start jitter
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

    # resume: če json že obstaja, nadaljuj števec in preskoči že zajete (po key)
    existing_keys = set()
    if os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                d = json.load(f)
            if isinstance(d, list) and d:
                for x in d:
                    if isinstance(x, dict):
                        existing_keys.add(_item_key(x))
                _global_item_counter = max((int(x.get("Zap", 0)) for x in d if isinstance(x, dict)), default=0)
        except Exception:
            pass

    date_str = datetime.now().strftime("%d/%m/%Y")
    buffer: List[Dict[str, Any]] = []
    processed_in_run = 0

    try:
        for cat_slug, subcats in CATEGORIES.items():
            cat_name = cat_slug.replace("-", " ").capitalize()
            log_and_print(f"\n=== Kategorija: {cat_name} ===")

            for sub_slug in subcats:
                log_and_print(f"\n-- Podkategorija: {sub_slug}")

                product_urls = get_product_links_from_subcategory(session, cat_slug, sub_slug)

                for product_url in product_urls:
                    # najprej poberi detajle, ker sku (key) je na detail strani
                    log_and_print(f"    Izdelek: {product_url}")
                    human_sleep()

                    details = extract_product_details(
                        session=session,
                        product_url=product_url,
                        group_name=cat_name,
                        date_str=date_str,
                        referer=f"{BASE_URL}{cat_slug}/{sub_slug}",
                    )

                    if not details:
                        continue

                    k = _item_key(details)
                    if k in existing_keys:
                        continue

                    buffer.append(details)
                    existing_keys.add(k)
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
