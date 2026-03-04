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
# MERKUR scraper (GitHub/CI friendly) - v6 (stable)
# ============================================================

SHOP_NAME = "Merkur"
BASE_URL = "https://www.merkur.si"
DDV_RATE = 0.22

EXPORT_EXCEL = True

SLEEP_MIN = float(os.environ.get("SCRAPE_SLEEP_MIN", "4.0"))
SLEEP_MAX = float(os.environ.get("SCRAPE_SLEEP_MAX", "6.0"))
BUFFER_FLUSH = int(os.environ.get("BUFFER_FLUSH", "50"))
MAX_PAGES = int(os.environ.get("MAX_PAGES", "250"))
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
    "Accept-Language": "sl-SI,sl;q=0.9,en;q=0.7,en-US;q=0.5",
    "Connection": "keep-alive",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
}

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

# --- EM whitelist ---
_ALLOWED_EM = {
    "ar", "ha", "kam", "kg", "km", "kwh", "kw", "wat",
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

EXCEL_COLS = [
    "Skupina", "Zap", "Oznaka / naziv", "EAN", "Opis", "Opis izdelka",
    "EM", "Valuta", "DDV", "Proizvajalec", "Veljavnost od", "Dobava",
    "Cena / EM (z DDV)", "Akcijska cena / EM (z DDV)",
    "Cena / EM (brez DDV)", "Akcijska cena / EM (brez DDV)",
    "URL", "SLIKA URL"
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


def human_sleep(min_s: float = None, max_s: float = None) -> None:
    mn = SLEEP_MIN if min_s is None else min_s
    mx = SLEEP_MAX if max_s is None else max_s
    time.sleep(random.uniform(mn, mx))


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
    return json_path, excel_path, log_path, daily_dir


def is_block_page(html: str) -> bool:
    if not html:
        return False
    t = html.lower()
    strong = [
        "/cdn-cgi/challenge-platform", "cf-chl-", "cloudflare ray id",
        "attention required", "access denied", "request blocked",
        "your request has been blocked", "verifying you are human",
        "verify you are human", "please enable cookies",
        "enable javascript and cookies", "perimeterx", "px-captcha",
        "datadome", "incapsula", "sucuri website firewall",
        "ddos-guard", "akamai bot manager",
    ]
    if any(n in t for n in strong):
        return True

    if re.search(r"\b(hcaptcha|cf-turnstile|g-recaptcha|px-captcha|data-sitekey)\b", t):
        if any(x in t for x in ("verify", "verifying", "blocked", "access denied", "challenge")):
            return True

    soft = 0
    for n in ("captcha", "challenge", "bot", "blocked", "verify", "verification"):
        if n in t:
            soft += 1
    return soft >= 4


def build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=2,
        connect=2,
        read=2,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
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
                log_and_print(f"[BLOCK] challenge @ {url} -> sleep {wait:.1f}s (poskus {attempt}/{MAX_BLOCK_RETRIES})")
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
    cand = soup.select_one(".product.attribute.description") or soup.select_one("#description") or soup.select_one(".product-info")
    if cand:
        txt = cand.get_text("\n", strip=True)
        if txt and len(txt) > 30:
            return (txt[:8000].rstrip() + "…") if len(txt) > 8000 else txt
    return ""


def extract_image_url(soup: BeautifulSoup) -> str:
    og = soup.find("meta", attrs={"property": "og:image"})
    if og and og.get("content"):
        return og.get("content").strip()
    img = soup.select_one("img[src]")
    return img.get("src").strip() if img and img.get("src") else ""


def extract_product_code_from_url(url: str) -> str:
    """Merkur URL pogosto vsebuje številko na koncu ali v poti; fallback: nič."""
    # npr. ...-134410.jpg je slika; produkt url: /gaseno-apno-...-134410/
    m = re.search(r"-(\d{4,})/?$", url.strip("/"))
    return m.group(1) if m else ""


def extract_product_details(session: requests.Session, product_url: str, group_name: str, date_str: str, referer: str) -> Optional[Dict[str, Any]]:
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

    h1 = soup.select_one("h1") or soup.select_one(".page-title h1")
    if h1:
        data["Opis"] = h1.get_text(" ", strip=True)

    data["Opis izdelka"] = extract_long_description(soup)
    data["SLIKA URL"] = extract_image_url(soup)
    data["EAN"] = extract_ean_raw(soup)

    # Oznaka/koda: najprej poskusi "product sku" iz strani
    txt = soup.get_text("\n", strip=True).replace("\xa0", " ")
    # Merkur pogosto nima jasnega "Šifra:", zato uporabimo URL kodo (stabilno znotraj Merkurja)
    data["Oznaka / naziv"] = extract_product_code_from_url(product_url)

    # Cene: poskusi standardne selectorje
    price_text = ""
    for sel in (".price-wrapper .price", ".product-info-price .price", "span.price"):
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            price_text = el.get_text(" ", strip=True)
            break
    if price_text:
        data["Cena / EM (z DDV)"] = round_price_2dec(price_text)
        data["Cena / EM (brez DDV)"] = convert_price_to_without_vat(data["Cena / EM (z DDV)"], DDV_RATE)

    # EM: best-effort iz teksta (€/m2 ipd)
    munit = re.search(r"€\s*/\s*([A-Za-z0-9²³]+)", txt)
    if munit:
        data["EM"] = normalize_em(munit.group(1))

    # Dobava: best-effort (na zalogi / ni na zalogi)
    tl = txt.lower()
    if "ni na zalogi" in tl:
        data["Dobava"] = "NE"
    elif "na zalogi" in tl:
        data["Dobava"] = "DA"
    else:
        m = re.search(r"dobavni\s+rok\s*[:\-]?\s*([0-9]+\s*[-–]\s*[0-9]+\s*\w+)", tl)
        data["Dobava"] = m.group(1).strip() if m else ""

    # Proizvajalec: best-effort
    mman = re.search(r"Proizvajalec\s*:\s*([^\n\r]+)", txt, flags=re.IGNORECASE)
    if mman:
        data["Proizvajalec"] = mman.group(1).strip()[:250]

    data["EM"] = normalize_em(data.get("EM") or "kos")
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


def get_product_links_from_category(session: requests.Session, category_url: str) -> List[str]:
    links: List[str] = []
    last_first = None

    for page in range(1, MAX_PAGES + 1):
        page_url = f"{category_url}?p={page}#section-products"
        log_and_print(f"  Stran {page}: {page_url}")

        html = get_page_content(session, page_url, referer=category_url)
        if not html:
            break

        soup = BeautifulSoup(html, "html.parser")
        container = soup.find("div", class_="list-items")
        if not container:
            break

        items = container.find_all("div", class_="item")
        if not items:
            break

        first_title = items[0].get_text(" ", strip=True)[:80]
        if page > 1 and last_first and first_title == last_first:
            log_and_print("  Stran se ponavlja. Konec kategorije.")
            break
        last_first = first_title

        for it in items:
            a = it.find("a")
            if not a or not a.get("href"):
                continue
            href = a["href"]
            full = href if href.startswith("http") else urljoin(BASE_URL, href)
            if "/p/" in full or full.startswith(BASE_URL):
                links.append(full)

        if not soup.select_one("a.next"):
            break

    return list(dict.fromkeys(links))


def main():
    global _log_file, _debug_dir, _global_item_counter

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

    # continue Zap if json exists
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

    try:
        for main_cat, urls in MERKUR_CATEGORIES.items():
            log_and_print(f"\n=== {main_cat} ===")
            for u in urls:
                sub_name = u.strip("/").split("/")[-1]
                log_and_print(f"\n-- Podkategorija: {sub_name}")

                product_urls = get_product_links_from_category(session, u)
                for purl in product_urls:
                    log_and_print(f"    Izdelek: {purl}")
                    det = extract_product_details(session, purl, sub_name, date_str, u)
                    if det:
                        buffer.append(det)

                    if len(buffer) >= BUFFER_FLUSH:
                        save_data_append(buffer, json_path)
                        buffer = []

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
