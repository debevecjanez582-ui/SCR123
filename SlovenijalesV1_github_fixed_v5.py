import pandas as pd
import os
import sys
from datetime import datetime
import time
import random
import re
import json
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# --- Konfiguracija ---
SHOP_NAME = "Slovenijales"
BASE_URL = "https://trgovina.slovenijales.si"
DDV_RATE = 0.22

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

# --- GitHub/CI nastavitve ---
CHECKPOINT_EVERY_N_ITEMS = 5  # JSON checkpoint batch
EXPORT_STOCK_COLUMNS = os.environ.get("EXPORT_STOCK_COLUMNS", "").strip().lower() == "true"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
]

# EM whitelist (če ni v whitelist -> "kos")
EM_WHITELIST = {
    "kos",
    "m",
    "m2",
    "m3",
    "kg",
    "g",
    "l",
    "ml",
    "pak",
    "set",
    "par",
    "rola",
}

EM_ALIASES = {
    "m²": "m2",
    "m^2": "m2",
    "m^{2}": "m2",
    "㎡": "m2",
    "m³": "m3",
    "m^3": "m3",
    "m^{3}": "m3",
    "㎥": "m3",
    "kom": "kos",
    "kom.": "kos",
    "pc": "kos",
    "pcs": "kos",
    "KOS": "kos",
}

# --- Globalne spremenljivke ---
_log_file = None
_global_item_counter = 0
_session = requests.Session()

_existing_urls = set()
_all_data_by_url = {}  # URL -> item dict


# --- Standardne pomožne funkcije ---

def log_and_print(message, to_file=True):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_message = f"[{timestamp}] {message}"
    print(full_message)
    if to_file and _log_file:
        try:
            _log_file.write(full_message + "\n")
            _log_file.flush()
        except Exception as e:
            print(f"NAPAKA: Ni mogoče zapisati v log datoteko: {e}")


def create_output_paths():
    """
    create_output_paths() mora obstajati v vsaki datoteki.

    Output struktura:
      OUTPUT_DIR/Ceniki_Scraping/<SHOP>/<YYYY-MM-DD>/
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_root = os.environ.get("OUTPUT_DIR") or script_dir

    run_date = datetime.now().strftime("%Y-%m-%d")
    daily_dir = os.path.join(output_root, "Ceniki_Scraping", SHOP_NAME, run_date)
    os.makedirs(daily_dir, exist_ok=True)

    json_path = os.path.join(daily_dir, f"{SHOP_NAME}_{run_date}.json")
    excel_path = os.path.join(daily_dir, f"{SHOP_NAME}_{run_date}.xlsx")
    log_path = os.path.join(daily_dir, f"{SHOP_NAME}_{run_date}.log")

    print(f"JSON pot:  {json_path}")
    print(f"Excel pot: {excel_path}")
    print(f"Log pot:   {log_path}")

    return {"daily_dir": daily_dir, "json_path": json_path, "excel_path": excel_path, "log_path": log_path}


def _atomic_write_json(obj, path):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def normalize_em(em_raw: str) -> str:
    if not em_raw:
        return "kos"
    em = str(em_raw).strip()
    em = EM_ALIASES.get(em, em)
    em = em.lower().replace(" ", "")
    em = EM_ALIASES.get(em, em)
    # odstrani leading slash ali nepotrebne znake
    em = em.replace("/", "")
    # normaliziraj m 2 / m^{2} / m^2 -> m2, enako za m3
    em = re.sub(r"m\s*\^?\s*\{?\s*2\s*\}?", "m2", em)
    em = re.sub(r"m\s*\^?\s*\{?\s*3\s*\}?", "m3", em)
    em = em.replace("{", "").replace("}", "").replace("^", "")
    em = EM_ALIASES.get(em, em)
    return em if em in EM_WHITELIST else "kos"


def parse_price_eur(price_raw: str):
    """'1.234,56' -> 1234.56  (float)"""
    if not price_raw:
        return None
    s = str(price_raw).strip()
    s = s.replace("\xa0", " ")
    s = s.replace(".", "").replace(",", ".")
    try:
        return round(float(s), 2)
    except Exception:
        return None


def price_without_vat(price_float, vat_rate):
    if price_float is None:
        return None
    try:
        return round(float(price_float) / (1 + vat_rate), 2)
    except Exception:
        return None


def get_page_content(url):
    """
    Brez bypass/anti-bot.
    Če naleti na 403/429/503 ipd: log + preskoči URL (run naj teče dalje).
    """
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "sl-SI,sl;q=0.9,en;q=0.7",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive",
    }
    try:
        resp = _session.get(url, headers=headers, timeout=25, allow_redirects=True)
        if resp.status_code in (403, 429, 503):
            log_and_print(f"⚠️ Možen challenge ({resp.status_code}) za URL: {url} -> preskakujem", to_file=True)
            return None
        if resp.status_code >= 400:
            log_and_print(f"HTTP {resp.status_code} za URL: {url} -> preskakujem", to_file=True)
            return None
        return resp.text
    except requests.exceptions.RequestException as e:
        log_and_print(f"Napaka pri dostopu do URL-ja {url}: {e}", to_file=True)
        return None


def load_existing_json(json_path):
    """Resume iz obstoječega JSON."""
    global _global_item_counter, _existing_urls, _all_data_by_url

    if not os.path.exists(json_path):
        return

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            return

        max_zap = 0
        by_url = {}
        for row in data:
            if not isinstance(row, dict):
                continue
            u = row.get("URL")
            if not u:
                continue
            by_url[u] = row
            _existing_urls.add(u)
            try:
                z = int(row.get("Zap", 0))
                if z > max_zap:
                    max_zap = z
            except Exception:
                pass

        _all_data_by_url = by_url
        _global_item_counter = max_zap
        log_and_print(f"Resume: {len(_all_data_by_url)} zapisov, Zap={_global_item_counter}", to_file=True)
    except Exception as e:
        log_and_print(f"Napaka pri nalaganju obstoječega JSON: {e}", to_file=True)


def save_json_checkpoint(json_path):
    """Shrani trenutne podatke v JSON (checkpoint)."""
    try:
        final_list = list(_all_data_by_url.values())
        try:
            final_list.sort(key=lambda x: int(x.get("Zap", 0)))
        except Exception:
            pass
        _atomic_write_json(final_list, json_path)
        log_and_print("JSON checkpoint shranjen.", to_file=True)
    except Exception as e:
        log_and_print(f"Napaka pri shranjevanju JSON: {e}", to_file=True)


def _parse_stock_json(s):
    if not s or not isinstance(s, str):
        return {}
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def save_excel_final(excel_path):
    """Excel se naredi 1× na koncu."""
    final_list = list(_all_data_by_url.values())
    if not final_list:
        log_and_print("Ni podatkov za Excel.", to_file=True)
        return

    try:
        final_list.sort(key=lambda x: int(x.get("Zap", 0)))
    except Exception:
        pass

    df = pd.DataFrame(final_list)

    # opcijsko: Zaloga po centrih -> stolpci
    if EXPORT_STOCK_COLUMNS and "Zaloga po centrih" in df.columns:
        stocks = df["Zaloga po centrih"].apply(_parse_stock_json)
        all_centers = sorted(set().union(*stocks.tolist())) if len(stocks) else []
        for c in all_centers:
            df[f"Zaloga - {c}"] = stocks.apply(lambda d: d.get(c, "") if isinstance(d, dict) else "")

    cols = [
        "Skupina", "Zap", "Oznaka / naziv", "EAN", "Opis", "Opis izdelka", "EM", "Valuta", "DDV", "Proizvajalec",
        "Veljavnost od", "Dobava", "Zaloga po centrih",
        "Cena / EM (z DDV)", "Akcijska cena / EM (z DDV)",
        "Cena / EM (brez DDV)", "Akcijska cena / EM (brez DDV)",
        "URL", "SLIKA URL"
    ]
    # dodaj dinamične stolpce zaloge, če obstajajo
    dyn = [c for c in df.columns if isinstance(c, str) and c.startswith("Zaloga - ")]
    cols = cols + [c for c in dyn if c not in cols]

    for c in cols:
        if c not in df.columns:
            df[c] = ""

    df.drop_duplicates(subset=["URL"], keep="last", inplace=True)
    df[cols].to_excel(excel_path, index=False)
    log_and_print("Excel shranjen (final).", to_file=True)


# --- Slovenijales parsanje ---

PER_UNIT_RE = re.compile(
    r"(\d{1,3}(?:\.\d{3})*,\d{2})\s*€\s*/\s*([A-Za-z]{1,5}(?:\s*\^?\s*\{?\s*[0-9]{1,2}\s*\}?|[0-9]{1,2}|[²³])?)",
    flags=re.IGNORECASE,
)


def get_product_links_from_category(category_url):
    """Pobere vse produktne linke iz kategorije (s paginacijo)."""
    all_links = []
    stariprvi_url = "star"
    page = 1

    while True:
        url = f"{category_url}?page={page}"
        log_and_print(f"  Stran {page}: {url}", to_file=True)

        html = get_page_content(url)
        if not html:
            break

        soup = BeautifulSoup(html, "html.parser")

        # primarni selector (kot v tvoji verziji)
        products = soup.select('div.single-product.border-left[itemscope]')
        # fallback selectorji (minimalno)
        if not products:
            products = soup.select("div.single-product[itemscope], div.single-product")

        if not products:
            break

        # Preverjanje ponavljanja (prvi izdelek isti kot prej)
        first_tag = products[0].select_one(".product-img a[href]") or products[0].select_one("a[href]")
        noviprvi_url = first_tag["href"] if (first_tag and first_tag.get("href")) else None
        if page > 1 and noviprvi_url and noviprvi_url == stariprvi_url:
            log_and_print("  Vsebina se ponavlja. Konec.", to_file=True)
            break
        if noviprvi_url:
            stariprvi_url = noviprvi_url

        for p in products:
            a = p.select_one(".product-img a[href]") or p.select_one("a[href]")
            if not a or not a.get("href"):
                continue
            href = a["href"].strip()
            full = href if href.startswith("http") else urljoin(BASE_URL, href)
            # filter: samo produktne strani (ne kategorije)
            if full.startswith(BASE_URL):
                all_links.append(full)

        log_and_print(f"  Najdenih {len(products)} izdelkov.", to_file=True)

        # paginacija: če ni "Naprej", konec
        if not soup.select_one('ul.pagination a[aria-label="Naprej"], ul.pagination a[rel="next"], a[rel="next"]'):
            break

        page += 1
        time.sleep(random.uniform(0.7, 2.0) if os.environ.get("GITHUB_ACTIONS", "").lower() == "true" else random.uniform(2.0, 5.0))

    # dedup brez izgube vrstnega reda
    return list(dict.fromkeys(all_links))


def _extract_title(soup):
    h1 = soup.find("h1")
    return h1.get_text(strip=True) if h1 else ""


def _extract_koda_artikla(full_text):
    m = re.search(r"\bKoda artikla\s+([0-9A-Za-z\-]+)\b", full_text)
    return m.group(1).strip() if m else ""


def _extract_ean_raw(soup, full_text):
    # meta (če obstaja)
    meta_gtin = soup.select_one('meta[itemprop^="gtin"], meta[itemprop="gtin13"], meta[itemprop="gtin14"]')
    if meta_gtin and meta_gtin.get("content"):
        return meta_gtin["content"].strip()

    # tekst (RAW, brez validacije dolžine)
    m = re.search(r"\b(?:EAN|GTIN)\b\s*[:\-]?\s*([0-9A-Za-z\-]+)", full_text, flags=re.IGNORECASE)
    return m.group(1).strip() if m else ""


def _extract_image(soup):
    meta = soup.find("meta", property="og:image")
    if meta and meta.get("content"):
        return meta["content"].strip()
    img = soup.select_one("img[src]")
    if img and img.get("src"):
        src = img["src"].strip()
        return src if src.startswith("http") else urljoin(BASE_URL, src)
    return ""


def _extract_prices_and_unit(lines, title):
    """
    Preferira '€ / EM' (per-unit).
    Če sta 2 per-unit ceni: akcijska = prva, redna = druga.
    Če ni per-unit: pobere 'xx,xx €' (in če sta 2 -> akcijska prva, redna druga).
    """
    start = 0
    if title and title in lines:
        start = lines.index(title)

    end = len(lines)
    # čim prej odrežemo “sorodni artikli”, da ne poberemo tujih cen
    for marker in ["Sorodni artikli", "Stopite v kontakt z nami"]:
        if marker in lines[start:]:
            end = min(end, start + lines[start:].index(marker))

    # še bolj: cena je običajno pred "Koda artikla" ali "Količina"
    for marker in ["Koda artikla", "Količina"]:
        for i in range(start, end):
            if marker in lines[i]:
                end = min(end, i)
                break

    block = " ".join(lines[start:end])

    # 1) per-unit
    matches = PER_UNIT_RE.findall(block)
    uniq = []
    seen = set()
    for pr, un in matches:
        unit_norm = normalize_em(un)
        key = (pr, unit_norm)
        if key in seen:
            continue
        seen.add(key)
        uniq.append(key)

    if uniq:
        em = uniq[0][1] if uniq[0][1] else "kos"
        prices = [parse_price_eur(pr) for pr, _ in uniq]
        prices = [p for p in prices if p is not None]
        if not prices:
            return None, None, em

        if len(prices) >= 2:
            akc = prices[0]
            redna = prices[1]
        else:
            akc = None
            redna = prices[0]
        return redna, akc, em

    # 2) fallback: brez unit
    prices_raw = re.findall(r"(\d{1,3}(?:\.\d{3})*,\d{2})\s*€", block)
    uniqp = []
    seenp = set()
    for pr in prices_raw:
        if pr in seenp:
            continue
        seenp.add(pr)
        uniqp.append(pr)

    prices = [parse_price_eur(pr) for pr in uniqp]
    prices = [p for p in prices if p is not None]
    if not prices:
        return None, None, "kos"

    if len(prices) >= 2:
        akc = prices[0]
        redna = prices[1]
    else:
        akc = None
        redna = prices[0]

    return redna, akc, "kos"


def _extract_long_description(lines):
    """
    Slovenijales pogosto nima klasičnega "Opis" taba; opis je pogosto kratek tekst
    takoj po gumbu "V košarico" ali "Oddaj povpraševanje".
    """
    start_idx = None
    for marker in ["V košarico", "Oddaj povpraševanje"]:
        if marker in lines:
            start_idx = lines.index(marker) + 1
            break
    if start_idx is None:
        return ""

    end_markers = {
        "Preveri razpoložljivost v poslovalnicah",
        "Sorodni artikli",
        "Stopite v kontakt z nami",
    }

    buf = []
    for i in range(start_idx, len(lines)):
        if lines[i] in end_markers:
            break
        # ne pobiraj cookie teksta ipd
        if lines[i].lower().startswith("piškotki"):
            break
        buf.append(lines[i])

    # vrni “razumen” opis
    txt = "\n".join(buf).strip()
    return txt


def _extract_stock_centers_from_text(lines):
    """
    Če bi bila v HTML kje prikazana zaloga po poslovalnicah, jo poskusimo pobrat.
    V praksi je pogosto interaktivno (modal/API), zato bo velikokrat prazno.

    Podpira nekaj osnovnih formatov, npr:
      "Celje - Na zalogi"
      "Ljubljana: Na zalogi"
    """
    stock = {}
    for ln in lines:
        # Center - Na zalogi / Ni na zalogi
        m = re.match(r"^(.+?)\s*[-:]\s*(Na zalogi|Ni na zalogi|Razprodano|Po naročilu).*$", ln, flags=re.IGNORECASE)
        if m:
            center = m.group(1).strip()
            status = m.group(2).strip()
            # minimalen filter, da ne poberemo “neumnosti”
            if 2 <= len(center) <= 60:
                stock[center] = status

    return stock


def _compute_dobava(full_text, lines, stock_dict):
    """
    Dobava:
      - če je stock po centrih: DA če vsaj en center "Na zalogi"
      - sicer:
         - "V košarico" -> DA
         - "Oddaj povpraševanje" -> NE
         - "Ni na zalogi"/"Razprodano" -> NE
         - drugače: ""
    """
    if stock_dict:
        for v in stock_dict.values():
            if "na zalogi" in str(v).lower():
                return "DA"
        return "NE"

    t = (full_text or "").lower()

    if "ni na zalogi" in t or "razprodano" in t:
        return "NE"
    if "v košarico" in t:
        return "DA"
    if "oddaj povpraševanje" in t:
        return "NE"

    return ""


def get_product_details(url, group_name, date):
    global _global_item_counter

    log_and_print(f"    - Detajli: {url}", to_file=True)
    html = get_page_content(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    full_text = soup.get_text("\n", strip=True)
    lines = [l.strip() for l in full_text.splitlines() if l.strip()]

    title = _extract_title(soup)
    if not title:
        # fallback: prva smiselna vrstica
        title = lines[0] if lines else ""

    koda = _extract_koda_artikla(full_text)

    # fallback za oznako: meta sku (če obstaja)
    if not koda:
        sku = soup.select_one('meta[itemprop="sku"]')
        if sku and sku.get("content"):
            koda = sku["content"].strip()

    ean_raw = _extract_ean_raw(soup, full_text)

    redna, akcijska, em = _extract_prices_and_unit(lines, title=title)

    # Če nimamo nobene cene, preskočimo (ne crash)
    if redna is None and akcijska is None:
        log_and_print("      Preskakujem (ni cene).", to_file=True)
        return None

    # Če obstaja samo akcijska (redna None), jo premaknemo v redno
    if redna is None and akcijska is not None:
        redna, akcijska = akcijska, None

    opis_izdelek = _extract_long_description(lines)

    # Proizvajalec: Slovenijales pogosto ne podaja jasno -> pustimo prazno (minimalno)
    proizvajalec = ""

    # slika
    img_url = _extract_image(soup)

    # stock po centrih (če slučajno obstaja v HTML)
    stock_dict = _extract_stock_centers_from_text(lines)
    stock_json = json.dumps(stock_dict, ensure_ascii=False) if stock_dict else ""

    dobava = _compute_dobava(full_text, lines, stock_dict)

    # Zap dodelimo šele tukaj (da ne delamo lukenj pri preskočenih)
    _global_item_counter += 1

    data = {
        "Skupina": group_name,
        "Zap": _global_item_counter,
        "Oznaka / naziv": koda,  # šifra artikla, clean
        "EAN": ean_raw,          # raw (brez validacije)
        "Opis": title,
        "Opis izdelka": opis_izdelek,
        "EM": normalize_em(em),
        "Valuta": "EUR",
        "DDV": "22",
        "Proizvajalec": proizvajalec,
        "Veljavnost od": date,
        "Dobava": dobava,
        "Zaloga po centrih": stock_json,
        "Cena / EM (z DDV)": round(float(redna), 2) if redna is not None else "",
        "Akcijska cena / EM (z DDV)": round(float(akcijska), 2) if akcijska is not None else "",
        "Cena / EM (brez DDV)": price_without_vat(redna, DDV_RATE) if redna is not None else "",
        "Akcijska cena / EM (brez DDV)": price_without_vat(akcijska, DDV_RATE) if akcijska is not None else "",
        "URL": url,
        "SLIKA URL": img_url,
    }

    return data


def main():
    global _log_file, _global_item_counter

    is_ci = os.environ.get("GITHUB_ACTIONS", "").lower() == "true"
    time.sleep(random.uniform(0.0, 2.0) if is_ci else random.uniform(1.0, 8.0))

    paths = create_output_paths()
    json_path = paths["json_path"]
    excel_path = paths["excel_path"]
    log_path = paths["log_path"]

    try:
        _log_file = open(log_path, "w", encoding="utf-8")
    except Exception as e:
        print(f"CRITICAL ERROR: Ni mogoče ustvariti log datoteke: {e}")
        return

    log_and_print(f"--- Zagon {SHOP_NAME} ---", to_file=True)

    # Resume
    load_existing_json(json_path)

    date = datetime.now().strftime("%d/%m/%Y")
    newly_added = 0

    try:
        for main_cat, urls in SLOVENIJALES_CATEGORIES.items():
            log_and_print(f"\n--- {main_cat} ---", to_file=True)

            for cat_url in urls:
                # Skupina naj bo bolj granularna (slug zadnjega dela URL-ja)
                group_name = cat_url.rstrip("/").split("/")[-1] or main_cat
                log_and_print(f"  Podkategorija: {group_name}", to_file=True)

                links = get_product_links_from_category(cat_url)
                if not links:
                    continue

                for link in links:
                    if link in _existing_urls:
                        continue

                    det = get_product_details(link, group_name, date)
                    if det:
                        _all_data_by_url[link] = det
                        _existing_urls.add(link)
                        newly_added += 1

                        if newly_added >= CHECKPOINT_EVERY_N_ITEMS:
                            save_json_checkpoint(json_path)
                            newly_added = 0

                    time.sleep(random.uniform(0.6, 2.2) if is_ci else random.uniform(2.0, 5.0))

                # checkpoint po podkategoriji
                save_json_checkpoint(json_path)
                newly_added = 0

    except KeyboardInterrupt:
        log_and_print("Prekinjeno (KeyboardInterrupt). Shranjujem...", to_file=True)
    except Exception as e:
        log_and_print(f"NAPAKA: {e}", to_file=True)
    finally:
        # final JSON + Excel
        save_json_checkpoint(json_path)
        save_excel_final(excel_path)

        log_and_print("--- KONEC ---", to_file=True)
        if _log_file:
            _log_file.close()


if __name__ == "__main__":
    main()
