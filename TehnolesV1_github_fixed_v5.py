import pandas as pd
import os
import sys
from datetime import datetime
import time
import random
import re
import json
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# --- Konfiguracija ---
SHOP_NAME = "Tehnoles"
BASE_URL = "https://www.tehnoles.si"
DDV_RATE = 0.22

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
    "m", "m2", "m3",
    "kg", "g",
    "l", "ml",
    "t",
    "cm", "mm",
    "pak", "set", "par", "rola",
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
    "M2": "m2",
    "M3": "m3",
    "KG": "kg",
    "G": "g",
    "L": "l",
}

# --- Globalne ---
_log_file = None
_global_item_counter = 0

_existing_urls = set()
_all_data_by_url = {}  # URL -> dict

_session = requests.Session()
_session.headers.update(
    {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "sl-SI,sl;q=0.9,en;q=0.7",
        "Connection": "keep-alive",
    }
)


def log_and_print(message, to_file=True):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_message = f"[{timestamp}] {message}"
    print(full_message)
    if to_file and _log_file:
        try:
            _log_file.write(full_message + "\n")
            _log_file.flush()
        except Exception:
            pass


def create_output_paths():
    """
    Zahteva: create_output_paths() mora obstajati v vsaki datoteki.

    Output struktura:
      OUTPUT_DIR/Ceniki_Scraping/<SHOP>/<YYYY-MM-DD>/

    GitHub/CI:
      - če je nastavljen env OUTPUT_DIR, se vse piše pod to mapo (npr. artifacts/)
      - drugače se piše ob skripti
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_root = os.environ.get("OUTPUT_DIR") or script_dir

    run_date = datetime.now().strftime("%Y-%m-%d")
    daily_dir = os.path.join(output_root, "Ceniki_Scraping", SHOP_NAME, run_date)
    os.makedirs(daily_dir, exist_ok=True)

    json_path = os.path.join(daily_dir, f"{SHOP_NAME}_{run_date}.json")
    excel_path = os.path.join(daily_dir, f"{SHOP_NAME}_{run_date}.xlsx")
    log_path = os.path.join(daily_dir, f"{SHOP_NAME}_{run_date}.log")

    return {"daily_dir": daily_dir, "json_path": json_path, "excel_path": excel_path, "log_path": log_path}


def _atomic_write_json(obj, path):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def normalize_em(em_raw):
    """EM normalizacija: whitelist, sicer 'kos'."""
    if not em_raw:
        return "kos"
    em = str(em_raw).strip()
    em = EM_ALIASES.get(em, em)
    em = em.lower().replace(" ", "")
    em = EM_ALIASES.get(em, em)

    # odstrani '/', in normaliziraj m^2/m^{2} ipd
    em = em.replace("/", "")
    em = re.sub(r"m\s*\^?\s*\{?\s*2\s*\}?", "m2", em)
    em = re.sub(r"m\s*\^?\s*\{?\s*3\s*\}?", "m3", em)
    em = em.replace("{", "").replace("}", "").replace("^", "")

    em = EM_ALIASES.get(em, em)
    return em if em in EM_WHITELIST else "kos"


def parse_price_eur(price_raw):
    """
    '1.234,56' -> 1234.56
    '39,44'    -> 39.44
    """
    if not price_raw:
        return None
    s = str(price_raw).strip().replace("\xa0", " ")
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
    Brez bypass zaščit.
    Če naletimo na tipične challenge statuse (403/429/503), samo log + preskoči.
    """
    headers = {"User-Agent": random.choice(USER_AGENTS)}
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
    """Resume iz obstoječega JSON (če obstaja)."""
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
    """Checkpoint JSON (atomic write)."""
    try:
        final_list = list(_all_data_by_url.values())
        try:
            final_list.sort(key=lambda x: int(x.get("Zap", 0)))
        except Exception:
            pass
        _atomic_write_json(final_list, json_path)
        log_and_print("JSON checkpoint shranjen.", to_file=True)
    except Exception as e:
        log_and_print(f"Napaka pri shranjevanju JSON checkpointa: {e}", to_file=True)


def _parse_stock_json(s):
    if not s or not isinstance(s, str):
        return {}
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def save_excel_final(excel_path):
    """Excel naredimo 1× na koncu (opcijsko razširimo zalogo po centrih)."""
    final_list = list(_all_data_by_url.values())
    if not final_list:
        log_and_print("Ni podatkov za Excel.", to_file=True)
        return

    try:
        final_list.sort(key=lambda x: int(x.get("Zap", 0)))
    except Exception:
        pass

    df = pd.DataFrame(final_list)
    if "URL" in df.columns:
        df.drop_duplicates(subset=["URL"], keep="last", inplace=True)

    # Opcijsko: razširi Zaloga po centrih -> stolpci "Zaloga - <center>"
    if EXPORT_STOCK_COLUMNS and "Zaloga po centrih" in df.columns:
        stocks = df["Zaloga po centrih"].apply(_parse_stock_json)
        all_centers = sorted(set().union(*stocks.tolist())) if len(stocks) else []
        for c in all_centers:
            df[f"Zaloga - {c}"] = stocks.apply(lambda d: d.get(c, "") if isinstance(d, dict) else "")

    desired_cols = [
        "Skupina", "Zap", "Oznaka / naziv", "EAN",
        "Opis", "Opis izdelka",
        "EM", "Valuta", "DDV", "Proizvajalec",
        "Veljavnost od", "Dobava", "Zaloga po centrih",
        "Cena / EM (z DDV)", "Akcijska cena / EM (z DDV)",
        "Cena / EM (brez DDV)", "Akcijska cena / EM (brez DDV)",
        "URL", "SLIKA URL",
    ]
    dyn = [c for c in df.columns if isinstance(c, str) and c.startswith("Zaloga - ")]
    final_cols = desired_cols + [c for c in dyn if c not in desired_cols]

    for c in final_cols:
        if c not in df.columns:
            df[c] = ""

    # Zap: stabilno preštejemo 1..N (brez lukenj)
    if "Zap" in df.columns:
        df["Zap"] = pd.to_numeric(df["Zap"], errors="coerce")
        df.sort_values(by=["Zap"], inplace=True, na_position="last")
        df.reset_index(drop=True, inplace=True)
        df["Zap"] = df.index + 1

    df[final_cols].to_excel(excel_path, index=False)
    log_and_print("Excel shranjen (final).", to_file=True)


# --- Tehnoles-specific: kategorije + produkti ---

def _build_category_page_url(category_url, page_num):
    # Tehnoles uporablja pagenum. Če URL že vsebuje "?", uporabi "&"
    sep = "&" if "?" in category_url else "?"
    return f"{category_url}{sep}pagenum={page_num}"


def get_product_links_from_category(category_url):
    """
    Pobere produktne linke iz kategorije.
    Minimalno robustno: več selectorjev + detekcija ponavljanja (prvi produkt).
    """
    all_links = []
    page = 1
    first_prev = None

    while True:
        url = _build_category_page_url(category_url, page)
        log_and_print(f"  Stran {page}: {url}", to_file=True)

        html = get_page_content(url)
        if not html:
            break

        soup = BeautifulSoup(html, "html.parser")

        # Primarni selector iz tvoje skripte + fallback
        products = soup.select("li.wrapper_prods.category")
        if not products:
            products = soup.select("li.wrapper_prods, div.wrapper_prods, li.product, div.product")

        if not products:
            break

        # prva povezava (za anti-loop)
        first_link = None
        for it in products:
            a = it.select_one(".name a[href]") or it.select_one('a[href*="-p-"][href$=".aspx"]') or it.select_one("a[href]")
            if a and a.get("href"):
                first_link = urljoin(BASE_URL, a["href"].strip())
                break

        if page > 1 and first_link and first_prev and first_link == first_prev:
            log_and_print("  Vsebina strani se ponavlja -> zaključujem kategorijo.", to_file=True)
            break
        if first_link:
            first_prev = first_link

        for item in products:
            a = item.select_one(".name a[href]") or item.select_one('a[href*="-p-"][href$=".aspx"]')
            if not a or not a.get("href"):
                continue
            full = urljoin(BASE_URL, a["href"].strip())
            all_links.append(full)

        page += 1
        time.sleep(random.uniform(0.6, 2.0) if os.environ.get("GITHUB_ACTIONS", "").lower() == "true" else random.uniform(2.0, 5.0))

    # dedup, ohrani vrstni red
    return list(dict.fromkeys(all_links))


def _extract_image_url(soup):
    meta = soup.find("meta", property="og:image")
    if meta and meta.get("content"):
        return urljoin(BASE_URL, meta["content"].strip())

    a = soup.select_one("a.lightbox-image[href]")
    if a and a.get("href"):
        return urljoin(BASE_URL, a["href"].strip())

    img = soup.select_one("img[src]")
    if img and img.get("src"):
        return urljoin(BASE_URL, img["src"].strip())

    return ""


def _extract_long_description(soup):
    """
    Daljši opis (Opis izdelka): poskusi tipične containerje.
    Minimalno: če ne najde, vrne "".
    """
    for sel in [
        "#tab-description",
        "div#tab-description",
        "#tab_description",
        "div#tab_description",
        "div.productDescription",
        ".productDescription",
        ".product-info .description",
        "div.description",
    ]:
        el = soup.select_one(sel)
        if el:
            txt = el.get_text("\n", strip=True)
            if txt and len(txt) >= 20:
                return txt

    # fallback: včasih je opis v "productInfoContent"
    el = soup.select_one(".productInfoContent, .product-info-content")
    if el:
        txt = el.get_text("\n", strip=True)
        if txt and len(txt) >= 20:
            return txt

    return ""


def _extract_center_stock(soup):
    """
    Če obstaja tabela zaloge po poslovalnicah/centrih, jo poskusi prebrati.
    Tehnoles pogosto nima; zato naj bo funkcija "tiha" in varna.
    Vrne dict: {center: status/qty}
    """
    stock = {}

    # poskusi najti tabelo, ki po headerjih izgleda kot zaloga po poslovalnicah
    for tbl in soup.select("table"):
        header = " ".join(th.get_text(" ", strip=True).lower() for th in tbl.select("th"))
        if ("posloval" in header or "trgovin" in header or "center" in header) and ("zalog" in header or "količin" in header):
            rows = tbl.select("tr")
            for r in rows:
                tds = r.select("td")
                if len(tds) >= 2:
                    c = tds[0].get_text(" ", strip=True)
                    v = tds[1].get_text(" ", strip=True)
                    if c:
                        stock[c] = v
            if stock:
                return stock

    # fallback: div list (redko)
    for row in soup.select(".store-stock-row, .storeStockRow, .stockRow"):
        cols = row.find_all(["div", "span"], recursive=True)
        if len(cols) >= 2:
            c = cols[0].get_text(" ", strip=True)
            v = cols[1].get_text(" ", strip=True)
            if c:
                stock[c] = v

    return stock


def _compute_dobava(page_text, stock_dict, stock_value_text):
    """
    Dobava:
      - če zaloga po centrih: DA če vsaj en center kaže zalogo (heuristika)
      - sicer: iz 'Zaloga' vrednosti ali tekstov 'Na zalogi' / 'Ni na zalogi'
    """
    if stock_dict:
        for v in stock_dict.values():
            s = str(v).lower()
            if "na zalogi" in s:
                return "DA"
            m = re.search(r"(\d+)", s)
            if m and int(m.group(1)) > 0:
                return "DA"
        return "NE"

    if stock_value_text:
        s = str(stock_value_text).lower()
        if "na zalogi" in s:
            return "DA"
        if "ni na zalogi" in s or "razprod" in s:
            return "NE"
        m = re.search(r"(\d+)", s)
        if m:
            return "DA" if int(m.group(1)) > 0 else "NE"

    t = (page_text or "").lower()
    if "ni na zalogi" in t or "razprod" in t:
        return "NE"
    if "na zalogi" in t:
        return "DA"

    return ""


def _extract_from_stock_table(soup):
    """
    Tehnoles (kot v tvoji skripti) ima tabelo .listing.stockMargin:
      Ident, Enota mere, včasih EAN, Blagovna znamka/Proizvajalec, Zaloga, ...
    """
    ident = ""
    em = ""
    ean = ""
    manufacturer = ""
    stock_value = ""

    rows = soup.select(".listing.stockMargin tr")
    for row in rows:
        cells = row.select("td")
        if len(cells) != 2:
            continue
        k = cells[0].get_text(" ", strip=True)
        v = cells[1].get_text(" ", strip=True)

        lk = k.lower()
        if "ident" in lk or "šifra" in lk or "sifra" in lk:
            ident = v
        elif "enota mere" in lk or (lk.strip() == "enota"):
            em = v
        elif "ean" in lk or "gtin" in lk:
            ean = v  # RAW (brez validacije dolžine)
        elif "proizvajalec" in lk or "blagovna znamka" in lk or "znamka" in lk:
            manufacturer = v
        elif "zaloga" in lk or "dobavljivost" in lk:
            stock_value = v

    return ident, em, ean, manufacturer, stock_value


def _extract_prices(soup):
    """
    Vrne (regular_price_float, action_price_float).
    Pravila:
      - Če je akcijska in redna: akcijska=productSpecialPrice, redna=stara/old/priceColor
      - Če je samo ena: redna = ta, akcijska = None
    """
    def _text_price(sel):
        el = soup.select_one(sel)
        if not el:
            return None
        m = re.search(r"(\d{1,3}(?:\.\d{3})*,\d{2}|\d+,\d{2}|\d+)", el.get_text(" ", strip=True))
        return parse_price_eur(m.group(1)) if m else None

    special = _text_price("span.productSpecialPrice")
    # "old" / regular pri akciji (različne teme)
    old_candidates = [
        "span.productOldPrice",
        "span.productListPrice",
        "span.oldPrice",
        "span.priceColor",
        "span.price",
        ".price-old",
    ]
    regular = None
    for sel in old_candidates:
        p = _text_price(sel)
        if p is None:
            continue
        # če je special enaka regular, ne štej kot 'old'
        if special is not None and abs(p - special) < 1e-9:
            continue
        regular = p
        break

    if special is not None:
        # akcijska obstaja
        action = special
        if regular is None:
            # če ne najdemo stare, daj vsaj eno ceno
            regular, action = special, None
        return regular, action

    # brez akcije -> regular iz priceColor/price
    regular = _text_price("span.priceColor") or _text_price("span.price") or _text_price(".price-new")
    return regular, None


def get_product_details(url, group_name, date):
    """Pridobi podrobnosti izdelka (cena 2 dec, EM whitelist, opis izdelka, dobava...)."""
    global _global_item_counter

    log_and_print(f"    - Detajli: {url}", to_file=True)
    html = get_page_content(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text("\n", strip=True)

    # naslov
    h1 = soup.select_one("h1.productInfo") or soup.find("h1")
    title = h1.get_text(strip=True) if h1 else ""

    # tabela specifikacij
    ident, em_raw, ean_raw, manufacturer, stock_value = _extract_from_stock_table(soup)

    # cene
    regular_price, action_price = _extract_prices(soup)

    if regular_price is None and action_price is None:
        log_and_print("      Preskakujem (ni cene).", to_file=True)
        return None

    # Opis izdelka (daljši opis)
    long_desc = _extract_long_description(soup)

    # slika
    img_url = _extract_image_url(soup)

    # zaloga po centrih (če obstaja)
    center_stock = _extract_center_stock(soup)
    center_stock_json = json.dumps(center_stock, ensure_ascii=False) if center_stock else ""

    # dobava
    dobava = _compute_dobava(page_text, center_stock, stock_value)

    # Zap dodelimo šele, ko vemo da je izdelek OK (da ne delamo lukenj)
    _global_item_counter += 1

    data = {
        "Skupina": group_name,
        "Zap": _global_item_counter,

        # zahteva: oznaka/naziv = šifra artikla (clean)
        "Oznaka / naziv": ident,

        # zahteva: EAN raw (brez validacije)
        "EAN": ean_raw,

        "Opis": title,
        "Opis izdelka": long_desc,

        # zahteva: EM whitelist, sicer kos
        "EM": normalize_em(em_raw),

        "Valuta": "EUR",
        "DDV": "22",
        "Proizvajalec": manufacturer,
        "Veljavnost od": date,

        # zahteva: dobava + zaloga po centrih json string
        "Dobava": dobava,
        "Zaloga po centrih": center_stock_json,

        # zahteva: cena 2 dec
        "Cena / EM (z DDV)": round(float(regular_price), 2) if regular_price is not None else "",
        "Akcijska cena / EM (z DDV)": round(float(action_price), 2) if action_price is not None else "",

        "Cena / EM (brez DDV)": price_without_vat(regular_price, DDV_RATE) if regular_price is not None else "",
        "Akcijska cena / EM (brez DDV)": price_without_vat(action_price, DDV_RATE) if action_price is not None else "",

        "URL": url,
        "SLIKA URL": img_url,
    }

    return data


def main():
    global _log_file, _global_item_counter

    # CI: manjši jitter; lokalno: malo več
    is_ci = os.environ.get("GITHUB_ACTIONS", "").lower() == "true" or bool(os.environ.get("CI"))
    time.sleep(random.uniform(0.2, 1.0) if is_ci else random.uniform(1.0, 8.0))

    paths = create_output_paths()
    json_path = paths["json_path"]
    excel_path = paths["excel_path"]
    log_path = paths["log_path"]

    try:
        _log_file = open(log_path, "w", encoding="utf-8")
    except Exception:
        return

    log_and_print(f"--- Zagon {SHOP_NAME} ---", to_file=True)
    log_and_print(f"JSON:  {json_path}", to_file=True)
    log_and_print(f"Excel: {excel_path}", to_file=True)
    log_and_print(f"Log:   {log_path}", to_file=True)

    # resume (če obstaja JSON za ta dan)
    load_existing_json(json_path)

    date = datetime.now().strftime("%d/%m/%Y")
    newly_added = 0

    try:
        for cat, urls in TEHNOLES_CATEGORIES.items():
            log_and_print(f"\n--- {cat} ---", to_file=True)

            for u in urls:
                # ime podkategorije iz URL-ja (kot prej)
                sub_name = u.split("/")[-1].split("-c-")[0] or cat
                log_and_print(f"  Podkategorija: {sub_name}", to_file=True)

                links = get_product_links_from_category(u)
                if not links:
                    continue

                for link in links:
                    if link in _existing_urls:
                        continue

                    det = get_product_details(link, sub_name, date)
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
