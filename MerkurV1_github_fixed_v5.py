import pandas as pd
import os
from datetime import datetime
import time
import random
import re
import json
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# --- Konfiguracija ---
SHOP_NAME = "Merkur"
BASE_URL = "https://www.merkur.si"
DDV_RATE = 0.22

# --- Varnostne nastavitve (brez bypass; samo osnovni headers + random UA) ---
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
]

# EM whitelist (če ni v tem seznamu -> "kos")
EM_WHITELIST = {
    "kos",
    "m", "m2", "m3",
    "kg",
    "l",
    "t",
    "cm", "mm",
}

EM_ALIASES = {
    "m²": "m2",
    "m^2": "m2",
    "㎡": "m2",
    "m³": "m3",
    "m^3": "m3",
    "㎥": "m3",
    "pc": "kos",
    "pcs": "kos",
    "kom": "kos",
    "komad": "kos",
}

PRICE_UNIT_RE = re.compile(
    r"(?P<price>\d{1,3}(?:\.\d{3})*,\d{2})\s*€\s*/\s*(?P<unit>[^\s]+)",
    re.IGNORECASE
)

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

# --- Globalne spremenljivke ---
_log_file = None
_global_item_counter = 0
_session = requests.Session()
_session.headers.update({
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "sl-SI,sl;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
})


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
    Zahteva: create_output_paths() mora obstajati v vsaki datoteki.

    Output struktura:
      OUTPUT_DIR/Ceniki_Scraping/<SHOP>/<YYYY-MM-DD>/

    GitHub/CI:
      - če je nastavljen env OUTPUT_DIR, se vse piše pod to mapo (npr. artifacts/)
      - drugače se piše ob skripti
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_root = os.environ.get("OUTPUT_DIR", script_dir)

    run_date = datetime.now().strftime("%Y-%m-%d")
    daily_dir = os.path.join(output_root, "Ceniki_Scraping", SHOP_NAME, run_date)
    os.makedirs(daily_dir, exist_ok=True)

    excel_path = os.path.join(daily_dir, f"{SHOP_NAME}_{run_date}.xlsx")
    json_path = os.path.join(daily_dir, f"{SHOP_NAME}_{run_date}.json")
    log_path = os.path.join(daily_dir, f"{SHOP_NAME}_{run_date}.log")

    return {
        "daily_dir": daily_dir,
        "excel_path": excel_path,
        "json_path": json_path,
        "log_path": log_path,
        "run_date": run_date,
    }


def save_to_json(data, filepath):
    """Shrani podatke v JSON (UTF-8, pretty) - atomic write (GitHub friendly)."""
    try:
        tmp_path = filepath + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data if data else [], f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, filepath)
        log_and_print(f"JSON checkpoint shranjen: {filepath}", to_file=True)
    except Exception as e:
        log_and_print(f"Napaka pri shranjevanju v JSON: {e}", to_file=True)


def _parse_center_stock_json(s):
    if not s or not isinstance(s, str):
        return {}
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def save_to_excel(data, filepath):
    """Excel generiramo 1× na koncu (in v finally), brez agresivnih vmesnih zapisov."""
    if not data:
        log_and_print("Ni podatkov za shranjevanje v Excel.", to_file=True)
        return

    try:
        df = pd.DataFrame(data)

        # dedup po URL
        if "URL" in df.columns:
            df.drop_duplicates(subset=["URL"], keep="last", inplace=True)

        # Zap: vedno preštejemo na novo po sortiranju (stabilno)
        if "Zap" in df.columns:
            df["Zap"] = pd.to_numeric(df["Zap"], errors="coerce")

        # Razširi zalogo po centrih v ločene Excel stolpce (opcijsko)
        if "Zaloga po centrih" in df.columns:
            center_dicts = df["Zaloga po centrih"].apply(_parse_center_stock_json)
            all_centers = sorted(set().union(*center_dicts.tolist())) if len(center_dicts) else []
            for center in all_centers:
                col = f"Zaloga - {center}"
                df[col] = center_dicts.apply(lambda d: d.get(center, "") if isinstance(d, dict) else "")

        desired_columns = [
            "Skupina", "Zap", "Oznaka / naziv", "EAN",
            "Opis", "Opis izdelka",
            "EM", "Valuta", "DDV", "Proizvajalec", "Veljavnost od",
            "Dobava", "Zaloga po centrih",
            "Cena / EM (z DDV)", "Akcijska cena / EM (z DDV)",
            "Cena / EM (brez DDV)", "Akcijska cena / EM (brez DDV)",
            "URL", "SLIKA URL",
        ]

        # dodamo dinamične "Zaloga - <center>" stolpce na konec (če obstajajo)
        dynamic_stock_cols = [c for c in df.columns if isinstance(c, str) and c.startswith("Zaloga - ")]
        desired_columns_extended = desired_columns + [c for c in dynamic_stock_cols if c not in desired_columns]

        for col in desired_columns_extended:
            if col not in df.columns:
                df[col] = ""

        # sort + Zap reset
        if "Zap" in df.columns:
            df.sort_values(by=["Zap"], inplace=True, na_position="last")
            df.reset_index(drop=True, inplace=True)
            df["Zap"] = df.index + 1

        df_final = df[desired_columns_extended]
        df_final.to_excel(filepath, index=False)

        log_and_print(f"Excel uspešno shranjen: {filepath}", to_file=True)
    except Exception as e:
        error_msg = f"Napaka pri shranjevanju v Excel: {e}"
        log_and_print(error_msg, to_file=True)
        print(f"CRITICAL ERROR: {error_msg}")


def get_page_content(url):
    """
    Brez bypass zaščit.
    Če naletimo na očiten challenge po status kodi (403/429/503), samo logamo in vrnemo None.
    """
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    try:
        resp = _session.get(url, headers=headers, timeout=25, allow_redirects=True)
        status = resp.status_code

        if status in (403, 429, 503):
            log_and_print(f"⚠️ Možen challenge ({status}) za URL: {url} -> preskakujem", to_file=True)
            return None

        if status >= 400:
            log_and_print(f"Napaka HTTP {status} za URL: {url} -> preskakujem", to_file=True)
            return None

        return resp.text
    except requests.exceptions.RequestException as e:
        log_and_print(f"Napaka pri dostopu do URL-ja {url}: {e}", to_file=True)
        return None


# --- Normalizacije / parsing ---

def normalize_em(em_raw):
    if not em_raw:
        return "kos"
    em = em_raw.strip().lower()
    em = EM_ALIASES.get(em, em)
    em = em.replace("/", "").strip()
    em = re.sub(r"[^a-z0-9²³^]", "", em)  # pustimo unicode ²/³ za mapiranje
    em = EM_ALIASES.get(em, em)
    em = re.sub(r"[^a-z0-9]", "", em)
    return em if em in EM_WHITELIST else "kos"


def parse_price_eur(price_raw):
    """
    "1.234,56" -> 1234.56
    "21,45"    -> 21.45
    """
    if not price_raw:
        return None
    s = price_raw.strip().replace(".", "").replace(",", ".")
    try:
        return round(float(s), 2)
    except ValueError:
        return None


def infer_em_from_text(text):
    """Fallback, če na strani ni jasnega '€ / <enota>'."""
    if not text:
        return "kos"
    t = text.lower()
    if "m²" in t or "m2" in t:
        return "m2"
    if "m³" in t or "m3" in t:
        return "m3"
    if re.search(r"\bkg\b", t):
        return "kg"
    # pazimo, da ne zamenjamo "cm/mm" za "m"
    if re.search(r"(^|[^a-z0-9])m($|[^a-z0-9])", t):
        return "m"
    return "kos"


def extract_price_and_em(page_text, title_text=""):
    """
    Izbere prvi match "xx,xx € / enota" (na Merkur strani je to običajno glavna cena).
    """
    matches = list(PRICE_UNIT_RE.finditer(page_text or ""))
    if matches:
        m = matches[0]
        price = parse_price_eur(m.group("price"))
        unit = normalize_em(m.group("unit"))
        if price is not None:
            return price, unit

    # fallback: poskusi EM iz naslova/opisa
    unit = normalize_em(infer_em_from_text((title_text or "") + " " + (page_text or "")))
    return None, unit


def extract_sifra_izdelka(page_text, product_url=""):
    # Merkur: "Šifra izdelka: 3377474"
    m = re.search(r"Šifra\s+izdelka:\s*([^\s]+)", page_text or "", flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()

    # fallback: zadnji številčni del URL-ja (če obstaja)
    m2 = re.search(r"(\d{5,})/?$", product_url or "")
    if m2:
        return m2.group(1)
    return ""


def extract_ean_raw(page_text):
    """
    EAN pustimo "raw" (brez validacije dolžine).
    Če je na strani, ga poberemo iz vrstice.
    """
    if not page_text:
        return ""
    patterns = [
        r"\bEAN\b\s*[:\-]?\s*([^\n\r]+)",
        r"\bEAN\s+koda\b\s*[:\-]?\s*([^\n\r]+)",
    ]
    for pat in patterns:
        m = re.search(pat, page_text, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return ""


def extract_brand_or_manufacturer(page_text):
    # "Blagovna znamka URSAXPS"
    m = re.search(r"\bBlagovna\s+znamka\s+([^\n\r]+)", page_text or "", flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()

    # "Proizvajalec/Uvoznik: FRAGMAT TIM D.O.O."
    m2 = re.search(r"\bProizvajalec/Uvoznik:\s*([^\n\r]+)", page_text or "", flags=re.IGNORECASE)
    if m2:
        return m2.group(1).strip()

    return ""


def extract_long_description(page_text):
    """
    Poskusi zajeti tekst med sekcijo "Opis" in "Tehnične podrobnosti".
    """
    if not page_text:
        return ""

    m = re.search(
        r"\bOpis\b(.*?)(\bTehnične\s+podrobnosti\b|\bDodatne\s+priloge\b|\bMnenja\b)",
        page_text,
        flags=re.IGNORECASE | re.DOTALL
    )
    if not m:
        return ""

    desc = m.group(1)
    desc = re.sub(r"\n{3,}", "\n\n", desc).strip()
    return desc


def extract_center_stock(page_text):
    """
    Pobere zalogo po centrih iz sekcije "Zaloga v trgovskih centrih".
    Vrne dict: {"MERKUR LJUBLJANA BTC": "Ni zaloge", ...}
    """
    if not page_text:
        return {}

    idx = (page_text.lower()).find("zaloga v trgovskih centrih")
    if idx == -1:
        return {}

    block = page_text[idx:]
    lines = [ln.strip() for ln in block.splitlines() if ln.strip()]

    stock = {}
    status = None
    for ln in lines:
        lnl = ln.lower()

        if lnl in ("na zalogi", "ni zaloge", "zadnji kosi"):
            status = ln.strip()
            continue

        if status and ln.upper().startswith("MERKUR "):
            # odreži telefon
            center_clean = re.split(r"\s*\(\+?\d", ln)[0].strip()
            stock[center_clean] = status
            status = None

        # varovalka: če pridemo do zelo nerelevantnih delov, lahko zaključimo (ne agresivno)
        if "brezplačna pomoč pri nakupu" in lnl:
            break

    return stock


def compute_dobava(center_stock, page_text):
    """
    Dobava = DA/NE:
      - če imamo center_stock: DA če vsaj en center ni "Ni zaloge"
      - sicer fallback na globalni "Na zalogi"/"Ni zaloge" v vrhu strani
    """
    if center_stock:
        for st in center_stock.values():
            if st.strip().lower() != "ni zaloge":
                return "DA"
        return "NE"

    if re.search(r"\bNa\s+zalogi\b", page_text or "", flags=re.IGNORECASE):
        return "DA"
    if re.search(r"\bNi\s+zaloge\b", page_text or "", flags=re.IGNORECASE):
        return "NE"
    return "NE"


def _get_best_image_url(item_html, soup2):
    """
    Najprej og:image iz produktne strani, sicer fallback na sliko iz liste.
    """
    try:
        meta = soup2.find("meta", property="og:image")
        if meta and meta.get("content"):
            return urljoin(BASE_URL, meta.get("content").strip())
    except Exception:
        pass

    # fallback: lista
    if item_html:
        img = item_html.find("img")
        if img:
            src = img.get("src") or img.get("data-src") or ""
            if src:
                return urljoin(BASE_URL, src.strip())
    return ""


# --- Funkcije, specifične za Merkur ---

def get_product_details(product_url, group_name, query_date, item_html):
    """Pridobi podrobnosti o izdelku (Cena/EM, opis, zaloga po centrih...)."""
    global _global_item_counter

    details_html = get_page_content(product_url)
    if not details_html:
        log_and_print(f"      Preskakujem (ni vsebine): {product_url}", to_file=True)
        return None

    soup2 = BeautifulSoup(details_html, "html.parser")
    page_text = soup2.get_text("\n")

    # naslov (kratki opis)
    h1 = soup2.find("h1")
    title = h1.get_text(strip=True) if h1 else ""
    if not title and item_html and item_html.h3:
        title = item_html.h3.get_text(strip=True)

    # šifra (oznaka/naziv)
    sifra = extract_sifra_izdelka(page_text, product_url=product_url)

    # cena + EM
    price_with_vat, em = extract_price_and_em(page_text, title_text=title)
    em = normalize_em(em)

    # fallback: če ni cene na produktni strani, poskusi iz liste (zelo konservativno)
    if price_with_vat is None and item_html:
        try:
            # včasih je v listi že cena; poberemo prvo "xx,xx"
            spans = item_html.find_all("span")
            for sp in spans:
                m = re.search(r"(\d{1,3}(?:\.\d{3})*,\d{2})", sp.get_text(" ", strip=True))
                if m:
                    price_with_vat = parse_price_eur(m.group(1))
                    break
        except Exception:
            pass

    if price_with_vat is None:
        log_and_print(f"      Preskakujem (ni cene): {product_url}", to_file=True)
        return None

    # dolgi opis
    long_desc = extract_long_description(page_text)

    # EAN raw (brez validacije)
    ean_raw = extract_ean_raw(page_text)

    # proizvajalec/blagovna znamka (če je)
    manufacturer = extract_brand_or_manufacturer(page_text)

    # zaloga po centrih + dobava DA/NE
    center_stock = extract_center_stock(page_text)
    dobava = compute_dobava(center_stock, page_text)
    center_stock_json = json.dumps(center_stock, ensure_ascii=False) if center_stock else ""

    # slika
    image_url = _get_best_image_url(item_html, soup2)

    _global_item_counter += 1

    product_data = {
        "Skupina": group_name,
        "Zap": _global_item_counter,
        "URL": product_url,
        "Veljavnost od": query_date,
        "Valuta": "EUR",
        "DDV": "22",

        # Zahteva: Oznaka/naziv = šifra artikla (clean)
        "Oznaka / naziv": sifra,

        # Zahteva: EAN raw
        "EAN": ean_raw,

        # kratki opis/naziv izdelka
        "Opis": title,

        # Zahteva: dodan daljši opis
        "Opis izdelka": long_desc,

        # Zahteva: EM whitelist, sicer kos
        "EM": em,

        "Proizvajalec": manufacturer,

        # Zahteva: Dobava DA/NE + zaloga po centrih JSON string
        "Dobava": dobava,
        "Zaloga po centrih": center_stock_json,

        # Zahteva: cena zaokrožena na 2 dec
        "Cena / EM (z DDV)": round(float(price_with_vat), 2),

        # trenutno ne parsamo akcijske (pustimo prazno)
        "Akcijska cena / EM (z DDV)": "",
        "Akcijska cena / EM (brez DDV)": "",

        "Cena / EM (brez DDV)": round(float(price_with_vat) / (1 + DDV_RATE), 2),

        "SLIKA URL": image_url,
    }

    # če slučajno šifra manjka, vsaj ne nastavi labela
    if not product_data["Oznaka / naziv"]:
        product_data["Oznaka / naziv"] = ""

    return product_data


# --- Glavna funkcija ---

def main():
    global _log_file, _global_item_counter

    paths = create_output_paths()
    output_filepath = paths["excel_path"]
    json_filepath = paths["json_path"]
    log_filepath = paths["log_path"]

    try:
        _log_file = open(log_filepath, "w", encoding="utf-8")
    except Exception as e:
        print(f"CRITICAL ERROR: Ni mogoče ustvariti log datoteke: {e}")
        return

    log_and_print(f"--- Zagon zajemanja podatkov iz {SHOP_NAME} ---", to_file=True)
    log_and_print(f"Excel: {output_filepath}", to_file=True)
    log_and_print(f"JSON:  {json_filepath}", to_file=True)
    log_and_print(f"LOG:   {log_filepath}", to_file=True)

    all_products_data = []

    # GitHub-friendly resume: najprej poskusi JSON (ker ga checkpointamo), potem Excel (če obstaja)
    if os.path.exists(json_filepath):
        try:
            with open(json_filepath, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, list):
                all_products_data = loaded
            numeric_zaps = pd.to_numeric(pd.Series([d.get("Zap") for d in all_products_data]), errors="coerce").dropna()
            if not numeric_zaps.empty:
                _global_item_counter = int(numeric_zaps.max())
            log_and_print(f"Naložen obstoječ JSON. Števec 'Zap' = {_global_item_counter}.", to_file=True)
        except Exception as e:
            log_and_print(f"Napaka pri branju obstoječega JSON: {e}. Začenjam na novo.", to_file=True)
            all_products_data = []

    elif os.path.exists(output_filepath):
        try:
            existing_df = pd.read_excel(output_filepath)
            all_products_data = existing_df.to_dict(orient="records")
            if not existing_df.empty and "Zap" in existing_df.columns:
                numeric_zaps = pd.to_numeric(existing_df["Zap"], errors="coerce").dropna()
                if not numeric_zaps.empty:
                    _global_item_counter = int(numeric_zaps.max())
            log_and_print(f"Naložen obstoječ Excel. Števec 'Zap' = {_global_item_counter}.", to_file=True)
        except Exception as e:
            log_and_print(f"Napaka pri nalaganju obstoječih podatkov: {e}. Začenjam na novo.", to_file=True)
            all_products_data = []

    # datum v ISO (bolj konsistentno za CI)
    query_date = datetime.now().strftime("%Y-%m-%d")

    try:
        for main_category_name, subcategory_urls in MERKUR_CATEGORIES.items():
            log_and_print(f"\n--- Obdelujem glavno kategorijo: {main_category_name} ---", to_file=True)

            for sub_cat_url in subcategory_urls:
                sub_cat_name = sub_cat_url.strip("/").split("/")[-1]
                group_name_for_excel = sub_cat_name.replace("-", " ").capitalize()

                log_and_print(f"\n  -- Podkategorija: {group_name_for_excel} --", to_file=True)

                stariprvi = "star"
                n = 1
                new_data_for_category = []

                # set obstoječih URL-jev
                existing_urls = {d.get("URL") for d in all_products_data if d.get("URL")}
                existing_urls |= {d.get("URL") for d in new_data_for_category if d.get("URL")}

                while True:
                    paginated_url = f"{sub_cat_url}?p={n}#section-products"
                    log_and_print(f"    Stran {n}: {paginated_url}", to_file=True)

                    html_content = get_page_content(paginated_url)
                    if not html_content:
                        log_and_print(f"      Ni vsebine za stran {n} -> zaključujem podkategorijo.", to_file=True)
                        break

                    soup1 = BeautifulSoup(html_content, "html.parser")
                    item_container = soup1.find("div", class_="list-items")
                    if not item_container:
                        log_and_print("      Ni 'list-items' container -> zaključujem podkategorijo.", to_file=True)
                        break

                    izdelek_list = item_container.find_all("div", class_="item")
                    if not izdelek_list:
                        log_and_print("      Ni izdelkov na strani -> zaključujem podkategorijo.", to_file=True)
                        break

                    noviprvi = izdelek_list[0].h3.get_text(strip=True) if izdelek_list[0].h3 else None
                    if n > 1 and noviprvi == stariprvi:
                        log_and_print(f"      Vsebina strani {n} se ponavlja -> zaključujem.", to_file=True)
                        break
                    stariprvi = noviprvi

                    page_new_count = 0

                    for item in izdelek_list:
                        link_tag = item.find("a")
                        if not (link_tag and link_tag.get("href")):
                            continue

                        href = link_tag.get("href").strip()
                        product_url = urljoin(BASE_URL, href)

                        if product_url in existing_urls:
                            continue

                        details = get_product_details(product_url, group_name_for_excel, query_date, item)

                        if details:
                            new_data_for_category.append(details)
                            existing_urls.add(product_url)
                            page_new_count += 1

                        # throttling (GitHub-friendly)
                        is_ci = os.environ.get("GITHUB_ACTIONS", "").lower() == "true"
                        if is_ci:
                            time.sleep(random.uniform(0.6, 2.2))
                        else:
                            time.sleep(random.uniform(2.0, 12.0))

                    # JSON checkpoint po vsaki strani (batch)
                    if page_new_count > 0:
                        save_to_json(all_products_data + new_data_for_category, json_filepath)

                    # naslednja stran?
                    if not soup1.select_one("a.next"):
                        break
                    n += 1

                if new_data_for_category:
                    all_products_data.extend(new_data_for_category)
                    save_to_json(all_products_data, json_filepath)

        # Excel 1× na koncu
        save_to_excel(all_products_data, output_filepath)

    except KeyboardInterrupt:
        log_and_print("\nSkripta prekinjena (KeyboardInterrupt). Shranjujem JSON + Excel...", to_file=True)
    except Exception as e:
        log_and_print(f"\nNEPRIČAKOVANA NAPAKA: {e}", to_file=True)
        import traceback
        traceback.print_exc(file=_log_file)
        print(f"Nepričakovana napaka: {e}. Podrobnosti so v logu.")
    finally:
        # vedno poskusi shraniti
        save_to_json(all_products_data, json_filepath)
        save_to_excel(all_products_data, output_filepath)
        log_and_print("\n--- Zajemanje zaključeno ---", to_file=True)
        print(f"Zaključeno. Podatki so v: {output_filepath} in {json_filepath}")
        if _log_file:
            _log_file.close()


# --- ZAGON BREZ GUI ---
if __name__ == "__main__":
    main()
