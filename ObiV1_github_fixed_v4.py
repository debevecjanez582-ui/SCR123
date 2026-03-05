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
        "https://www.obi.si/c/gradnja-877/ploscice-308/robne-ploscice-1152"
    ],
    "Ureditev okolice": [
        "https://www.obi.si/c/gradnja-877/ureditev-okolice-336/pohodne-plosce-914",
        "https://www.obi.si/c/gradnja-877/ureditev-okolice-336/tlakovci-608",
        "https://www.obi.si/c/gradnja-877/ureditev-okolice-336/obrobe-stopnice-in-zidni-sistemi-1281",
        "https://www.obi.si/c/gradnja-877/ureditev-okolice-336/terasne-deske-1464",
        "https://www.obi.si/c/gradnja-877/ureditev-okolice-336/terasne-in-pohodne-plosce-1279",
        "https://www.obi.si/c/gradnja-877/ureditev-okolice-336/okrasni-prod-in-okrasni-drobljenec-1382"
    ],
    "Gradbeni materiali": [
        "https://www.obi.si/c/gradnja-877/gradbeni-materiali-175/omet-malta-in-cement-619",
        "https://www.obi.si/c/gradnja-877/gradbeni-materiali-175/suha-gradnja-764",
        "https://www.obi.si/c/gradnja-877/gradbeni-materiali-175/kamni-in-pesek-720",
        "https://www.obi.si/c/gradnja-877/gradbeni-materiali-175/izolacijski-material-233"
    ]
}

# --- Stabilnost / GitHub-friendly ---
CHECKPOINT_EVERY_N_ITEMS = 5  # JSON checkpoint batch
EXPORT_STOCK_COLUMNS = os.environ.get("EXPORT_STOCK_COLUMNS", "").strip().lower() == "true"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
]

# --- Enote (whitelist + normalizacija) ---
UNIT_WHITELIST = {
    "kos", "m", "m2", "m3", "kg", "g", "l", "ml",
    "pak", "set", "par", "rola"
}
UNIT_ALIASES = {
    "m²": "m2",
    "m2": "m2",
    "m³": "m3",
    "m3": "m3",
    "kom": "kos",
    "kom.": "kos",
    "kos": "kos",
    "/kos": "kos",
    "/m2": "m2",
    "/m²": "m2",
    "/m3": "m3",
    "/m³": "m3",
    "/m": "m",
    "/kg": "kg",
    "/l": "l",
}

_log_file = None
_global_item_counter = 0

# In-memory state for resume / idempotency
_existing_urls = set()
_all_data_by_url = {}

_session = requests.Session()


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


def create_output_paths(shop_name):
    """Ustvari poti za JSON/Excel in log.

    GitHub/CI:
      - če je nastavljen env OUTPUT_DIR, se vse piše pod to mapo (npr. artifacts/)
      - drugače se piše ob skripti
    Struktura:
      OUTPUT_ROOT/Ceniki_Scraping/<SHOP>/<YYYY-MM-DD>/
    """
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


def normalize_em(raw_unit: str) -> str:
    """Normalizira EM; če ni v whitelist -> 'kos'."""
    if not raw_unit:
        return "kos"
    u = raw_unit.strip().lower()
    u = u.replace(" ", "")
    u = UNIT_ALIASES.get(u, u)
    # odstrani leading slash, če je
    if u.startswith("/"):
        u = u[1:]
    u = UNIT_ALIASES.get(u, u)
    return u if u in UNIT_WHITELIST else "kos"


def parse_price_to_float(price_text: str):
    """Izlušči številko cene (EU format) -> float. Vrne None, če ne gre."""
    if not price_text:
        return None
    # iščemo npr. 6,48 ali 1.234,56 ali 19,99
    m = re.search(r"(\d{1,3}(?:\.\d{3})*,\d{2}|\d+,\d{2}|\d+)", price_text)
    if not m:
        return None
    s = m.group(1)
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def format_price_2dec(price_float):
    """float -> '6,48' (2 dec)."""
    if price_float is None:
        return ""
    try:
        return f"{float(price_float):.2f}".replace(".", ",")
    except Exception:
        return ""


def convert_price_to_without_vat(price_str, vat_rate):
    """Cena brez DDV, vedno 2 dec."""
    val = parse_price_to_float(price_str)
    if val is None:
        return ""
    return format_price_2dec(val / (1 + vat_rate))


def get_page_content(url):
    """Fetch HTML. Ne dela 'block/captcha' agresivnih detekcij.
    Če pride do 403/429/503 ali podobno: log + vrni None (skripta nadaljuje).
    """
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "sl-SI,sl;q=0.9,en;q=0.7",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive",
    }
    try:
        resp = _session.get(url, headers=headers, timeout=25)
        if resp.status_code >= 400:
            log_and_print(f"HTTP {resp.status_code} za {url} (preskakujem)", to_file=True)
            return None
        return resp.text
    except requests.exceptions.RequestException as e:
        log_and_print(f"Napaka pri dostopu do URL-ja {url}: {e}", to_file=True)
        return None


def _extract_balanced_json(text: str, start_index: int):
    """Izreže JSON objekt iz texta od '{' dalje (uravnotežene oklepaje + string escape)."""
    if start_index < 0 or start_index >= len(text) or text[start_index] != "{":
        return None
    depth = 0
    in_str = False
    esc = False
    for i in range(start_index, len(text)):
        ch = text[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
        else:
            if ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return text[start_index : i + 1]
    return None


def extract_stock_from_listing_item(item_soup):
    """Izlušči zalogo po centrih iz JS (window.options... = {...};) znotraj product tile-a.
    Vrne:
      - dobava ('DA'/'NE'/'')
      - zaloga_json_string (ali '')
      - stock_dict (ali None)
    """
    try:
        raw = str(item_soup)
        m = re.search(r"window\.options[a-zA-Z0-9_]+\s*=\s*\{", raw)
        if not m:
            return "", "", None

        json_start = raw.find("{", m.start())
        obj_str = _extract_balanced_json(raw, json_start)
        if not obj_str:
            return "", "", None

        obj = json.loads(obj_str)
        stock_list = obj.get("stock", []) or []
        stock_dict = {}

        has_any_stock = False
        for entry in stock_list:
            # ime centra:
            center_name = entry.get("name")
            if not center_name:
                center_name = (entry.get("store") or {}).get("name")

            qty = entry.get("quantity")
            qty_text = entry.get("quantity_text")

            if center_name:
                # raje zapišemo "163 kosov" kot tekst (v JSON string)
                stock_dict[center_name] = qty_text if qty_text is not None else qty

            try:
                if qty is not None and float(qty) > 0:
                    has_any_stock = True
            except Exception:
                pass

        dobava = "DA" if has_any_stock else "NE"
        zaloga_json_string = json.dumps(stock_dict, ensure_ascii=False)

        return dobava, zaloga_json_string, stock_dict
    except Exception:
        return "", "", None


def _extract_price_units_from_lines(lines, preferred_unit=None):
    """Iz vrste stringov poišče cene in enote in izbere najbolj smiselno.
    Vrne (price_str_2dec, em_normalized).
    """
    pairs = []
    for ln in lines:
        if "€" not in ln:
            continue
        # npr: "6,48 € / m2" ali "4,86 € /kos" ali "3,19 €"
        m = re.search(r"(\d{1,3}(?:\.\d{3})*,\d{2}|\d+,\d{2}|\d+)\s*€(?:\s*/\s*([A-Za-z0-9²³]+))?", ln)
        if not m:
            continue
        price_raw = m.group(1)
        unit_raw = (m.group(2) or "").strip()
        pairs.append((price_raw, unit_raw))

    if not pairs:
        return "", "kos"

    # če imamo preferred unit (npr. Moja površina -> m2), poskusi zadet
    if preferred_unit:
        pu = normalize_em(preferred_unit)
        for pr, ur in pairs:
            if normalize_em(ur) == pu and pr:
                return format_price_2dec(parse_price_to_float(pr)), pu

    # drugače preferiraj prvo "ne-kos" enoto iz whitelista
    for pr, ur in pairs:
        em = normalize_em(ur)
        if em != "kos":
            return format_price_2dec(parse_price_to_float(pr)), em

    # fallback: prva cena, kos
    pr, ur = pairs[0]
    em = normalize_em(ur)
    return format_price_2dec(parse_price_to_float(pr)), em


def parse_product_page_details(html):
    """Parsanje product page: naziv, št. art, cena/enota, opis (daljši), proizvajalec, EAN, slika, dostava."""
    soup = BeautifulSoup(html, "lxml")

    # Full text lines (robustno za sekcije "Opis", "Podatki proizvajalca", ...)
    full_text = soup.get_text("\n", strip=True)
    lines = [l.strip() for l in full_text.splitlines() if l.strip()]

    # Naziv (Opis)
    title = ""
    h1 = soup.find("h1")
    if h1:
        title = h1.get_text(strip=True)

    # Št. art. -> Oznaka / naziv
    oznaka = ""
    m_art = re.search(r"Št\.\s*art\.?:\s*([0-9A-Za-z\-]+)", full_text)
    if m_art:
        oznaka = m_art.group(1).strip()

    # Proizvajalec
    proizvajalec = ""
    # (1) Blagovna znamka X
    for ln in lines:
        if ln.lower().startswith("blagovna znamka"):
            proizvajalec = ln.replace("Blagovna znamka", "").strip()
            break
    # (2) Podatki proizvajalca -> naslednja vrstica
    if not proizvajalec:
        try:
            idx = lines.index("Podatki proizvajalca")
            if idx + 1 < len(lines):
                proizvajalec = lines[idx + 1].strip()
        except Exception:
            pass

    # EAN (raw, brez validacije dolžine)
    ean = ""
    m_ean = re.search(r"(?:EAN|GTIN)\s*[:\-]?\s*([0-9]+)", full_text, flags=re.IGNORECASE)
    if m_ean:
        ean = m_ean.group(1).strip()

    # Cena + EM (iz sekcije "Vaša cena")
    preferred_unit = None
    # npr. "Moja površina" -> naslednja vrstica "m2"
    try:
        idx = lines.index("Moja površina")
        if idx + 1 < len(lines):
            preferred_unit = lines[idx + 1].strip()
    except Exception:
        pass

    price_lines = []
    try:
        idx_price = lines.index("Vaša cena")
        # zbiraj naslednjih n vrstic, dokler ne naletiš na "Preverite zalogo" ali "Količina" ipd.
        for j in range(idx_price + 1, min(idx_price + 12, len(lines))):
            if lines[j].lower().startswith("preverite zalogo"):
                break
            if lines[j].lower() in {"količina", "končna cena"}:
                break
            if "€" in lines[j]:
                price_lines.append(lines[j])
    except Exception:
        # fallback: najdi prve vrstice s "€"
        for ln in lines:
            if "€" in ln:
                price_lines.append(ln)
            if len(price_lines) >= 3:
                break

    price_with_vat, em = _extract_price_units_from_lines(price_lines, preferred_unit=preferred_unit)

    # Daljši opis: med "Opis" in naslednjo sekcijo
    opis_izdelek = ""
    try:
        idx_opis = lines.index("Opis")
        end_markers = {"Podatki proizvajalca", "Tehnične lastnosti", "Ocene", "Dokumenti"}
        buf = []
        for j in range(idx_opis + 1, len(lines)):
            if lines[j] in end_markers:
                break
            buf.append(lines[j])
        opis_izdelek = "\n".join(buf).strip()
    except Exception:
        opis_izdelek = ""

    # Slika (og:image)
    slika_url = ""
    meta_img = soup.find("meta", property="og:image")
    if meta_img and meta_img.get("content"):
        slika_url = meta_img["content"].strip()

    # Dobava iz product page (fallback, če nimamo po centrih):
    # iščemo "Dostava ... Na voljo/Ni na voljo" in "Prevzem ..."
    dobava_fallback = ""
    try:
        txt = full_text.lower()
        delivery_yes = "dostava" in txt and "ni na voljo" not in txt[txt.find("dostava"):txt.find("dostava") + 80]
        pickup_yes = "prevzem" in txt and "ni na voljo" not in txt[txt.find("prevzem"):txt.find("prevzem") + 80]
        if delivery_yes or pickup_yes:
            dobava_fallback = "DA"
        elif "dostava" in txt or "prevzem" in txt:
            dobava_fallback = "NE"
    except Exception:
        dobava_fallback = ""

    return {
        "title": title,
        "oznaka": oznaka,
        "proizvajalec": proizvajalec,
        "ean": ean,
        "em": em,
        "price_with_vat": price_with_vat,
        "opis_izdelek": opis_izdelek,
        "slika_url": slika_url,
        "dobava_fallback": dobava_fallback
    }


def has_next_page(soup):
    """Bolj robustno kot samo 'a.next'."""
    if soup.select_one("a.next"):
        return True
    if soup.find("a", rel="next"):
        return True
    if soup.find("link", rel="next"):
        return True
    # fallback: gumb "Naprej"
    if soup.find("a", string=re.compile(r"naprej", re.IGNORECASE)):
        return True
    return False


def build_paged_url(base_url, page_num):
    """OBI uporablja ?p=2; če URL že ima ?, dodaj &p=."""
    sep = "&" if "?" in base_url else "?"
    return f"{base_url}{sep}p={page_num}"


def get_product_details(url, skupina, date, listing_item_soup=None):
    """Pobere detajle z product page + (če obstaja) zalogo po centrih iz listing tile-a."""
    log_and_print(f"      - Detajli: {url}", to_file=True)

    listing_dobava = ""
    listing_stock_json = ""
    listing_stock_dict = None
    if listing_item_soup is not None:
        listing_dobava, listing_stock_json, listing_stock_dict = extract_stock_from_listing_item(listing_item_soup)

    html = get_page_content(url)
    if not html:
        log_and_print(f"        Preskakujem (ni HTML): {url}", to_file=True)
        return None

    parsed = parse_product_page_details(html)

    # Če nimamo nič pametnega, raje preskoči (ne ustavljaj run-a)
    if not parsed.get("title") and not parsed.get("price_with_vat"):
        log_and_print(f"        Preskakujem (manjka naslov/cena): {url}", to_file=True)
        return None

    data = {
        "Skupina": skupina,
        "Veljavnost od": date,
        "Valuta": "EUR",
        "DDV": "22",
        "URL": url,

        # standard polja
        "Oznaka / naziv": parsed.get("oznaka", ""),
        "EAN": parsed.get("ean", ""),  # RAW, brez validacije
        "Opis": parsed.get("title", ""),
        "Opis izdelka": parsed.get("opis_izdelek", ""),  # NOVO: daljši opis
        "Proizvajalec": parsed.get("proizvajalec", ""),
        "EM": normalize_em(parsed.get("em", "")),

        # Cene (vedno 2 dec)
        "Cena / EM (z DDV)": parsed.get("price_with_vat", ""),
        "Akcijska cena / EM (z DDV)": "",
        "Cena / EM (brez DDV)": convert_price_to_without_vat(parsed.get("price_with_vat", ""), DDV_RATE),
        "Akcijska cena / EM (brez DDV)": "",

        # Dobava + zaloga
        "Dobava": listing_dobava if listing_dobava else parsed.get("dobava_fallback", ""),
        "Zaloga po centrih": listing_stock_json if listing_stock_json else "",

        # Slika
        "SLIKA URL": parsed.get("slika_url", "")
    }

    # Če listing ni imel stock JSON, še vedno naj bo Dobava vsaj DA/NE, če je fallback dal
    if not data["Dobava"]:
        data["Dobava"] = parsed.get("dobava_fallback", "")

    return data


def save_json_checkpoint(json_path):
    """Checkpoint: zapiše _all_data_by_url v JSON (pretty, UTF-8)."""
    try:
        final_list = list(_all_data_by_url.values())
        # sort po Zap (če obstaja)
        try:
            final_list.sort(key=lambda x: int(x.get("Zap", 0)))
        except Exception:
            pass

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(final_list, f, ensure_ascii=False, indent=2)
        log_and_print("Checkpoint: JSON shranjen.", to_file=True)
    except Exception as e:
        log_and_print(f"Napaka pri shranjevanju JSON checkpointa: {e}", to_file=True)


def save_excel_final(excel_path):
    """Excel se naredi 1× na koncu (iz _all_data_by_url)."""
    final_list = list(_all_data_by_url.values())
    try:
        final_list.sort(key=lambda x: int(x.get("Zap", 0)))
    except Exception:
        pass

    df = pd.DataFrame(final_list)

    # opcijsko: razširi zalogo po centrih v stolpce "Zaloga - <center>"
    if EXPORT_STOCK_COLUMNS and "Zaloga po centrih" in df.columns:
        all_centers = set()
        parsed_stock = []
        for v in df["Zaloga po centrih"].fillna("").tolist():
            if not v:
                parsed_stock.append({})
                continue
            try:
                d = json.loads(v)
                if isinstance(d, dict):
                    parsed_stock.append(d)
                    all_centers.update(d.keys())
                else:
                    parsed_stock.append({})
            except Exception:
                parsed_stock.append({})

        all_centers = sorted(all_centers)
        for c in all_centers:
            col = f"Zaloga - {c}"
            df[col] = [d.get(c, "") for d in parsed_stock]

    cols = [
        "Skupina", "Zap", "Oznaka / naziv", "EAN", "Opis", "Opis izdelka", "EM", "Valuta", "DDV", "Proizvajalec",
        "Veljavnost od", "Dobava", "Zaloga po centrih",
        "Cena / EM (z DDV)", "Akcijska cena / EM (z DDV)",
        "Cena / EM (brez DDV)", "Akcijska cena / EM (brez DDV)",
        "URL", "SLIKA URL"
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = ""

    df = df[cols]

    # Zap reindex (varno): če hočeš ohraniti stare Zap-e, to zakomentiraj.
    # Jaz pustim originalne Zap-e (da resume ne premeša).
    df.to_excel(excel_path, index=False)
    log_and_print("Excel shranjen (final).", to_file=True)


def load_existing_json(json_path):
    """Naloži obstoječi JSON (za resume) in pripravi _existing_urls, _all_data_by_url, _global_item_counter."""
    global _global_item_counter, _existing_urls, _all_data_by_url
    if not os.path.exists(json_path):
        return

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            return

        by_url = {}
        max_zap = 0
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
        log_and_print(f"Resume: naloženih {len(_all_data_by_url)} izdelkov, Zap={_global_item_counter}.", to_file=True)
    except Exception as e:
        log_and_print(f"Napaka pri nalaganju obstoječega JSON: {e}", to_file=True)


def main():
    global _log_file, _global_item_counter

    # Naključen zamik za "mehko" obnašanje (brez anti-bot bypass)
    is_ci = os.environ.get("GITHUB_ACTIONS", "").lower() == "true"
    time.sleep(random.uniform(0, 2) if is_ci else random.randint(1, 10))

    json_path, excel_path, log_path = create_output_paths(SHOP_NAME)

    try:
        _log_file = open(log_path, "w", encoding="utf-8")
    except Exception as e:
        print(f"CRITICAL: ne morem odpreti log datoteke: {e}")
        return

    log_and_print(f"--- Zagon {SHOP_NAME} ---", to_file=True)

    # Resume (če JSON že obstaja)
    load_existing_json(json_path)

    date = datetime.now().strftime("%d/%m/%Y")
    newly_added_since_checkpoint = 0

    try:
        for cat, urls in OBI_CATEGORIES.items():
            log_and_print(f"\n--- Kategorija: {cat} ---", to_file=True)

            for base_cat_url in urls:
                sub_name = base_cat_url.strip("/").split("/")[-1]
                skupina = sub_name  # bolj granularno kot samo "cat"

                log_and_print(f"  Podkategorija: {skupina}", to_file=True)

                n = 1
                stariprvi = "star"

                while True:
                    p_url = build_paged_url(base_cat_url, n)
                    log_and_print(f"    Stran {n}: {p_url}", to_file=True)

                    html = get_page_content(p_url)
                    if not html:
                        break

                    soup = BeautifulSoup(html, "lxml")

                    # Poskusi najti product tile-e (več selectorjev, da je stabilneje)
                    items = []
                    for sel in [
                        "div.list-items.list-category-products div.item",
                        "div.list-items div.item",
                        "div.category-products div.item",
                        "div.products-grid div.item",
                        "li.product-item",
                    ]:
                        items = soup.select(sel)
                        if items:
                            break

                    if not items:
                        # Fallback: če ne najdemo tile-ov, ne crashaj - samo zaključi stran
                        log_and_print("    Ne najdem produktnih tile-ov (selectorji fail). Preskakujem to stran.", to_file=True)
                        break

                    # prepreči ponavljanje strani
                    def _first_title(it):
                        for tagname in ["h4", "h3", "h2"]:
                            t = it.find(tagname)
                            if t:
                                return t.get_text(strip=True)
                        return ""

                    noviprvi = _first_title(items[0])
                    if n > 1 and noviprvi and noviprvi == stariprvi:
                        log_and_print("    Stran se ponavlja. Konec podkategorije.", to_file=True)
                        break
                    if noviprvi:
                        stariprvi = noviprvi

                    for it in items:
                        # poišči link do produkta
                        a = it.select_one('a[href*="/p/"]')
                        if not a or not a.get("href"):
                            continue

                        url = urljoin(BASE_URL, a["href"])

                        if url in _existing_urls:
                            continue

                        # Detajli (product page + stock by centers iz listing tile-a)
                        details = get_product_details(url, skupina, date, listing_item_soup=it)
                        if not details:
                            # samo preskoči, ne ubijaj run-a
                            continue

                        # Zdaj šele dodelimo Zap (da ne delamo lukenj za preskočene)
                        _global_item_counter += 1
                        details["Zap"] = _global_item_counter

                        _all_data_by_url[url] = details
                        _existing_urls.add(url)
                        newly_added_since_checkpoint += 1

                        if newly_added_since_checkpoint >= CHECKPOINT_EVERY_N_ITEMS:
                            save_json_checkpoint(json_path)
                            newly_added_since_checkpoint = 0

                        # Sleep: CI hitreje, lokalno počasneje
                        if is_ci:
                            time.sleep(random.uniform(0.7, 2.5))
                        else:
                            time.sleep(random.uniform(2.0, 6.0))

                    if not has_next_page(soup):
                        break
                    n += 1

                # checkpoint po podkategoriji
                save_json_checkpoint(json_path)
                newly_added_since_checkpoint = 0

    except KeyboardInterrupt:
        log_and_print("Prekinjeno (KeyboardInterrupt). Shranjujem JSON...", to_file=True)
    except Exception as e:
        log_and_print(f"NAPAKA: {e}", to_file=True)
    finally:
        # final JSON
        save_json_checkpoint(json_path)
        # final Excel (1× na koncu)
        try:
            save_excel_final(excel_path)
        except Exception as e:
            log_and_print(f"Napaka pri končnem Excel exportu: {e}", to_file=True)

        log_and_print("--- KONEC ---", to_file=True)
        if _log_file:
            _log_file.close()


if __name__ == "__main__":
    main()
