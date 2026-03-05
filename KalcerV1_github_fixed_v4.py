import pandas as pd
import os
import sys
from datetime import datetime
import time
import random
import re
import json
from urllib.parse import urljoin, urlparse, urlencode, parse_qs, urlunparse

import requests
from bs4 import BeautifulSoup

# --- Konfiguracija ---
SHOP_NAME = "Kalcer"
BASE_URL = "https://www.trgovina-kalcer.si"
DDV_RATE = 0.22

# Celoten seznam kategorij iz vaše datoteke
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

# --- Globalno ---
_log_file = None
_global_item_counter = 0

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
]

_session = requests.Session()
_session.headers.update(
    {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "sl-SI,sl;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
    }
)

# EM whitelist (če ni v tem seznamu -> "kos")
EM_WHITELIST = {
    "kos",
    "m",
    "m2",
    "m3",
    "kg",
    "l",
    "t",
    "cm",
    "mm",
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
    "KOS": "kos",
    "M2": "m2",
    "M3": "m3",
    "KG": "kg",
    "M": "m",
}

PRICE_PER_UNIT_RE = re.compile(
    r"(\d{1,3}(?:\.\d{3})*,\d{2})\s*€\s*/\s*([A-Za-z0-9²³]+)",
    flags=re.IGNORECASE,
)
PRICE_SIMPLE_RE = re.compile(
    r"(?:od\s*)?(\d{1,3}(?:\.\d{3})*,\d{2})\s*€",
    flags=re.IGNORECASE,
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
    if not em_raw:
        return "kos"
    em = em_raw.strip()
    em = EM_ALIASES.get(em, em)
    em = em.lower()
    em = EM_ALIASES.get(em, em)
    em = em.replace("/", "").strip()
    em = re.sub(r"[^a-z0-9²³^]", "", em)
    em = EM_ALIASES.get(em, em)
    em = re.sub(r"[^a-z0-9]", "", em)
    return em if em in EM_WHITELIST else "kos"


def parse_price_eur(price_raw):
    """
    '1.234,56' -> 1234.56
    '39,44'    -> 39.44
    """
    if not price_raw:
        return None
    s = str(price_raw).strip().replace(".", "").replace(",", ".")
    try:
        return round(float(s), 2)
    except Exception:
        return None


def get_page_content(url):
    """
    Brez bypass zaščit.
    Če naletimo na 403/429/503, samo logamo in preskočimo (ne ustavimo runa).
    """
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    try:
        resp = _session.get(url, headers=headers, timeout=25, allow_redirects=True)
        status = resp.status_code

        if status in (403, 429, 503):
            log_and_print(f"⚠️ Možen challenge ({status}) za URL: {url} -> preskakujem", to_file=True)
            return None

        if status >= 400:
            log_and_print(f"HTTP {status} za URL: {url} -> preskakujem", to_file=True)
            return None

        return resp.text
    except requests.exceptions.RequestException as e:
        log_and_print(f"Napaka pri dostopu do URL-ja {url}: {e}", to_file=True)
        return None


def _with_page_param(category_url, page_num):
    """
    Kalcer/OpenCart tipično uporablja query param `page`.
    Ta funkcija varno doda/posodobi page param (ne glede na to, ali URL že ima query).
    """
    parts = urlparse(category_url)
    qs = parse_qs(parts.query)
    qs["page"] = [str(page_num)]
    new_query = urlencode(qs, doseq=True)
    return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, new_query, parts.fragment))


def get_product_links_from_category(category_url):
    """
    Pobere produktne linke iz kategorije.
    Minimalno robustno (ne agresivno): poskusi več selectorjev, potem filtrira na product URL-je.
    """
    all_links = []
    page = 1
    first_link_prev = None

    while True:
        url = _with_page_param(category_url, page)
        log_and_print(f"  Stran {page}: {url}", to_file=True)

        html = get_page_content(url)
        if not html:
            break

        soup = BeautifulSoup(html, "html.parser")

        # Primarni selectorji (OpenCart variacije)
        anchors = []
        for sel in [
            ".product-thumb h4 a",
            ".product-thumb .name a",
            ".product-grid .name a",
            ".product-list .name a",
            ".product-layout h4 a",
            ".product-layout .name a",
            ".name a",
        ]:
            anchors.extend(soup.select(sel))

        # Fallback: vsi a tagi z href (če primarni selectorji ne najdejo nič)
        if not anchors:
            anchors = soup.select("a[href]")

        links_on_page = []
        for a in anchors:
            href = a.get("href")
            if not href:
                continue
            abs_url = urljoin(BASE_URL, href)

            # filtriranje: produkti so tipično na rootu: https://www.trgovina-kalcer.si/<slug>
            # kategorije so /gradnja/... in ostale navigacije.
            p = urlparse(abs_url).path or ""
            if not p or p == "/":
                continue
            if p.startswith("/gradnja/"):
                continue
            if p.startswith("/index.php"):
                # včasih so produkti tudi preko route=product/product
                if "route=product/product" not in abs_url:
                    continue
            if p.startswith("/m-") or p.startswith("/module/"):
                continue
            if p.lower().endswith((".pdf", ".jpg", ".png", ".webp")):
                continue
            if "#" in abs_url:
                continue

            # osnovna zaščita pred “neprodukti”
            if any(x in abs_url.lower() for x in ["kontakt", "prijava", "wishlist", "compare", "newsletter"]):
                continue

            links_on_page.append(abs_url)

        # Če nismo našli nič produktov, zaključimo
        links_on_page = list(dict.fromkeys(links_on_page))  # dedup, ohrani vrstni red
        if not links_on_page:
            break

        # varovalka proti ponavljanju strani
        first_link = links_on_page[0]
        if page > 1 and first_link_prev and first_link == first_link_prev:
            log_and_print(f"  Stran {page} se ponavlja -> zaključujem kategorijo.", to_file=True)
            break
        first_link_prev = first_link

        all_links.extend(links_on_page)

        # Če stran v tekstu kaže, da je samo 1 stran, lahko zaključimo (ne obvezno, ampak hitreje)
        page_text = soup.get_text(" ", strip=True)
        if re.search(r"\(\s*1\s*strani\s*\)", page_text, flags=re.IGNORECASE):
            break

        page += 1
        time.sleep(random.uniform(1.0, 3.5) if os.environ.get("GITHUB_ACTIONS") else random.uniform(2.0, 6.0))

    # dedup globalno
    return list(dict.fromkeys(all_links))


def extract_sifra(page_text):
    # "Šifra: 14610" ali "Šifra: ISOVER PIANO 037-V"
    m = re.search(r"\bŠifra:\s*([^\n\r]+)", page_text, flags=re.IGNORECASE)
    return m.group(1).strip() if m else ""


def extract_manufacturer(page_text):
    # "Proizvajalec: SAINT- GOBAIN ..."
    m = re.search(r"\bProizvajalec:\s*([^\n\r]+)", page_text, flags=re.IGNORECASE)
    return m.group(1).strip() if m else ""


def extract_ean_raw(page_text):
    # EAN raw, brez validacije dolžine
    m = re.search(r"\bEAN\b\s*[:\-]?\s*([^\n\r]+)", page_text, flags=re.IGNORECASE)
    return m.group(1).strip() if m else ""


def extract_price_and_em(page_text, em_fallback="kos"):
    """
    Kalcer pogosto prikazuje: "42,46€ (4,25€/M2)".
    Pravilo: če obstaja cena na enoto (€/M2, €/KG, ...), jo preferiramo.
    """
    # 1) prefer per-unit
    m = PRICE_PER_UNIT_RE.search(page_text or "")
    if m:
        price = parse_price_eur(m.group(1))
        em = normalize_em(m.group(2))
        if price is not None:
            return price, em

    # 2) fallback: prva “glavna” cena (od xx,xx€ ali xx,xx€)
    m2 = PRICE_SIMPLE_RE.search(page_text or "")
    if m2:
        price = parse_price_eur(m2.group(1))
        if price is not None:
            return price, normalize_em(em_fallback)

    return None, normalize_em(em_fallback)


def extract_long_description(soup):
    """
    Opis izdelka (daljši opis) - poskusimo iz tab-a "Opis".
    OpenCart tipično: div#tab-description
    """
    for sel in ["#tab-description", "div#tab-description", ".tab-content #tab-description"]:
        el = soup.select_one(sel)
        if el:
            txt = el.get_text("\n", strip=True)
            return txt

    # fallback: pogosto je opis v div.product-description ali .description (odvisno od teme)
    for sel in [".product-description", ".product-info .description", ".description"]:
        el = soup.select_one(sel)
        if el:
            txt = el.get_text("\n", strip=True)
            # preveč kratko? še vedno vrnemo (bolje kot nič)
            return txt

    return ""


def extract_stock_by_centers(page_text):
    """
    Kalcer primer: "Zaloga:\nLjubljana: DA,  Maribor: DA,  Novo Mesto: DA"
    Vrne dict: {"Ljubljana": "DA", "Maribor": "DA", ...}
    """
    if not page_text:
        return {}

    m = re.search(r"\bZaloga:\s*(?:\n\s*)?([^\n\r]+)", page_text, flags=re.IGNORECASE)
    if not m:
        return {}

    raw = m.group(1).strip()
    if not raw:
        return {}

    # primer: "Ljubljana: DA,  Maribor: DA,  Novo Mesto: DA"
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    stock = {}
    for p in parts:
        if ":" in p:
            k, v = p.split(":", 1)
            stock[k.strip()] = v.strip()
        else:
            # če ni center: "Zaloga: DA"
            stock["Zaloga"] = p.strip()

    return stock


def compute_dobava(stock_dict, page_text):
    """
    Dobava:
      - če imamo center dict: DA če vsaj en center ima DA ali pozitivno število
      - sicer:
        - če eksplicitno vsebuje "Ni na zalogi" -> NE
        - če vsebuje "Zaloga:" in "DA" -> DA
        - če "Za prikaz zaloge izberite možnosti" -> "" (ne vemo)
        - fallback: ""
    """
    if stock_dict:
        for v in stock_dict.values():
            vv = str(v).strip().upper()
            if "DA" == vv or vv.startswith("DA"):
                return "DA"
            # če je številka (npr. zaloga=12)
            num = re.search(r"(\d+)", vv)
            if num and int(num.group(1)) > 0:
                return "DA"
        return "NE"

    t = (page_text or "").lower()
    if "za prikaz zaloge izberite možnosti" in t:
        return ""
    if "ni na zalogi" in t:
        return "NE"
    if "zaloga" in t and " da" in t:
        return "DA"

    return ""


def _get_best_image_url(soup):
    meta = soup.find("meta", property="og:image")
    if meta and meta.get("content"):
        return urljoin(BASE_URL, meta["content"].strip())

    a = soup.select_one("a.lightbox-image[href]")
    if a and a.get("href"):
        return urljoin(BASE_URL, a["href"].strip())

    img = soup.select_one(".thumbnails img, .image img, img")
    if img and (img.get("src") or img.get("data-src")):
        return urljoin(BASE_URL, (img.get("src") or img.get("data-src")).strip())

    return ""


def get_product_details(url, cat, query_date):
    """
    Pridobi detajle izdelka.
    Upošteva pravila:
      - Cena: float, round 2
      - EM whitelist, sicer kos
      - EAN raw (če obstaja)
      - Oznaka/naziv = Šifra
      - Opis izdelka (daljši opis)
      - Dobava + zaloga po centrih (kjer obstaja)
    """
    global _global_item_counter

    log_and_print(f"    - Detajli: {url}", to_file=True)
    html = get_page_content(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text("\n")

    _global_item_counter += 1

    # kratki naslov
    h1 = soup.select_one("h1.product-name") or soup.select_one("h1.productInfo") or soup.find("h1")
    title = h1.get_text(strip=True) if h1 else ""

    # šifra = oznaka/naziv
    sifra = extract_sifra(page_text)

    # EM fallback iz tabele (če obstaja)
    em_from_table = ""
    rows = soup.select(".listing.stockMargin tr")
    for row in rows:
        cells = row.select("td")
        if len(cells) == 2:
            k = cells[0].get_text(strip=True)
            v = cells[1].get_text(strip=True)
            if "Enota mere" in k:
                em_from_table = v

    # cena + EM
    price_with_vat, em = extract_price_and_em(page_text, em_fallback=em_from_table or "kos")
    if price_with_vat is None:
        # brez cene => preskoči
        log_and_print(f"      Preskakujem (ni cene): {url}", to_file=True)
        return None

    # DDV -> brez DDV
    price_without_vat = round(float(price_with_vat) / (1 + DDV_RATE), 2)

    # EAN raw
    ean_raw = extract_ean_raw(page_text)

    # proizvajalec
    manufacturer = extract_manufacturer(page_text)

    # daljši opis
    long_desc = extract_long_description(soup)

    # zaloga po centrih
    stock_dict = extract_stock_by_centers(page_text)
    dobava = compute_dobava(stock_dict, page_text)
    stock_json = json.dumps(stock_dict, ensure_ascii=False) if stock_dict else ""

    # slika
    img_url = _get_best_image_url(soup)

    data = {
        "Skupina": cat,
        "Zap": _global_item_counter,
        "Oznaka / naziv": sifra,  # čist šifra brez prefixov
        "EAN": ean_raw,  # raw (brez validacije)
        "Opis": title,
        "Opis izdelka": long_desc,
        "EM": normalize_em(em),
        "Valuta": "EUR",
        "DDV": "22",
        "Proizvajalec": manufacturer,
        "Veljavnost od": query_date,
        "Dobava": dobava,
        "Zaloga po centrih": stock_json,
        "Cena / EM (z DDV)": round(float(price_with_vat), 2),
        "Akcijska cena / EM (z DDV)": "",
        "Cena / EM (brez DDV)": price_without_vat,
        "Akcijska cena / EM (brez DDV)": "",
        "URL": url,
        "SLIKA URL": img_url,
    }

    return data


def _merge_by_url(existing_list, new_items):
    by_url = {x.get("URL"): x for x in existing_list if x.get("URL")}
    for it in new_items:
        u = it.get("URL")
        if u:
            by_url[u] = it
    merged = list(by_url.values())
    # poskusi sort po Zap (če gre)
    try:
        merged.sort(key=lambda x: int(str(x.get("Zap") or 0).split(".")[0]))
    except Exception:
        pass
    return merged


def save_json_checkpoint(all_data, json_path):
    try:
        _atomic_write_json(all_data if all_data else [], json_path)
        log_and_print("JSON checkpoint shranjen.", to_file=True)
    except Exception as e:
        log_and_print(f"Napaka pri shranjevanju JSON: {e}", to_file=True)


def _parse_center_stock_json(s):
    if not s or not isinstance(s, str):
        return {}
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def save_to_excel(all_data, excel_path):
    """
    Excel naredimo 1× na koncu (in v finally).
    Opcijsko razširimo "Zaloga po centrih" v stolpce 'Zaloga - <center>'.
    """
    if not all_data:
        log_and_print("Ni podatkov za Excel.", to_file=True)
        return

    try:
        df = pd.DataFrame(all_data)

        if "URL" in df.columns:
            df.drop_duplicates(subset=["URL"], keep="last", inplace=True)

        # dinamični stolpci za centre
        if "Zaloga po centrih" in df.columns:
            center_dicts = df["Zaloga po centrih"].apply(_parse_center_stock_json)
            all_centers = sorted(set().union(*center_dicts.tolist())) if len(center_dicts) else []
            for center in all_centers:
                col = f"Zaloga - {center}"
                df[col] = center_dicts.apply(lambda d: d.get(center, "") if isinstance(d, dict) else "")

        desired_cols = [
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
            "Zaloga po centrih",
            "Cena / EM (z DDV)",
            "Akcijska cena / EM (z DDV)",
            "Cena / EM (brez DDV)",
            "Akcijska cena / EM (brez DDV)",
            "URL",
            "SLIKA URL",
        ]

        # dodaj dinamične zaloge
        dynamic_stock_cols = [c for c in df.columns if isinstance(c, str) and c.startswith("Zaloga - ")]
        final_cols = desired_cols + [c for c in dynamic_stock_cols if c not in desired_cols]

        for c in final_cols:
            if c not in df.columns:
                df[c] = ""

        # stabilno: Zap reset 1..N
        if "Zap" in df.columns:
            df["Zap"] = pd.to_numeric(df["Zap"], errors="coerce")
            df.sort_values(by=["Zap"], inplace=True, na_position="last")
            df.reset_index(drop=True, inplace=True)
            df["Zap"] = df.index + 1

        df[final_cols].to_excel(excel_path, index=False)
        log_and_print("Excel shranjen.", to_file=True)
    except Exception as e:
        log_and_print(f"Napaka pri shranjevanju Excel: {e}", to_file=True)


def main():
    global _log_file, _global_item_counter

    # majhen random delay (CI manj)
    time.sleep(random.uniform(0.0, 2.0) if os.environ.get("GITHUB_ACTIONS") else random.uniform(1.0, 8.0))

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

    all_data = []

    # resume: JSON je primaren (ker checkpointamo)
    if os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, list):
                all_data = loaded
            # nastavi števec
            zaps = pd.to_numeric(pd.Series([x.get("Zap") for x in all_data]), errors="coerce").dropna()
            if not zaps.empty:
                _global_item_counter = int(zaps.max())
            log_and_print(f"Naložen obstoječ JSON. Zap = {_global_item_counter}", to_file=True)
        except Exception as e:
            log_and_print(f"Napaka pri branju JSON: {e}. Začenjam na novo.", to_file=True)
            all_data = []

    query_date = datetime.now().strftime("%Y-%m-%d")
    buffer = []

    try:
        existing_urls = {d.get("URL") for d in all_data if d.get("URL")}

        for cat_name, urls in KALCER_CATEGORIES.items():
            log_and_print(f"\n--- {cat_name} ---", to_file=True)

            for cat_url in urls:
                sub_name = (urlparse(cat_url).path or "").strip("/").split("/")[-1] or cat_name
                log_and_print(f"\n  Kategorija: {sub_name}", to_file=True)

                links = get_product_links_from_category(cat_url)
                if not links:
                    log_and_print("  (brez linkov / ni produktov)", to_file=True)
                    continue

                for link in links:
                    if link in existing_urls:
                        continue

                    det = get_product_details(link, sub_name, query_date)
                    if det:
                        buffer.append(det)
                        existing_urls.add(link)

                    # JSON checkpoint batch (na 5)
                    if len(buffer) >= 5:
                        all_data = _merge_by_url(all_data, buffer)
                        save_json_checkpoint(all_data, json_path)
                        buffer = []

                    # throttling
                    is_ci = os.environ.get("GITHUB_ACTIONS", "").lower() == "true"
                    time.sleep(random.uniform(0.6, 2.2) if is_ci else random.uniform(2.0, 5.5))

                # flush po kategoriji
                if buffer:
                    all_data = _merge_by_url(all_data, buffer)
                    save_json_checkpoint(all_data, json_path)
                    buffer = []

    except Exception as e:
        log_and_print(f"NAPAKA: {e}", to_file=True)
    finally:
        # zadnji flush
        if buffer:
            all_data = _merge_by_url(all_data, buffer)
            save_json_checkpoint(all_data, json_path)
            buffer = []

        # Excel 1× na koncu
        save_to_excel(all_data, excel_path)

        if _log_file:
            _log_file.close()


if __name__ == "__main__":
    main()
