import pandas as pd
import os
from datetime import datetime
import time
import random
import re
import json
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# --- Konfiguracija ---
SHOP_NAME = "Kalcer"
BASE_URL = "https://www.trgovina-kalcer.si"
DDV_RATE = 0.22

# Celoten seznam kategorij
KALCER_CATEGORIES = {
    'Gradnja': [
        'https://www.trgovina-kalcer.si/gradnja/izolacije/fasadni-izdelki/fasadne-izolacije',
        'https://www.trgovina-kalcer.si/gradnja/izolacije/fasadni-izdelki/fasadna-lepila-in-malte',
        'https://www.trgovina-kalcer.si/gradnja/izolacije/fasadni-izdelki/fasadne-barve-in-zakljucni-sloji',
        'https://www.trgovina-kalcer.si/gradnja/izolacije/fasadni-izdelki/fasadna-sidra',
        'https://www.trgovina-kalcer.si/gradnja/izolacije/fasadni-izdelki/fasadne-mrezice-in-profili',
        'https://www.trgovina-kalcer.si/gradnja/izolacije/fasadni-izdelki/fasadne-stukature',
        'https://www.trgovina-kalcer.si/gradnja/izolacije/toplotne-izolacije/steklena-izolacija',
        'https://www.trgovina-kalcer.si/gradnja/izolacije/toplotne-izolacije/kamena-izolacija',
        'https://www.trgovina-kalcer.si/gradnja/izolacije/toplotne-izolacije/izolacijske-plosce',
        'https://www.trgovina-kalcer.si/gradnja/izolacije/toplotne-izolacije/izolacijska-folija',
        'https://www.trgovina-kalcer.si/gradnja/izolacije/toplotne-izolacije/izolacijsko-nasutje',
        'https://www.trgovina-kalcer.si/gradnja/izolacije/folije-za-izolacijo',
        'https://www.trgovina-kalcer.si/gradnja/izolacije/izolacijski-lepilni-trakovi',
        'https://www.trgovina-kalcer.si/gradnja/izolacije/izolacijska-tesnila',
        'https://www.trgovina-kalcer.si/gradnja/izolacije/pozarni-izdelki-plosce',
        'https://www.trgovina-kalcer.si/gradnja/suhomontazni-sistemi/gradbene-plosce-gradnja',
        'https://www.trgovina-kalcer.si/gradnja/suhomontazni-sistemi/konstrukcija',
        'https://www.trgovina-kalcer.si/gradnja/suhomontazni-sistemi/pribor-za-suhi-estrih',
        'https://www.trgovina-kalcer.si/gradnja/suhomontazni-sistemi/suhi-estrihi',
        'https://www.trgovina-kalcer.si/gradnja/suhomontazni-sistemi/podlage-za-suhi-estrih',
        'https://www.trgovina-kalcer.si/gradnja/suhomontazni-sistemi/ogrevanje-hlajenje/talno-ogrevanje-hlajenje',
        'https://www.trgovina-kalcer.si/gradnja/suhomontazni-sistemi/ogrevanje-hlajenje/stensko-in-stropno-ogrevanje-hlajenje',
        'https://www.trgovina-kalcer.si/gradnja/pripomocki-suha-gradnja/svetila',
        'https://www.trgovina-kalcer.si/gradnja/pripomocki-suha-gradnja/pripomocki-pritrjevanje-suha-gradnja',
        'https://www.trgovina-kalcer.si/gradnja/pripomocki-suha-gradnja/fugiranje-armiranje/mase',
        'https://www.trgovina-kalcer.si/gradnja/pripomocki-suha-gradnja/fugiranje-armiranje/trakovi',
        'https://www.trgovina-kalcer.si/gradnja/pripomocki-suha-gradnja/fugiranje-armiranje/vogalniki',
        'https://www.trgovina-kalcer.si/gradnja/pripomocki-suha-gradnja/revizijske-odprtine',
        'https://www.trgovina-kalcer.si/gradnja/pripomocki-suha-gradnja/ciscenje',
        'https://www.trgovina-kalcer.si/gradnja/pripomocki-suha-gradnja/barvanje',
        'https://www.trgovina-kalcer.si/gradnja/lepljenje-tesnenje/hidroizolacije/stresne-folije',
        'https://www.trgovina-kalcer.si/gradnja/lepljenje-tesnenje/hidroizolacije/tekoce-brezsivne-folije',
        'https://www.trgovina-kalcer.si/gradnja/lepljenje-tesnenje/hidroizolacije/bitumenske-hidroizolacije',
        'https://www.trgovina-kalcer.si/gradnja/lepljenje-tesnenje/hidroizolacije/cementne-hidroizolacije',
        'https://www.trgovina-kalcer.si/gradnja/lepljenje-tesnenje/izravnalne-mase',
        'https://www.trgovina-kalcer.si/gradnja/lepljenje-tesnenje/radonska-zascita',
        'https://www.trgovina-kalcer.si/gradnja/lepljenje-tesnenje/tesnilne-mase',
        'https://www.trgovina-kalcer.si/gradnja/lepljenje-tesnenje/tesnilni-trakovi',
        'https://www.trgovina-kalcer.si/gradnja/lepljenje-tesnenje/lepila',
        'https://www.trgovina-kalcer.si/gradnja/gradbena-akustika/zvocni-absorberji',
        'https://www.trgovina-kalcer.si/gradnja/gradbena-akustika/zvocne-izolacije',
        'https://www.trgovina-kalcer.si/gradnja/gradbena-akustika/modularni-stropi',
        'https://www.trgovina-kalcer.si/gradnja/gradbena-akustika/akusticni-pribor'
    ]
}

# --- EM whitelist (če ni na seznamu -> kos) ---
_ALLOWED_EM = {
    "ar","ha","kam","kg","km","kwh","kw","wat","kpl","kos","kos dan","kos mes",
    "m","m2","m3","cm","kN","km2","kg/m3","kg/h","kg/l",
    "m/dan","m/h","m/min","m/s","m2 dan","m2 mes",
    "m3/dan","m3/h","m3/min","m3/s","m3 d",
    "t","tm","t/dan","t/h","t/let","h","min","s",
    "lit/dan","lit/h","lit/min","lit/s","L",
    "par","pal","sto","skl","del","ključ","os","os d","x","delež","oc","op"
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
]

# stabilen UA na zagon
_RUN_UA = random.choice(USER_AGENTS)

_log_file = None
_global_item_counter = 0

# session (cookie-friendly)
_session = requests.Session()


def log_and_print(message, to_file=True):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_message = f"[{timestamp}] {message}"
    print(full_message)
    if to_file and _log_file:
        try:
            _log_file.write(full_message + '\n')
            _log_file.flush()
        except:
            pass


def create_output_paths(shop_name):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_root = os.environ.get("OUTPUT_DIR") or script_dir

    today_date_folder = datetime.now().strftime("%Y-%m-%d")
    daily_dir = os.path.join(output_root, "Ceniki_Scraping", shop_name, today_date_folder)
    os.makedirs(daily_dir, exist_ok=True)

    filename_date = datetime.now().strftime("%d_%m_%Y")
    json_path = os.path.join(daily_dir, f"{shop_name}_Podatki_{filename_date}.json")
    excel_path = os.path.join(daily_dir, f"{shop_name}_Podatki_{filename_date}.xlsx")
    log_path = os.path.join(daily_dir, f"{shop_name}_Scraping_Log_{datetime.now().strftime('%H-%M-%S')}.txt")
    return json_path, excel_path, log_path


def _parse_float_any(price_str):
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
    except:
        return None


def fmt_2dec(val):
    if val is None:
        return ""
    return f"{val:.2f}".replace(".", ",")


def round_price_2dec(price_str):
    return fmt_2dec(_parse_float_any(price_str))


def convert_price_to_without_vat(price_str, vat_rate):
    v = _parse_float_any(price_str)
    if v is None:
        return ""
    return fmt_2dec(v / (1 + vat_rate))


def normalize_em(unit):
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


def get_page_content(url, referer=None, timeout=25, retries=3):
    headers = {
        "User-Agent": _RUN_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "sl-SI,sl;q=0.9,en-US;q=0.7,en;q=0.5",
        "Connection": "keep-alive",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
        "Referer": referer or BASE_URL,
    }

    for attempt in range(1, retries + 1):
        try:
            r = _session.get(url, headers=headers, timeout=timeout)

            # nežni backoff pri 429/5xx
            if r.status_code == 429:
                wait = random.uniform(30, 90)
                log_and_print(f"HTTP 429 @ {url} -> sleep {wait:.1f}s")
                time.sleep(wait)
                continue
            if r.status_code in (500, 502, 503, 504):
                wait = random.uniform(10, 45)
                log_and_print(f"HTTP {r.status_code} @ {url} -> sleep {wait:.1f}s")
                time.sleep(wait)
                continue

            r.raise_for_status()
            return r.text

        except Exception as e:
            wait = random.uniform(2, 8) * attempt
            log_and_print(f"Napaka pri dostopu {url}: {e} -> sleep {wait:.1f}s", to_file=True)
            time.sleep(wait)

    return None


def save_data(new_data, json_path, excel_path):
    if not new_data:
        return

    all_data = []
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                all_data = json.load(f)
        except:
            all_data = []
    elif os.path.exists(excel_path):
        try:
            df = pd.read_excel(excel_path)
            all_data = df.to_dict(orient='records')
        except:
            all_data = []

    # dedupe po URL + varianta
    def key(x):
        return f"{x.get('URL','')}|{x.get('Varianta','')}"
    data_dict = {key(item): item for item in all_data if isinstance(item, dict)}
    for item in new_data:
        data_dict[key(item)] = item

    final_list = list(data_dict.values())
    try:
        final_list.sort(key=lambda x: int(x.get('Zap', 0)))
    except:
        pass

    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(final_list, f, ensure_ascii=False, indent=2)
    log_and_print("Shranjen JSON.", to_file=True)

    df = pd.DataFrame(final_list)

    # dinamični centri (Zaloga - X)
    store_cols = sorted([c for c in df.columns if c.startswith("Zaloga - ")])

    cols = [
        "Skupina", "Zap", "Oznaka / naziv", "EAN", "Opis", "Opis izdelka", "Varianta",
        "EM", "Valuta", "DDV", "Proizvajalec", "Veljavnost od",
        "Dobava", "Zaloga po centrih",
        "Cena / EM (z DDV)", "Akcijska cena / EM (z DDV)",
        "Cena / EM (brez DDV)", "Akcijska cena / EM (brez DDV)",
        "URL", "SLIKA URL"
    ] + store_cols

    for c in cols:
        if c not in df.columns:
            df[c] = ""

    df[cols].to_excel(excel_path, index=False)
    log_and_print("Shranjen Excel.", to_file=True)


def get_product_links_from_category(category_url):
    all_links = []
    page = 1

    while True:
        sep = "&" if "?" in category_url else "?"
        url = f"{category_url}{sep}page={page}"

        log_and_print(f"  Stran {page}: {url}", to_file=True)
        html = get_page_content(url, referer=category_url)
        if not html:
            break

        soup = BeautifulSoup(html, 'html.parser')
        products = soup.select('.product-list > div, .product-grid .product')
        if not products:
            break

        for item in products:
            a = item.select_one('.name a')
            if a and a.get('href'):
                full = urljoin(BASE_URL, a['href'])
                all_links.append(full)

        text = soup.select_one('.pagination-results .text-right')
        if not text or "Prikazujem" not in text.get_text():
            break

        page += 1
        time.sleep(random.uniform(2.0, 5.0))

    # unique (ohrani vrstni red)
    return list(dict.fromkeys(all_links))


def extract_stock_centers(text):
    """
    Primer na Kalcer:
      Zaloga:
      Ljubljana: DA,  Maribor: DA,  Novo Mesto: DA
    """
    if not text:
        return {}
    # zajemi Ljubljana/Maribor/Novo Mesto (in morebitne druge)
    pairs = re.findall(r"([A-Za-zČŠŽčšž\.\-\s]+?)\s*:\s*(DA|NE)", text, flags=re.IGNORECASE)
    out = {}
    for city, val in pairs:
        city = re.sub(r"\s+", " ", city).strip().strip(",")
        v = val.strip().upper()
        if city and city.lower() != "zaloga":
            out[city] = v
    return out


def extract_long_description(soup):
    # Kalcer je tipično OpenCart: opis je pogosto v #tab-description
    desc = ""
    el = soup.select_one("#tab-description")
    if not el:
        # fallback: tab-content (pobere več, ampak je ok za klasifikacijo)
        el = soup.select_one(".tab-content")
    if el:
        desc = el.get_text("\n", strip=True)

    desc = (desc or "").strip()
    if len(desc) > 8000:
        desc = desc[:8000].rstrip() + "…"
    return desc


def parse_price_and_unit_from_priceblock(price_block_text):
    """
    Primer:
      42,46€ (4,25€/M2)
      od 39,44€ (4,21€/M2)
    Želimo:
      Cena = 4,25  EM=m2  (če oklepaj obstaja)
    """
    t = (price_block_text or "").replace("\xa0", " ").strip()

    # najprej poskusi enoto v oklepaju
    m = re.search(r"\(\s*([\d\.,]+)\s*€\s*/\s*([A-Za-z0-9²³]+)\s*\)", t)
    if m:
        p = round_price_2dec(m.group(1))
        u = m.group(2).strip()
        u = u.replace("M2", "m2").replace("M3", "m3").replace("M", "m")
        u = u.replace("m²", "m2").replace("m³", "m3").replace("²", "2").replace("³", "3")
        u = normalize_em(u)
        return p, u

    # fallback: prva cena v tekstu
    m2 = re.search(r"([\d\.,]+)\s*€", t)
    if m2:
        return round_price_2dec(m2.group(1)), "kos"

    return "", "kos"


def extract_sifra(text):
    m = re.search(r"Šifra:\s*([^\n\r]+)", text)
    return m.group(1).strip() if m else ""


def extract_ean(text):
    m = re.search(r"\bEAN\b\s*[:#]?\s*([0-9]{6,20})", text, flags=re.IGNORECASE)
    return m.group(1).strip() if m else ""


def extract_proizvajalec(text):
    # na nekaterih straneh: "Proizvajalec: SAINT- GOBAIN ..."
    m = re.search(r"Proizvajalec:\s*([^\n\r]+)", text)
    if not m:
        return ""
    val = m.group(1).strip()
    # občasno gre naprej do "Šifra:" – odreži
    val = re.split(r"\bŠifra:\b", val)[0].strip()
    return val


def extract_variants(soup):
    """
    Za Isover: "Debelina 50 mm, 80 mm"
    V HTML je lahko select ali radio; tukaj vzamemo najpreprosteje:
    - poiščemo select option liste
    - ali radio inpute
    """
    variants = []

    # select
    for sel in soup.select('select[name^="option["]'):
        label = ""
        fg = sel.find_parent(class_="form-group")
        if fg:
            lab = fg.select_one("label")
            if lab:
                label = lab.get_text(" ", strip=True)
        label = label or "Opcija"

        for opt in sel.select("option"):
            vid = (opt.get("value") or "").strip()
            txt = opt.get_text(" ", strip=True)
            if not vid or vid == "0":
                continue
            variants.append(f"{label}: {txt}")

        if variants:
            return variants  # 1 group je dovolj

    # radio
    radios = soup.select('input[type="radio"][name^="option["]')
    if radios:
        # label poskusi iz bližnjega "legend" ali podobno
        label = "Opcija"
        for r in radios:
            pl = r.find_parent("label")
            txt = pl.get_text(" ", strip=True) if pl else (r.get("value") or "").strip()
            if txt:
                variants.append(f"{label}: {txt}")
        if variants:
            return variants

    # fallback: če v tekstu piše "Debelina" in potem so vrednosti kot plain text
    txt = soup.get_text("\n", strip=True)
    if "Debelina" in txt:
        mm = re.findall(r"\b(\d{2,3}\s*mm)\b", txt)
        mm = list(dict.fromkeys([x.replace(" ", "") for x in mm]))
        if mm:
            return [f"Debelina: {x}" for x in mm]

    return []


def get_product_details(url, cat, date):
    global _global_item_counter

    log_and_print(f"    - Detajli: {url}", to_file=True)
    html = get_page_content(url, referer=url)
    if not html:
        return []

    soup = BeautifulSoup(html, 'html.parser')
    page_text = soup.get_text("\n", strip=True)

    base = {
        "Skupina": cat,
        "Zap": 0,
        "Veljavnost od": date,
        "Valuta": "EUR",
        "DDV": "22",
        "URL": url,
        "SLIKA URL": "",
        "Opis": "",
        "Opis izdelka": "",
        "Varianta": "",
        "Oznaka / naziv": "",
        "EAN": "",
        "EM": "kos",
        "Cena / EM (z DDV)": "",
        "Akcijska cena / EM (z DDV)": "",
        "Cena / EM (brez DDV)": "",
        "Akcijska cena / EM (brez DDV)": "",
        "Proizvajalec": "",
        "Dobava": "",
        "Zaloga po centrih": "",
    }

    # naziv
    h1 = soup.select_one('h1.product-name') or soup.select_one('h1.productInfo') or soup.select_one("h1")
    if h1:
        base['Opis'] = h1.get_text(" ", strip=True)

    # šifra (Kalcer jasno izpiše "Šifra: ...")
    base["Oznaka / naziv"] = extract_sifra(page_text)

    # proizvajalec
    base["Proizvajalec"] = extract_proizvajalec(page_text)

    # EAN (če obstaja)
    base["EAN"] = extract_ean(page_text)

    # opis izdelka (tab "Opis")
    base["Opis izdelka"] = extract_long_description(soup)

    # slika
    img = soup.select_one('a.lightbox-image')
    if img and img.get('href'):
        base['SLIKA URL'] = urljoin(BASE_URL, img['href'])

    # cena blok (na Kalcerju je pogosto v tekstu ob naslovu)
    # poskusimo najti elemente, ki vsebujejo "€"
    price_el = soup.select_one("span.productSpecialPrice") \
               or soup.select_one(".price-new") \
               or soup.select_one(".price") \
               or soup.find(string=re.compile("€"))

    price_text = ""
    if price_el:
        price_text = price_el.get_text(" ", strip=True) if hasattr(price_el, "get_text") else str(price_el)

    # fallback: vzemi del strani okoli "Brez DDV:" / "Redna cena"
    if not price_text and "Brez DDV" in page_text:
        # izlušči nekaj vrstic okoli
        price_text = page_text

    price, unit = parse_price_and_unit_from_priceblock(price_text)
    base["Cena / EM (z DDV)"] = price
    base["EM"] = normalize_em(unit)
    base["Cena / EM (brez DDV)"] = convert_price_to_without_vat(price, DDV_RATE)

    # zaloga
    if "Za prikaz zaloge izberite možnosti" in page_text:
        # variacije bodo, stock brez JS ne moremo 100% prebrat
        base["Dobava"] = ""
    else:
        # poskusi pobrat center status iz tekstovnega bloka "Zaloga:"
        stock_dict = extract_stock_centers(page_text)
        if stock_dict:
            base["Zaloga po centrih"] = json.dumps(stock_dict, ensure_ascii=False)
            any_da = any(v.upper() == "DA" for v in stock_dict.values())
            base["Dobava"] = "DA" if any_da else "NE"
            for k, v in stock_dict.items():
                base[f"Zaloga - {k}"] = v

    # variacije
    variants = extract_variants(soup)
    if variants:
        out = []
        for var in variants:
            _global_item_counter += 1
            d = dict(base)
            d["Zap"] = _global_item_counter
            d["Varianta"] = var
            out.append(d)
        return out

    # brez variacij -> 1 zapis
    _global_item_counter += 1
    base["Zap"] = _global_item_counter
    return [base]


def main():
    global _log_file, _global_item_counter

    time.sleep(random.uniform(0.5, 3.0) if os.environ.get('GITHUB_ACTIONS') else random.uniform(1.0, 10.0))

    json_path, excel_path, log_path = create_output_paths(SHOP_NAME)
    try:
        _log_file = open(log_path, 'w', encoding='utf-8')
    except:
        return

    log_and_print(f"--- Zagon {SHOP_NAME} ---", to_file=True)
    log_and_print(f"UA: {_RUN_UA}", to_file=True)

    # warm-up (cookie/session)
    try:
        get_page_content(BASE_URL, referer=BASE_URL, retries=1)
    except:
        pass

    # nadaljuj Zap, če JSON že obstaja
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                d = json.load(f)
            if d:
                _global_item_counter = max((int(x.get('Zap', 0)) for x in d if isinstance(x, dict)), default=0)
        except:
            pass

    date = datetime.now().strftime("%d/%m/%Y")
    buffer = []
    FLUSH_EVERY = 30

    try:
        for cat, urls in KALCER_CATEGORIES.items():
            log_and_print(f"\n--- {cat} ---", to_file=True)

            for u in urls:
                sub_name = u.split('/')[-1]
                links = get_product_links_from_category(u)

                for link in links:
                    recs = get_product_details(link, sub_name, date)
                    if recs:
                        buffer.extend(recs)

                    if len(buffer) >= FLUSH_EVERY:
                        save_data(buffer, json_path, excel_path)
                        buffer = []

                    time.sleep(random.uniform(2.0, 5.0))

                if buffer:
                    save_data(buffer, json_path, excel_path)
                    buffer = []

    except Exception as e:
        log_and_print(f"NAPAKA: {e}", to_file=True)

    finally:
        if buffer:
            save_data(buffer, json_path, excel_path)
        if _log_file:
            _log_file.close()


if __name__ == "__main__":
    main()
