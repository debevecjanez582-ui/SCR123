# SCR123 – Scraperji cenikov (OBI, Kalcer, Merkur, Tehnoles, Slovenijales, Zagožen)

Repo vsebuje Python skripte, ki poberejo artikle in izvozijo:
- JSON (`<SHOP>_Podatki_<DD_MM_YYYY>.json`)
- Excel (`<SHOP>_Podatki_<DD_MM_YYYY>.xlsx`)
- Log (`<SHOP>_Scraping_Log_<HH-MM-SS>.txt`)

Izhodna struktura (lokalno ali na GitHub Actions):
`OUTPUT_DIR/Ceniki_Scraping/<SHOP>/<YYYY-MM-DD>/...`

Na GitHub Actions je `OUTPUT_DIR=artifacts` in se nato vse naloži kot *artifact*.

---

## Lokalni zagon (test danes)

### 1) Priprava okolja
```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Zagon ene trgovine
```bash
python ObiV1_github_fixed_v4.py
```

### 3) Zagon vseh (zaporedno)
```bash
python run_all.py
```

Ali samo izbrane:
```bash
python run_all.py --shops OBI Merkur
```

---

## GitHub Actions (1x na mesec + ročno)

Workflow: `.github/workflows/scrape_monthly.yml`

- `schedule`: 1. v mesecu (cron v UTC)
- `workflow_dispatch`: ročni zagon v Actions → Run workflow

Rezultate najdeš v:
Actions → izbereš run → sekcija **Artifacts**

---

## Nastavitve (env)
Vse skripte podpirajo:
- `OUTPUT_DIR` (privzeto: mapa skripte)
- `SCRAPE_SLEEP_MIN`, `SCRAPE_SLEEP_MAX` (privzeto ~4–6s)
- `BUFFER_FLUSH` (koliko zapisov v JSON naenkrat)
- `MAX_PAGES` (varovalo proti neskončnim zankam)

