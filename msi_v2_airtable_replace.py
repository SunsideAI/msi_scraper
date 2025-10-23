# msi_v2_airtable_replace.py
# -*- coding: utf-8 -*-
import sys, time, csv, os, json, requests, re
from urllib.parse import quote, urljoin
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Konfiguration / ENV
# ---------------------------------------------------------------------------
# Zielseite: https://www.msi-hessen.de/kaufen/immobilienangebote/
BASE = "https://www.msi-hessen.de"
HEADERS = {"User-Agent": "Mozilla/5.0"}

# GPT-Klassifikation optional (nur für Kauf-Unterkategorie-Fallback)
USE_GPT_CLASSIFY = True

# Airtable
AIRTABLE_TOKEN    = os.getenv("AIRTABLE_TOKEN", "").strip()
AIRTABLE_BASE     = os.getenv("AIRTABLE_BASE",  "").strip()      # app...
AIRTABLE_TABLE    = os.getenv("AIRTABLE_TABLE", "").strip()      # optional (Name)
AIRTABLE_TABLE_ID = os.getenv("AIRTABLE_TABLE_ID", "").strip()   # bevorzugt (tbl...)
AIRTABLE_VIEW     = os.getenv("AIRTABLE_VIEW", "").strip()       # optional

# OpenAI (nur optional für Kauf-Unterkategorie)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()

# ---------------------------------------------------------------------------
# HTTP & HTML
# ---------------------------------------------------------------------------
def soup_get(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def get_list_page_urls(mode: str, max_pages: int = 50):
    """
    Gleiche Signatur wie im Werneburg-Script.
    MSI nutzt /page/{n} Paginierung. 'miete' wird sauber behandelt (keine Seiten).
    """
    urls = []
    if mode == "kauf":
        first = f"{BASE}/kaufen/immobilienangebote/"
        pattern = f"{BASE}/kaufen/immobilienangebote/page/{{n}}/"
        for page in range(1, max_pages + 1):
            urls.append(first if page == 1 else pattern.format(n=page))
    else:
        # (optional) Wenn du später Mieten anbinden willst, hier anpassen:
        # first = f"{BASE}/mieten/..." ; pattern = f"{BASE}/mieten/.../page/{{n}}/"
        # Bis dahin liefern wir keine Seiten zurück.
        pass
    return urls

def collect_detail_links(list_url: str):
    """
    Gleiche API wie im Werneburg-Script.
    MSI-Listen zeigen Detailseiten unter /angebote/... (z.B. /angebote/efh-xyz/)
    """
    soup = soup_get(list_url)
    links = set()

    for a in soup.select('a[href*="/angebote/"]'):
        href = a.get("href")
        if not href:
            continue
        if not href.startswith("http"):
            href = urljoin(BASE, href)
        links.add(href)

    return list(links)

# ---------------------------------------------------------------------------
# Detail-Parser
# ---------------------------------------------------------------------------
RE_PRICE_EUR = re.compile(r"\d{1,3}(?:\.\d{3})*(?:,\d{2})?\s*€")
RE_OBJEKTNR  = re.compile(r"Objekt[-\s]?Nr\.?:\s*([A-Za-z0-9\-_/]+)")
RE_PLZ_ORT   = re.compile(r"\b([0-9]{5}\s+[A-Za-zÄÖÜäöüß\-\s]+)\b")

def parse_detail(detail_url: str, mode: str):
    """
    Signatur identisch zum Werneburg-Parser (detail_url, mode) – 'mode' wird nicht benötigt,
    ist aber für API-Gleichheit dabei.
    """
    soup = soup_get(detail_url)
    page_text = soup.get_text("\n", strip=True)

    # Titel: erstes <h1>
    h1 = soup.select_one("h1")
    title = h1.get_text(strip=True) if h1 else ""

    # Beschreibung: erste sinnvollen Absätze nach H1 (Abbruch bei Formular/Sidebar)
    description = ""
    if h1:
        parts, count = [], 0
        for sib in h1.find_all_next():
            if sib.name == "p":
                t = sib.get_text(" ", strip=True)
                if not t:
                    continue
                # Stopper (Kontakt-/Formular-/Sidebartexte vermeiden)
                if any(stop in t for stop in ("Ihre Anfrage", "Neueste Immobilien", "Kontakt", "Exposé anfordern")):
                    break
                parts.append(t)
                count += 1
                if count >= 10:
                    break
        description = "\n\n".join(parts).strip()

    # Objektnummer (z.B. "Objekt-Nr.: 4220")
    m_obj = RE_OBJEKTNR.search(page_text)
    objektnummer = m_obj.group(1).strip() if m_obj else ""

    # Preis: erster Euro-Betrag
    m_price = RE_PRICE_EUR.search(page_text)
    preis_value = m_price.group(0) if m_price else ""

    # Ort: versuche PLZ + Ort aus Headline/Intro zu extrahieren
    # Oft steht "… zum Kauf 34626 Neukirchen" bzw. im Body taucht die PLZ/Ort-Kombi einmal eindeutig auf.
    m_ort = RE_PLZ_ORT.search(page_text)
    ort = m_ort.group(1).strip() if m_ort else ""

    # Bild: bevorzugt Link zur externen Galerie (z.B. immo.screenwork.de), sonst erstes <img>
    image_url = ""
    a_img = soup.select_one('a[href*="immo."]') or soup.select_one('a[href*="screenwork"]')
    if a_img and a_img.has_attr("href"):
        image_url = a_img["href"]
    else:
        img = soup.find("img")
        if img and img.has_attr("src"):
            image_url = urljoin(BASE, img["src"])

    return {
        "Titel":        title,
        "URL":          detail_url,
        "Description":  description,
        "Objektnummer": objektnummer,
        "Preis":        preis_value,
        "Ort":          ort,
        "Bild_URL":     image_url,
    }

# ---------------------------------------------------------------------------
# Heuristiken/Klassifikation
# ---------------------------------------------------------------------------
KEYS_WOHNUNG    = ["wohnung", "etagenwohnung", "eigentumswohnung", "apartment", "dachgeschoss", "maisonette", "penthouse", "balkon"]
KEYS_HAUS       = ["haus", "einfamilienhaus", "zweifamilienhaus", "reihenhaus", "doppelhaushälfte", "stadtvilla", "mehrfamilienhaus", "dhh"]
KEYS_GEWERBE    = ["gewerbe", "büro", "laden", "praxis", "lager", "halle", "gastronomie", "gewerbeeinheit", "gewerbefläche"]
KEYS_KAPITAL    = ["kapitalanlage", "rendite", "vermietet", "anlageobjekt", "investment"]
KEYS_STELLPLATZ = ["stellplatz", "parkplatz", "tiefgarage", "garage", "carport"]

def heuristic_subcategory(row, mode):
    """
    Gleiche API wie im Werneburg-Script.
    Bei MSI scrapen wir aktuell 'kauf'; Logik bleibt identisch & robust.
    """
    text = f"{row.get('Titel','')} {row.get('Description','')}".lower()

    if mode == "miete":
        if any(k in text for k in KEYS_STELLPLATZ): return "Stellplatz"
        if any(k in text for k in KEYS_GEWERBE):    return "Gewerbe"
        if any(k in text for k in KEYS_HAUS):       return "Haus"
        if any(k in text for k in KEYS_WOHNUNG):    return "Wohnung"
        return "Wohnung"

    # kauf
    score = {"Wohnung":0, "Haus":0, "Gewerbe":0, "Kapitalanlage":0}
    for k in KEYS_WOHNUNG:    score["Wohnung"]       += text.count(k)
    for k in KEYS_HAUS:       score["Haus"]          += text.count(k)
    for k in KEYS_GEWERBE:    score["Gewerbe"]       += text.count(k)
    for k in KEYS_KAPITAL:    score["Kapitalanlage"] += text.count(k)

    if score["Kapitalanlage"] >= 2: return "Kapitalanlage"
    if score["Gewerbe"] >= 2:       return "Gewerbe"
    if score["Haus"] >= 2 and score["Wohnung"] == 0: return "Haus"
    if score["Wohnung"] >= 2 and score["Haus"] == 0: return "Wohnung"

    best = max(score, key=score.get)
    if score[best] >= 2 and list(score.values()).count(score[best]) == 1:
        return best
    return "Haus"

def gpt_category(row):
    if not USE_GPT_CLASSIFY or not OPENAI_API_KEY:
        return ""
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
        prompt = (
            "Klassifiziere dieses Immobilien-Exposé in genau eine Kategorie:\n"
            "Wohnung, Haus, Gewerbe, Kapitalanlage.\n"
            "Gib nur das Wort aus.\n\n"
            f"Titel: {row.get('Titel','')}\n"
            f"Beschreibung: {row.get('Description','')}\n"
        )
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role":"system","content":"Du bist ein präziser Immobilien-Klassifizierer."},
                      {"role":"user","content": prompt}],
            temperature=0
        )
        out = (resp.choices[0].message.content or "").strip().lower()
        mapping = {"wohnung":"Wohnung","haus":"Haus","gewerbe":"Gewerbe","kapitalanlage":"Kapitalanlage"}
        return mapping.get(out.replace(".", "").strip(), "")
    except Exception as e:
        print(f"[GPT] Fehler: {e}")
        return ""

def decide_subcategory(row, mode):
    sub = heuristic_subcategory(row, mode)
    if sub:
        return sub
    if mode == "kauf":
        g = gpt_category(row)
        if g in {"Wohnung","Haus","Gewerbe","Kapitalanlage"}:
            return g
    return "Wohnung" if mode == "miete" else "Haus"

# ---------------------------------------------------------------------------
# Airtable API – Helpers (unverändert)
# ---------------------------------------------------------------------------
def airtable_table_segment():
    if AIRTABLE_TABLE_ID:
        return AIRTABLE_TABLE_ID
    return quote(AIRTABLE_TABLE, safe="") if AIRTABLE_TABLE else ""

def airtable_api_url():
    seg = airtable_table_segment()
    if not (AIRTABLE_BASE and seg):
        raise RuntimeError(f"[Airtable] BASE oder TABLE/TABLE_ID fehlt. "
                           f"BASE='{AIRTABLE_BASE}', TABLE_ID='{AIRTABLE_TABLE_ID}', TABLE='{AIRTABLE_TABLE}'")
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{seg}"
    print(f"[Airtable] URL: {url}")
    return url

def airtable_headers():
    if not AIRTABLE_TOKEN:
        raise RuntimeError("[Airtable] AIRTABLE_TOKEN fehlt.")
    return {"Authorization": f"Bearer {AIRTABLE_TOKEN}", "Content-Type": "application/json"}

def airtable_existing_fields():
    url = airtable_api_url()
    params = {"maxRecords": 1}
    if AIRTABLE_VIEW:
        params["view"] = AIRTABLE_VIEW
    r = requests.get(url, headers=airtable_headers(), params=params, timeout=30)
    if not r.ok:
        print(f"[Airtable] Schema-Check Warnung {r.status_code}: {r.text[:400]}")
        return set()
    data = r.json()
    if data.get("records"):
        return set(data["records"][0].get("fields", {}).keys())
    return set()

def sanitize_record_for_airtable(record: dict, allowed_fields: set) -> dict:
    out = {k: v for k, v in record.items() if (not allowed_fields or k in allowed_fields)}
    if "Preis" in out and (out["Preis"] is None or out["Preis"] == ""):
        out.pop("Preis", None)
    return out

def airtable_batch_create(rows):
    url = airtable_api_url()
    for i in range(0, len(rows), 10):
        payload = {"records": [{"fields": r} for r in rows[i:i+10]], "typecast": True}
        r = requests.post(url, headers=airtable_headers(), data=json.dumps(payload), timeout=60)
        if not r.ok:
            print(f"[Airtable] Create Fehler {r.status_code}: {r.text[:800]}")
            r.raise_for_status()
        time.sleep(0.25)

def airtable_batch_update(pairs):
    url = airtable_api_url()
    for i in range(0, len(pairs), 10):
        payload = {"records": pairs[i:i+10], "typecast": True}
        r = requests.patch(url, headers=airtable_headers(), data=json.dumps(payload), timeout=60)
        if not r.ok:
            print(f"[Airtable] Update Fehler {r.status_code}: {r.text[:800]}")
            r.raise_for_status()
        time.sleep(0.25)

def airtable_batch_delete(ids):
    url = airtable_api_url()
    for i in range(0, len(ids), 10):
        params = [("records[]", rid) for rid in ids[i:i+10]]
        r = requests.delete(url, headers=airtable_headers(), params=params, timeout=60)
        if not r.ok:
            print(f"[Airtable] Delete Fehler {r.status_code}: {r.text[:800]}")
            r.raise_for_status()
        time.sleep(0.2)

def airtable_list_all():
    ids, fields = [], []
    url = airtable_api_url()
    params = {}
    if AIRTABLE_VIEW:
        params["view"] = AIRTABLE_VIEW
    while True:
        r = requests.get(url, headers=airtable_headers(), params=params, timeout=60)
        if not r.ok:
            raise RuntimeError(f"[Airtable] List Fehler {r.status_code}: {r.text[:400]}")
        data = r.json()
        for rec in data.get("records", []):
            ids.append(rec["id"])
            fields.append(rec.get("fields", {}))
        offset = data.get("offset")
        if not offset:
            break
        params["offset"] = offset
        time.sleep(0.2)
    return ids, fields

# ---------------------------------------------------------------------------
# Felder/Keys
# ---------------------------------------------------------------------------
def parse_price_to_number(label: str):
    if not label:
        return None
    cleaned = re.sub(r"[^\d,\.]", "", label)
    # EU-Format → Komma als Dezimaltrenner, Punkte tausend
    if "," in cleaned and "." in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    elif "," in cleaned and "." not in cleaned:
        cleaned = cleaned.replace(",", ".")
    try:
        return float(cleaned)
    except Exception:
        return None

def make_record(row, kategorie_main, unterkat):
    return {
        "Titel":           row["Titel"],
        "Kategorie":       kategorie_main,       # Kaufen/Mieten
        "Unterkategorie":  unterkat,             # Wohnung/Haus/Gewerbe/Stellplatz; Kapitalanlage
        "Webseite":        row["URL"],
        "Objektnummer":    row["Objektnummer"],
        "Beschreibung":    row["Description"],
        "Bild":            row["Bild_URL"],
        "Preis":           parse_price_to_number(row["Preis"]),
        "Standort":        row["Ort"],
    }

def unique_key(fields: dict) -> str:
    obj = (fields.get("Objektnummer") or "").strip()
    if obj:
        return f"obj:{obj}"
    url = (fields.get("Webseite") or "").strip()
    if url:
        return f"url:{url}"
    return f"hash:{hash(json.dumps(fields, sort_keys=True))}"

# ---------------------------------------------------------------------------
# Allgemeine Sync-Logik (Upsert) je Kategorie – unverändert
# ---------------------------------------------------------------------------
def sync_category(scraped_rows, category_label: str):
    allowed = airtable_existing_fields()
    print(f"[Airtable] Erkannte Felder: {sorted(list(allowed)) or '(keine – Tabelle evtl. leer)'}")

    all_ids, all_fields = airtable_list_all()
    existing = {}
    for rec_id, f in zip(all_ids, all_fields):
        if f.get("Kategorie") == category_label:
            existing[unique_key(f)] = (rec_id, f)

    desired = {}
    for r in scraped_rows:
        if r.get("Kategorie") == category_label:
            k = unique_key(r)
            desired[k] = sanitize_record_for_airtable(r, allowed)

    to_create, to_update, keep = [], [], set()
    for k, fields in desired.items():
        if k in existing:
            rec_id, old = existing[k]
            diff = {fld: val for fld, val in fields.items() if old.get(fld) != val}
            if diff:
                to_update.append({"id": rec_id, "fields": diff})
            keep.add(k)
        else:
            to_create.append(fields)

    to_delete_ids = [rec_id for k, (rec_id, _) in existing.items() if k not in keep]

    print(f"[SYNC] {category_label} → create: {len(to_create)}, update: {len(to_update)}, delete: {len(to_delete_ids)}")
    if to_create: airtable_batch_create(to_create)
    if to_update: airtable_batch_update(to_update)
    if to_delete_ids: airtable_batch_delete(to_delete_ids)

# ---------------------------------------------------------------------------
def run(mode: str):
    """
    API identisch zu Werneburg: run(mode) mit 'kauf' oder 'miete'.
    Für MSI ist aktuell nur 'kauf' hinterlegt – 'miete' würde 0 Seiten liefern.
    """
    assert mode in ("kauf", "miete"), "Mode muss 'kauf' oder 'miete' sein."
    kategorie_main = "Kaufen" if mode == "kauf" else "Mieten"
    csv_name = f"msi_{mode}.csv"

    all_rows, seen = [], set()
    pages = get_list_page_urls(mode)
    if not pages:
        print(f"[INFO] Keine Listen-Seiten für Mode='{mode}'.")
        return

    for idx, list_url in enumerate(pages, 1):
        try:
            detail_links = collect_detail_links(list_url)
        except Exception as e:
            print(f"[WARN] Abbruch beim Lesen der Liste: {list_url} -> {e}")
            break

        if not detail_links:
            print(f"[INFO] Keine Einträge auf Seite {idx} – Stop.")
            break

        new_links = [u for u in detail_links if u not in seen]
        seen.update(new_links)
        if not new_links:
            print(f"[INFO] Seite {idx}: keine neuen Links – weiter.")
            continue

        print(f"[{mode.upper()}] Seite {idx}: {len(new_links)} Exposés")
        for j, url in enumerate(new_links, 1):
            try:
                row = parse_detail(url, mode)
                unterkat = decide_subcategory(row, mode)
                record = make_record(row, kategorie_main, unterkat)
                all_rows.append(record)
                print(f"  - ({j}/{len(new_links)}) {record['Titel'][:60]} → {record['Kategorie']} / {record['Unterkategorie']}")
                time.sleep(0.15)
            except Exception as e:
                print(f"    [FEHLER] {url} -> {e}")
                continue
        time.sleep(0.25)

    if all_rows:
        cols = ["Titel","Kategorie","Unterkategorie","Webseite","Objektnummer","Beschreibung","Bild","Preis","Standort"]
        with open(csv_name, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(all_rows)
        print(f"[OK] CSV gespeichert: {csv_name} ({len(all_rows)} Zeilen)")

        if AIRTABLE_TOKEN and AIRTABLE_BASE and airtable_table_segment():
            sync_category(all_rows, kategorie_main)
        else:
            print("[Airtable] ENV nicht gesetzt – Upload übersprungen.")
    else:
        print("[WARN] Keine Datensätze gefunden.")

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    mode = (sys.argv[1].strip().lower() if len(sys.argv) > 1 else "kauf")
    run(mode)
