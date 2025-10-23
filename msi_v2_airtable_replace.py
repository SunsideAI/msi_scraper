# msi_v2_airtable_replace.py
# -*- coding: utf-8 -*-
import sys, time, csv, os, json, requests, re
from urllib.parse import quote, urljoin
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Konfiguration / ENV
# ---------------------------------------------------------------------------
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
    MSI listet Kauf & Miete zusammen auf /kaufen/immobilienangebote/ und paginiert mit /page/{n}.
    Wir verwenden für ALLE Modi diese Listing-Seite.
    """
    first = f"{BASE}/kaufen/immobilienangebote/"
    pattern = f"{BASE}/kaufen/immobilienangebote/page/{{n}}/"
    return [first] + [pattern.format(n=i) for i in range(2, max_pages + 1)]

def collect_detail_links(list_url: str):
    """
    Sammelt alle Detail-URLs unter /angebote/...; dedupliziert.
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
# Detail-Parser – robuste Extraktion für Preis / Beschreibung / Ort
# ---------------------------------------------------------------------------
RE_EUR_ANY        = re.compile(r"\b\d{1,3}(?:\.\d{3})*(?:,\d{2})?\s*€")
RE_EUR_NUMBER     = re.compile(r"\b\d{1,3}(?:\.\d{3})*(?:,\d{2})?\b")
RE_PRICE_LINE     = re.compile(r"(kaufpreis|preis|kaltmiete|warmmiete|nettokaltmiete|miete)\s*:?\s*([0-9\.\,]+)\s*€?", re.I)
RE_OBJEKTNR       = re.compile(r"Objekt[-\s]?Nr\.?:\s*([A-Za-z0-9\-_/]+)")
RE_PLZ_ORT_STRICT = re.compile(r"\b(?!0{5})(\d{5})\s+([A-Za-zÄÖÜäöüß][A-Za-zÄÖÜäöüß\-\s]+?)\b(?![A-Za-zÄÖÜäöüß])")

RE_KAUF  = re.compile(r"\bzum\s*kauf\b", re.IGNORECASE)
RE_MIETE = re.compile(r"\bzur\s*miete\b|\b(kaltmiete|warmmiete|nettokaltmiete)\b", re.IGNORECASE)

SECTION_HEADERS = ("beschreibung", "objektbeschreibung", "ausstattung", "lage", "sonstiges", "weitere informationen")

STOP_STRINGS = ("Ihre Anfrage", "Kontakt", "Exposé anfordern", "Neueste Immobilien")

def detect_category(page_text: str) -> str:
    if RE_MIETE.search(page_text):
        return "Mieten"
    if RE_KAUF.search(page_text):
        return "Kaufen"
    # Heuristik: ohne Mietbegriffe → eher Kauf
    return "Kaufen"

def clean_price_string(s: str) -> str:
    if not s: return ""
    s = s.strip()
    m = RE_EUR_NUMBER.search(s)
    if not m: return ""
    n = m.group(0).replace(".", "").replace(",", ".")
    try:
        val = float(n)
        # Schön formatiert zurückgeben („123.456 €“)
        return f"{val:,.0f} €".replace(",", ".")
    except:
        eur = RE_EUR_ANY.search(s)
        return eur.group(0) if eur else ""

def extract_price_from_jsonld(soup: BeautifulSoup) -> str:
    """
    Liest Preis aus JSON-LD (schema.org Offer -> price / priceCurrency), falls vorhanden.
    """
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            data = json.loads(script.get_text(strip=True))
        except Exception:
            continue
        # JSON-LD kann Objekt oder Liste sein
        candidates = data if isinstance(data, list) else [data]
        for node in candidates:
            # Direkt Offer
            if isinstance(node, dict):
                offer = None
                if node.get("@type") in ("Offer", "AggregateOffer"):
                    offer = node
                elif "offers" in node:
                    offer = node["offers"]
                if isinstance(offer, dict):
                    price = offer.get("price") or offer.get("lowPrice")
                    if price:
                        try:
                            val = float(str(price).replace(".", "").replace(",", "."))
                            return f"{val:,.0f} €".replace(",", ".")
                        except:
                            pass
                # Fallback: irgendwo price-Feld
                for k in ("price", "lowPrice", "highPrice"):
                    if k in node and node[k]:
                        try:
                            val = float(str(node[k]).replace(".", "").replace(",", "."))
                            return f"{val:,.0f} €".replace(",", ".")
                        except:
                            continue
    return ""

def extract_price_dom(soup: BeautifulSoup) -> str:
    # dt/dd
    for dt in soup.select("dt"):
        label = (dt.get_text(" ", strip=True) or "").lower()
        if any(k in label for k in ("kaufpreis", "kaltmiete", "warmmiete", "nettokaltmiete", "miete", "preis")):
            dd = dt.find_next_sibling("dd")
            if dd:
                got = clean_price_string(dd.get_text(" ", strip=True))
                if got: return got
    # table tr th/td
    for tr in soup.select("table tr"):
        cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
        if len(cells) >= 2:
            label = cells[0].lower()
            if any(k in label for k in ("kaufpreis", "kaltmiete", "warmmiete", "nettokaltmiete", "miete", "preis")):
                got = clean_price_string(cells[1])
                if got: return got
    # label: value in Listen
    for li in soup.select("li"):
        txt = li.get_text(" ", strip=True)
        m = RE_PRICE_LINE.search(txt)
        if m:
            return clean_price_string(m.group(2) + " €")
    return ""

def extract_price_strict_top(page_text: str) -> str:
    """
    Fallback: Suche NUR im oberen Seitenabschnitt (bis erste STOP_STRINGS),
    und nimm die erste vernünftige €-Zahl (≥ 10.000).
    """
    top = page_text
    for stop in STOP_STRINGS:
        pos = top.lower().find(stop.lower())
        if pos != -1:
            top = top[:pos]
            break
    euros = [e.group(0) for e in RE_EUR_ANY.finditer(top)]
    euros_filtered = []
    for e in euros:
        num = RE_EUR_NUMBER.search(e)
        if not num:
            continue
        try:
            val = float(num.group(0).replace(".", "").replace(",", "."))
            if val >= 10000:  # realistische Untergrenze
                euros_filtered.append(e)
        except:
            continue
    if euros_filtered:
        # Erste vernünftige Zahl im Kopfbereich
        return clean_price_string(euros_filtered[0])
    return ""

def extract_price(soup: BeautifulSoup, page_text: str) -> str:
    # 0) JSON-LD (präzise)
    p = extract_price_from_jsonld(soup)
    if p: return p
    # 1) Zeilenweise Label→Wert
    for txt in page_text.splitlines():
        txt = txt.strip()
        m = RE_PRICE_LINE.search(txt)
        if m:
            return clean_price_string(m.group(2) + " €")
    # 2) DOM
    p = extract_price_dom(soup)
    if p: return p
    # 3) Kopfbereich–Fallback
    p = extract_price_strict_top(page_text)
    if p: return p
    # 4) letzter Fallback: größte Euro-Zahl im ganzen Dokument
    euros = [e.group(0) for e in RE_EUR_ANY.finditer(page_text)]
    if euros:
        def to_float(e):
            n = RE_EUR_NUMBER.search(e).group(0).replace(".", "").replace(",", ".")
            try: return float(n)
            except: return 0.0
        best = max(euros, key=to_float)
        return clean_price_string(best)
    return ""

def extract_description(soup: BeautifulSoup) -> str:
    """
    Bevorzugt den Abschnitt unter einer H2/H3-Überschrift mit 'Beschreibung' o.ä.
    Fallbacks: typische Content-Container, Meta/OG, Feature-Liste, dann erste <p> nach H1.
    """
    # 1) Abschnitt anhand Überschrift
    for h in soup.select("h2, h3"):
        head = (h.get_text(" ", strip=True) or "").lower()
        if any(k in head for k in SECTION_HEADERS):
            chunks = []
            for sib in h.find_all_next():
                if sib.name in ("h2", "h3"):
                    break
                if sib.name == "p":
                    t = sib.get_text(" ", strip=True)
                    if t and not any(stop in t for stop in STOP_STRINGS):
                        chunks.append(t)
                if sib.has_attr("class"):
                    cls = " ".join(sib.get("class"))
                    if any(k in cls for k in ("contact", "form", "sidebar", "widget")):
                        break
                if len(chunks) >= 12:
                    break
            if chunks:
                return "\n\n".join(chunks).strip()

    # 2) Content-Container
    candidates = soup.select(".entry-content, article, .content, .post-content, .single-content, [class*='content'], [class*='text']")
    for c in candidates:
        ps = [p.get_text(" ", strip=True) for p in c.select("p")]
        ps = [p for p in ps if p and not any(stop in p for stop in STOP_STRINGS)]
        if ps:
            return "\n\n".join(ps[:12]).strip()

    # 3) OG/META-Description
    ogd = soup.select_one('meta[property="og:description"]')
    if ogd and ogd.get("content"):
        c = ogd["content"].strip()
        if c:
            return c
    md = soup.select_one('meta[name="description"]')
    if md and md.get("content"):
        c = md["content"].strip()
        if c:
            return c

    # 4) Feature-Fallback (Zimmer/Schlafzimmer/Bäder etc.)
    features = []
    for li in soup.select("li"):
        t = li.get_text(" ", strip=True)
        if not t:
            continue
        if re.search(r"\b(zimmer|schlafzimmer|badezimmer|wohnfl|grundstück|nutzfl|balkon|terrasse|garage|stellplatz)\b", t, re.I):
            if not any(stop in t for stop in STOP_STRINGS):
                features.append(t)
    features = list(dict.fromkeys(features))
    if features:
        return " • ".join(features[:12])

    # 5) erste <p> nach H1
    h1 = soup.select_one("h1")
    if h1:
        ps = []
        for sib in h1.find_all_next():
            if sib.name == "p":
                t = sib.get_text(" ", strip=True)
                if t and not any(stop in t for stop in STOP_STRINGS):
                    ps.append(t)
                if len(ps) >= 8:
                    break
        if ps:
            return "\n\n".join(ps).strip()

    return ""

def extract_plz_ort(page_text: str) -> str:
    """
    Liefert NUR 'PLZ Ort' (ohne zusätzlichen Satz/Beifang).
    """
    m = RE_PLZ_ORT_STRICT.search(page_text)
    if not m:
        return ""
    plz, ort = m.group(1), m.group(2)
    # Ort hart säubern (bis zum ersten Trennzeichen)
    ort = re.split(r"[|,•·\-\–—/()]", ort)[0].strip()
    ort = re.sub(r"\s{2,}", " ", ort)
    return f"{plz} {ort}"

def parse_detail(detail_url: str, mode: str):
    soup = soup_get(detail_url)
    page_text = soup.get_text("\n", strip=True)

    # Titel
    h1 = soup.select_one("h1")
    title = h1.get_text(strip=True) if h1 else ""

    # Beschreibung (robust)
    description = extract_description(soup)

    # Objektnummer
    m_obj = RE_OBJEKTNR.search(page_text)
    objektnummer = m_obj.group(1).strip() if m_obj else ""

    # Preis
    preis_value = extract_price(soup, page_text)

    # Ort (nur 'PLZ Ort')
    ort = extract_plz_ort(page_text)

    # Bild (externes Galerie-Link bevorzugt)
    image_url = ""
    a_img = soup.select_one('a[href*="immo."]') or soup.select_one('a[href*="screenwork"]')
    if a_img and a_img.has_attr("href"):
        image_url = a_img["href"]
    else:
        img = soup.find("img")
        if img and img.has_attr("src"):
            image_url = urljoin(BASE, img["src"])

    # Kategorie aus Detailtext erkennen
    kategorie_detected = detect_category(page_text)

    return {
        "Titel":        title,
        "URL":          detail_url,
        "Description":  description,
        "Objektnummer": objektnummer,
        "Preis":        preis_value,
        "Ort":          ort,
        "Bild_URL":     image_url,
        "KategorieDetected": kategorie_detected,
    }

# ---------------------------------------------------------------------------
# Heuristiken/Klassifikation
# ---------------------------------------------------------------------------
KEYS_WOHNUNG    = ["wohnung", "etagenwohnung", "eigentumswohnung", "apartment", "dachgeschoss", "maisonette", "penthouse", "balkon"]
KEYS_HAUS       = ["haus", "einfamilienhaus", "zweifamilienhaus", "reihenhaus", "doppelhaushälfte", "stadtvilla", "mehrfamilienhaus", "dhh"]
KEYS_GEWERBE    = ["gewerbe", "büro", "laden", "praxis", "lager", "halle", "gastronomie", "gewerbeeinheit", "gewerbefläche"]
KEYS_KAPITAL    = ["kapitalanlage", "rendite", "vermietet", "anlageobjekt", "investment"]
KEYS_STELLPLATZ = ["stellplatz", "parkplatz", "tiefgarage", "garage", "carport"]

def heuristic_subcategory(row):
    text = f"{row.get('Titel','')} {row.get('Description','')}".lower()
    score = {"Wohnung":0, "Haus":0, "Gewerbe":0, "Kapitalanlage":0}
    for k in KEYS_WOHNUNG:  score["Wohnung"]       += text.count(k)
    for k in KEYS_HAUS:     score["Haus"]          += text.count(k)
    for k in KEYS_GEWERBE:  score["Gewerbe"]       += text.count(k)
    for k in KEYS_KAPITAL:  score["Kapitalanlage"] += text.count(k)

    if score["Kapitalanlage"] >= 2: return "Kapitalanlage"
    if score["Gewerbe"] >= 2:       return "Gewerbe"
    if score["Haus"] >= 2 and score["Wohnung"] == 0: return "Haus"
    if score["Wohnung"] >= 2 and score["Haus"] == 0: return "Wohnung"

    best = max(score, key=score.get)
    if score[best] >= 2 and list(score.values()).count(score[best]) == 1:
        return best
    return "Haus"  # pragmatischer Fallback

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
            messages=[
                {"role":"system","content":"Du bist ein präziser Immobilien-Klassifizierer."},
                {"role":"user","content": prompt}
            ],
            temperature=0
        )
        out = (resp.choices[0].message.content or "").strip().lower()
        mapping = {"wohnung":"Wohnung","haus":"Haus","gewerbe":"Gewerbe","kapitalanlage":"Kapitalanlage"}
        return mapping.get(out.replace(".", "").strip(), "")
    except Exception as e:
        print(f"[GPT] Fehler: {e}")
        return ""

def decide_subcategory(row):
    sub = heuristic_subcategory(row)
    if sub: return sub
    g = gpt_category(row)
    if g in {"Wohnung","Haus","Gewerbe","Kapitalanlage"}: return g
    return "Haus"

# ---------------------------------------------------------------------------
# Airtable API – Helpers (identisch zur Werneburg-Logik)
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
    if "," in cleaned and "." in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    elif "," in cleaned and "." not in cleaned:
        cleaned = cleaned.replace(",", ".")
    try:
        return float(cleaned)
    except Exception:
        return None

def make_record(row, unterkat):
    return {
        "Titel":           row["Titel"],
        "Kategorie":       row["KategorieDetected"],   # Wichtig: aus Detail erkannt („Kaufen“/„Mieten“)
        "Unterkategorie":  unterkat,
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
# Sync-Logik (Upsert) je Kategorie
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
    Modi:
      - 'kauf'  : nur Kaufen-Sätze schreiben/syncen
      - 'miete' : nur Mieten-Sätze schreiben/syncen
      - 'auto'  : beides erkennen; zwei CSVs; zwei Upserts
    """
    assert mode in ("kauf", "miete", "auto"), "Mode muss 'kauf', 'miete' oder 'auto' sein."
    csv_kauf  = "msi_kauf.csv"
    csv_miete = "msi_miete.csv"

    all_rows, seen = [], set()
    for idx, list_url in enumerate(get_list_page_urls("kauf"), 1):
        try:
            detail_links = collect_detail_links(list_url)
        except Exception as e:
            print(f"[WARN] Abbruch beim Lesen der Liste: {list_url} -> {e}")
            break

        new_links = [u for u in detail_links if u not in seen]
        if idx > 1 and not new_links:
            print(f"[INFO] Keine neuen Links auf Seite {idx} – Stop.")
            break
        seen.update(new_links)
        print(f"[Seite {idx}] {len(new_links)} neue Detailseiten")

        for j, url in enumerate(new_links, 1):
            try:
                row = parse_detail(url, mode)

                # --- NEU: Skip, wenn im Titel "verkauft" steht ---
                if re.search(r"\bverkauft\b", row.get("Titel", ""), re.IGNORECASE):
                    print(f"  - {j}/{len(new_links)} SKIPPED (verkauft) | {row.get('Titel','')[:70]}")
                    continue
                # -------------------------------------------------

                unterkat = decide_subcategory(row)
                record = make_record(row, unterkat)
                all_rows.append(record)
                print(f"  - {j}/{len(new_links)} {record['Kategorie']:6} | {record['Titel'][:70]}")
                time.sleep(0.15)
            except Exception as e:
                print(f"    [FEHLER] {url} -> {e}")
                continue
        time.sleep(0.25)

    if not all_rows:
        print("[WARN] Keine Datensätze gefunden.")
        return

    rows_kauf  = [r for r in all_rows if r["Kategorie"] == "Kaufen"]
    rows_miete = [r for r in all_rows if r["Kategorie"] == "Mieten"]

    # Filter je nach Modus
    if mode == "kauf":
        rows_miete = []
    elif mode == "miete":
        rows_kauf = []

    # CSVs schreiben
    cols = ["Titel","Kategorie","Unterkategorie","Webseite","Objektnummer","Beschreibung","Bild","Preis","Standort"]
    if rows_kauf:
        with open(csv_kauf, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols); w.writeheader(); w.writerows(rows_kauf)
        print(f"[CSV] {csv_kauf}: {len(rows_kauf)} Zeilen")
    if rows_miete:
        with open(csv_miete, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols); w.writeheader(); w.writerows(rows_miete)
        print(f"[CSV] {csv_miete}: {len(rows_miete)} Zeilen")

    # Upsert je Kategorie
    if AIRTABLE_TOKEN and AIRTABLE_BASE and airtable_table_segment():
        if rows_kauf:
            sync_category(rows_kauf, "Kaufen")
        if rows_miete:
            sync_category(rows_miete, "Mieten")
    else:
        print("[Airtable] ENV nicht gesetzt – Upload übersprungen.")

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    mode = (sys.argv[1].strip().lower() if len(sys.argv) > 1 else "auto")
    run(mode)
