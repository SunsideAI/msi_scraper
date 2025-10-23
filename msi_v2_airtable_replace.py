# msi_v2_airtable_replace.py
# -*- coding: utf-8 -*-
import sys, time, csv, os, json, requests, re
from urllib.parse import quote, urljoin
from bs4 import BeautifulSoup, NavigableString, Tag

# ---------------------------------------------------------------------------
# KONFIGURATION / ENV
# ---------------------------------------------------------------------------
BASE = "https://www.msi-hessen.de"
HEADERS = {"User-Agent": "Mozilla/5.0"}

# Airtable ENV
AIRTABLE_TOKEN    = os.getenv("AIRTABLE_TOKEN", "").strip()
AIRTABLE_BASE     = os.getenv("AIRTABLE_BASE",  "").strip()
AIRTABLE_TABLE    = os.getenv("AIRTABLE_TABLE", "").strip()    # optional (Name)
AIRTABLE_TABLE_ID = os.getenv("AIRTABLE_TABLE_ID", "").strip() # bevorzugt (tbl...)
AIRTABLE_VIEW     = os.getenv("AIRTABLE_VIEW", "").strip()     # optional

# ---------------------------------------------------------------------------
# HTTP / HTML
# ---------------------------------------------------------------------------
def soup_get(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def get_list_page_urls(mode: str, max_pages: int = 50):
    first = f"{BASE}/kaufen/immobilienangebote/"
    pattern = f"{BASE}/kaufen/immobilienangebote/page/{{n}}/"
    return [first] + [pattern.format(n=i) for i in range(2, max_pages + 1)]

def collect_detail_links(list_url: str):
    soup = soup_get(list_url)
    links = set()
    for a in soup.select('a[href*="/angebote/"]'):
        href = a.get("href")
        if href:
            links.add(href if href.startswith("http") else urljoin(BASE, href))
    return list(links)

# ---------------------------------------------------------------------------
# REGEX & KONSTANTEN
# ---------------------------------------------------------------------------
THIN_SPACES = "\u00A0\u202F\u2009"
RE_EUR_NUMBER = re.compile(r"\b\d{1,3}(?:[.\u00A0\u202F\u2009]\d{3})*(?:,\d{2})?\b")
RE_EUR_ANY = re.compile(r"\b\d{1,3}(?:[.\u00A0\u202F\u2009]\d{3})*(?:,\d{2})?\s*[€EUR]?")
RE_PRICE_LINE = re.compile(
    r"(kaufpreis|preis|kaltmiete|warmmiete|nettokaltmiete|miete)\s*:?\s*([0-9.\u00A0\u202F\u2009,]+)\s*[€]?",
    re.I,
)
RE_OBJEKTNR = re.compile(r"Objekt[-\s]?Nr\.?:\s*([A-Za-z0-9\-_/]+)")
RE_PLZ_ORT_STRICT = re.compile(
    r"\b(?!0{5})(\d{5})\s+([A-Za-zÄÖÜäöüß][A-Za-zÄÖÜäöüß\-\s]+?)\b(?![A-Za-zÄÖÜäöüß])"
)
RE_KAUF = re.compile(r"\bzum\s*kauf\b", re.IGNORECASE)
RE_MIETE = re.compile(
    r"\bzur\s*miete\b|\b(kaltmiete|warmmiete|nettokaltmiete)\b", re.IGNORECASE
)

# Footer/CTA/Contact-Filter
STOP_STRINGS = (
    "Ihre Anfrage", "Exposé anfordern", "Neueste Immobilien", "Teilen auf",
    "Datenschutz", "Impressum", "designed by wavepoint",
    "Ansprechpartner", "Kontaktieren Sie uns", "Zur Objektanfrage",
    "Telefon", "E-Mail", "Email", "Details", "msi-hessen.de"
)

TAB_LABELS = {
    "Beschreibung": ("beschreibung",),
    "Objektangaben": ("objektangaben", "objektdaten", "daten"),
    "Ausstattung": ("ausstattung", "merkmale"),
    "Lage": ("lage", "lagebeschreibung", "umfeld"),
    "Energieausweis": ("energieausweis", "energie", "energiekennwerte"),
}

# ---------------------------------------------------------------------------
# UTILS
# ---------------------------------------------------------------------------
def _norm(s: str) -> str:
    return re.sub(r"\s{2,}", " ", (s or "").strip())

def detect_category(page_text: str) -> str:
    if RE_MIETE.search(page_text):
        return "Mieten"
    if RE_KAUF.search(page_text):
        return "Kaufen"
    return "Kaufen"

# ---------------------------------------------------------------------------
# PREIS-PARSING
# ---------------------------------------------------------------------------
def _normalize_numstring(s: str) -> str:
    if not s:
        return ""
    s = s.strip()
    for ch in THIN_SPACES:
        s = s.replace(ch, "")
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    else:
        if "." in s:
            last = s.rsplit(".", 1)[-1]
            if last.isdigit() and len(last) in (3, 6):
                s = s.replace(".", "")
    return s

def clean_price_string(raw: str) -> str:
    if not raw:
        return ""
    m = RE_EUR_NUMBER.search(raw)
    if not m:
        return ""
    num = _normalize_numstring(m.group(0))
    try:
        val = float(num)
    except Exception:
        return ""
    return f"{val:,.0f} €".replace(",", ".")

def parse_price_to_number(label: str):
    if not label:
        return None
    m = RE_EUR_NUMBER.search(label)
    if not m:
        return None
    num = _normalize_numstring(m.group(0))
    try:
        return float(num)
    except Exception:
        return None

def _panel_from_tablink(a_tag):
    href = (a_tag.get("href") or "").strip()
    if href.startswith("#"):
        panel = a_tag.find_parent().find_parent().find_next(id=href[1:])
        if panel:
            return panel
    target = a_tag.get("aria-controls")
    if target:
        panel = a_tag.find_parent().find_parent().find_next(id=target)
        if panel:
            return panel
    return None

def _find_tab_navs(soup):
    pairs = []
    for nav in soup.select(".kt-tabs-title-list, .nav-tabs, .elementor-tabs-wrapper, ul"):
        for a in nav.select('a[href^="#"], a[aria-controls]'):
            label = _norm(a.get_text(" ", strip=True))
            if not label:
                continue
            panel = _panel_from_tablink(a)
            if panel:
                pairs.append((label, panel))
    return pairs

def extract_price_from_objektangaben(soup: BeautifulSoup) -> str:
    """Suche im Panel 'Objektangaben' nach Kaufpreis/Miete."""
    tab_pairs = _find_tab_navs(soup)
    target_panel = None
    for label, panel in tab_pairs:
        if any(alias in label.lower() for alias in TAB_LABELS["Objektangaben"]):
            target_panel = panel
            break
    if not target_panel:
        for tbl in soup.select("table"):
            if "kaufpreis" in tbl.get_text(" ", strip=True).lower():
                target_panel = tbl
                break
    if not target_panel:
        return ""
    keys = ("kaufpreis", "kaltmiete", "warmmiete", "nettokaltmiete", "miete", "preis")
    for tr in target_panel.select("tr"):
        cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
        if len(cells) >= 2 and any(k in cells[0].lower() for k in keys):
            got = clean_price_string(cells[1]);  if got: return got
    for dt in target_panel.select("dt"):
        dd = dt.find_next_sibling("dd")
        if any(k in (dt.get_text(" ", strip=True) or "").lower() for k in keys):
            got = clean_price_string(dd.get_text(" ", strip=True) if dd else "")
            if got: return got
    for li in target_panel.select("li"):
        txt = li.get_text(" ", strip=True)
        m = RE_PRICE_LINE.search(txt)
        if m:
            return clean_price_string(m.group(2) + " €")
    return ""

def extract_price_near_objnr(soup: BeautifulSoup) -> str:
    """Sucht Preis im Kopfbereich in der Nähe von 'Objekt-Nr.' (typisch MSI)."""
    obj_nodes = soup.find_all(string=re.compile(r"Objekt[-\s]?Nr", re.I))
    for txtnode in obj_nodes:
        container = txtnode
        for _ in range(4):
            if hasattr(container, "parent"):
                container = container.parent
        context = container.get_text(" ", strip=True)
        m = RE_EUR_ANY.search(context)
        if m:
            return clean_price_string(m.group(0))
        prev = container.previous_sibling
        if prev and hasattr(prev, "get_text"):
            t = prev.get_text(" ", strip=True)
            m = RE_EUR_ANY.search(t)
            if m:
                return clean_price_string(m.group(0))
    return ""

def extract_price_from_jsonld(soup: BeautifulSoup) -> str:
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            data = json.loads(script.get_text(strip=True))
        except Exception:
            continue
        nodes = data if isinstance(data, list) else [data]
        for node in nodes:
            if not isinstance(node, dict):
                continue
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
            for k in ("price", "lowPrice", "highPrice"):
                if k in node and node[k]:
                    try:
                        val = float(str(node[k]).replace(".", "").replace(",", "."))
                        return f"{val:,.0f} €".replace(",", ".")
                    except:
                        continue
    return ""

def extract_price_dom(soup: BeautifulSoup) -> str:
    for dt in soup.select("dt"):
        label = (dt.get_text(" ", strip=True) or "").lower()
        if any(k in label for k in ("kaufpreis","kaltmiete","warmmiete","nettokaltmiete","miete","preis")):
            dd = dt.find_next_sibling("dd")
            if dd:
                got = clean_price_string(dd.get_text(" ", strip=True))
                if got: return got
    for tr in soup.select("table tr"):
        cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th","td"])]
        if len(cells) >= 2:
            label = cells[0].lower()
            if any(k in label for k in ("kaufpreis","kaltmiete","warmmiete","nettokaltmiete","miete","preis")):
                got = clean_price_string(cells[1]);  if got: return got
    for li in soup.select("li"):
        txt = li.get_text(" ", strip=True)
        m = RE_PRICE_LINE.search(txt)
        if m:
            return clean_price_string(m.group(2) + " €")
    return ""

def extract_price_strict_top(page_text: str) -> str:
    top = page_text
    for stop in STOP_STRINGS:
        pos = top.lower().find(stop.lower())
        if pos != -1:
            top = top[:pos]
            break
    euros = [e.group(0) for e in RE_EUR_ANY.finditer(top)]
    euros_filtered = []
    for e in euros:
        mm = RE_EUR_NUMBER.search(e)
        if not mm: continue
        try:
            val = float(_normalize_numstring(mm.group(0)))
            if val >= 10000:
                euros_filtered.append(e)
        except: continue
    if euros_filtered:
        return clean_price_string(euros_filtered[0])
    return ""

def extract_price(soup: BeautifulSoup, page_text: str) -> str:
    p = extract_price_near_objnr(soup);            if p: return p
    p = extract_price_from_objektangaben(soup);    if p: return p
    p = extract_price_from_jsonld(soup);           if p: return p
    for line in page_text.splitlines():
        m = RE_PRICE_LINE.search(line.strip())
        if m:
            return clean_price_string(m.group(2) + " €")
    p = extract_price_dom(soup);                   if p: return p
    p = extract_price_strict_top(page_text);       if p: return p
    euros = [e.group(0) for e in RE_EUR_ANY.finditer(page_text)]
    if euros:
        def to_float(e):
            mm = RE_EUR_NUMBER.search(e)
            if not mm: return 0.0
            try: return float(_normalize_numstring(mm.group(0)))
            except: return 0.0
        best = max(euros, key=to_float)
        return clean_price_string(best)
    return ""

# -------------------- BESCHREIBUNG – NUR TAB "BESCHREIBUNG" --------------------

SECTION_ALIASES = {
    "beschreibung": {"beschreibung"},
    "objektangaben": {"objektangaben","objektdaten","daten"},
    "ausstattung": {"ausstattung","merkmale"},
    "lage": {"lage","lagebeschreibung","umfeld"},
    "energieausweis": {"energieausweis","energie","energiekennwerte"},
}

# Filter für Telefonnummern, E-Mails, Teaserzeilen
RE_PHONE = re.compile(r"\b(?:\+?\d{2,3}[\s/.-]?)?(?:0\d|\d{2,3})[\d\s/.-]{5,}\b")
RE_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
RE_TEASER_LINE = re.compile(
    r"^\s*(?:\d{5}\s+[A-ZÄÖÜ][\wÄÖÜäöüß/ -]+|Kaufpreis\s+\d[\d\.\u00A0\u202F\u2009,]*\s*€|Details)\s*$",
    re.I,
)

def _clean_lines(lines):
    out, seen = [], set()
    for t in lines:
        if not t:
            continue
        t = _norm(t)
        if any(s.lower() in t.lower() for s in STOP_STRINGS):
            continue
        if RE_PHONE.search(t) or RE_EMAIL.search(t) or RE_TEASER_LINE.match(t):
            continue
        if t and t not in seen:
            out.append(t); seen.add(t)
    return out

def _find_expose_container(soup: BeautifulSoup) -> Tag:
    """Begrenzt die Suche auf den eigentlichen Exposé-Bereich (Vuetify)."""
    cand = soup.select_one(".v-expose")
    if cand: return cand
    cand = soup.select_one(".sw-vframe .v-expose")
    if cand: return cand
    for sel in ['div[id*="expose"]', 'section[id*="expose"]', '.immo-listing_infotext', '.expose', '.exposé']:
        cand = soup.select_one(sel)
        if cand: return cand
    return soup

def _text_from_container(container: Tag) -> list[str]:
    """Nimmt NUR Inhalte innerhalb des übergebenen Containers .v-card__text."""
    lines = []
    # direkte <p> (ohne Überschrift)
    for p in container.select("> p"):
        if "h4" in (p.get("class") or []):  # Überschrift selbst auslassen
            continue
        t = _norm(p.get_text(" ", strip=True))
        if t: lines.append(t)
    # Listen
    for li in container.select("ul li, ol li"):
        t = _norm(li.get_text(" ", strip=True))
        if t: lines.append("• " + t)
    # einfache Tabellen / DL (falls doch vorhanden)
    for tr in container.select("table tr"):
        cells = [_norm(c.get_text(" ", strip=True)) for c in tr.find_all(["th","td"])]
        if len(cells) >= 2: lines.append(f"- {cells[0]}: {cells[1]}")
        elif cells: lines.append(" ".join(cells))
    for dt in container.select("dl dt"):
        dd = dt.find_next_sibling("dd")
        k = _norm(dt.get_text(" ", strip=True))
        v = _norm(dd.get_text(" ", strip=True)) if dd else ""
        if k or v: lines.append(f"- {k}: {v}".strip(" -:"))
    return lines

def extract_description(soup: BeautifulSoup) -> str:
    """
    NUR den Text aus dem Tab 'Beschreibung' zurückgeben.
    Reihenfolge:
      1) Aktives Tab-Panel .v-window-item.v-window-item--active (wenn dort 'Beschreibung'-Box existiert)
      2) Panel mit id #tab-0
      3) Erster .v-card__text mit Überschrift 'Beschreibung' im Exposé-Scope
    Keine globalen Fallbacks – so vermeiden wir Fremdtexte sicher.
    """
    scope = _find_expose_container(soup)

    # 1) aktives Panel (meist 'Beschreibung' aktiv)
    active = scope.select_one(".v-tabs-items .v-window__container .v-window-item.v-window-item--active")
    if active:
        for box in active.select(".v-card__text"):
            head = box.select_one("p.h4")
            if head and _norm(head.get_text(" ", strip=True)).lower() == "beschreibung":
                lines = _clean_lines(_text_from_container(box))
                if lines:
                    return "\n".join(lines)[:6000]

    # 2) Panel #tab-0 (üblicherweise 'Beschreibung')
    tab0 = scope.select_one("#tab-0")
    if tab0:
        for box in tab0.select(".v-card__text"):
            head = box.select_one("p.h4")
            if head and _norm(head.get_text(" ", strip=True)).lower() == "beschreibung":
                lines = _clean_lines(_text_from_container(box))
                if lines:
                    return "\n".join(lines)[:6000]

    # 3) Fallback im Exposé-Scope
    for box in scope.select(".v-card__text"):
        head = box.select_one("p.h4")
        if head and _norm(head.get_text(" ", strip=True)).lower() == "beschreibung":
            lines = _clean_lines(_text_from_container(box))
            if lines:
                return "\n".join(lines)[:6000]

    return ""

# ---------------------------------------------------------------------------
# DETAIL-PARSER
# ---------------------------------------------------------------------------
def extract_plz_ort(page_text: str) -> str:
    m = RE_PLZ_ORT_STRICT.search(page_text)
    if not m: return ""
    plz, ort = m.group(1), m.group(2)
    ort = re.split(r"[|,•·\-\–—/()]", ort)[0].strip()
    ort = re.sub(r"\s{2,}", " ", ort)
    return f"{plz} {ort}"

def parse_detail(detail_url: str, mode: str):
    soup = soup_get(detail_url)
    page_text = soup.get_text("\n", strip=True)

    h1 = soup.select_one("h1")
    title = h1.get_text(strip=True) if h1 else ""

    description = extract_description(soup)

    m_obj = RE_OBJEKTNR.search(page_text)
    objektnummer = m_obj.group(1).strip() if m_obj else ""

    preis_value = extract_price(soup, page_text)

    m_plz = RE_PLZ_ORT_STRICT.search(page_text)
    ort = ""
    if m_plz:
        plz, name = m_plz.group(1), m_plz.group(2)
        name = re.split(r"[|,•·\-\–—/()]", name)[0].strip()
        name = re.sub(r"\s{2,}", " ", name)
        ort = f"{plz} {name}"

    image_url = ""
    a_img = soup.select_one('a[href*="immo."]') or soup.select_one('a[href*="screenwork"]')
    if a_img and a_img.has_attr("href"):
        image_url = a_img["href"]
    else:
        img = soup.find("img")
        if img and img.has_attr("src"):
            image_url = urljoin(BASE, img["src"])

    page_text_low = page_text.lower()
    kategorie_detected = "Mieten" if RE_MIETE.search(page_text_low) else "Kaufen"

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
# AIRTABLE HELPER
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
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{seg}"

def airtable_headers():
    if not AIRTABLE_TOKEN:
        raise RuntimeError("[Airtable] AIRTABLE_TOKEN fehlt.")
    return {"Authorization": f"Bearer {AIRTABLE_TOKEN}", "Content-Type": "application/json"}

def sanitize_record_for_airtable(record: dict) -> dict:
    """
    NICHT anhand dynamischer 'allowed_fields' filtern – alle Keys senden (bis auf leeren Preis).
    """
    out = dict(record)
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

# ---------------------------------------------------------------------------
# RECORD BUILDER
# ---------------------------------------------------------------------------
def make_record(row):
    preis_value = parse_price_to_number(row["Preis"])
    return {
        "Titel":        row["Titel"],
        "Kategorie":    row["KategorieDetected"],
        "Webseite":     row["URL"],
        "Objektnummer": row["Objektnummer"],
        "Beschreibung": row["Description"],
        "Bild":         row["Bild_URL"],
        "Preis":        preis_value,
        "Standort":     row["Ort"],
    }

# ---------------------------------------------------------------------------
# HAUPTLAUF
# ---------------------------------------------------------------------------
def run(mode="auto"):
    assert mode in ("kauf", "miete", "auto")
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
                # Skip „VERKAUFT“
                if re.search(r"\bverkauft\b", row.get("Titel", ""), re.IGNORECASE):
                    print(f"  - {j}/{len(new_links)} SKIPPED (verkauft) | {row.get('Titel','')[:70]}")
                    continue
                record = make_record(row)
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

    if mode == "kauf":  rows_miete = []
    if mode == "miete": rows_kauf  = []

    cols = ["Titel","Kategorie","Webseite","Objektnummer","Beschreibung","Bild","Preis","Standort"]
    if rows_kauf:
        with open(csv_kauf, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols); w.writeheader(); w.writerows(rows_kauf)
        print(f"[CSV] {csv_kauf}: {len(rows_kauf)} Zeilen")
    if rows_miete:
        with open(csv_miete, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols); w.writeheader(); w.writerows(rows_miete)
        print(f"[CSV] {csv_miete}: {len(rows_miete)} Zeilen")

    if AIRTABLE_TOKEN and AIRTABLE_BASE and (AIRTABLE_TABLE_ID or AIRTABLE_TABLE):
        if rows_kauf:
            airtable_batch_create(rows_kauf)
        if rows_miete:
            airtable_batch_create(rows_miete)
    else:
        print("[Airtable] ENV nicht gesetzt – Upload übersprungen.")

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    mode = (sys.argv[1].strip().lower() if len(sys.argv) > 1 else "auto")
    run(mode)
