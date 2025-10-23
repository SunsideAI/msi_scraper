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

# OpenAI (optional)
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
    MSI listet Kauf & Miete gemeinsam unter /kaufen/immobilienangebote/, paginiert mit /page/{n}/
    -> Für alle Modi dieselbe Listing-Quelle.
    """
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
# Regex / Konstanten
# ---------------------------------------------------------------------------
THIN_SPACES = "\u00A0\u202F\u2009"  # NBSP, NARROW NBSP, THIN SPACE

RE_EUR_NUMBER = re.compile(r"\b\d{1,3}(?:[.\u00A0\u202F\u2009]\d{3})*(?:,\d{2})?\b")
RE_EUR_ANY    = re.compile(r"\b\d{1,3}(?:[.\u00A0\u202F\u2009]\d{3})*(?:,\d{2})?\s*[€EUR]?")
RE_PRICE_LINE = re.compile(
    r"(kaufpreis|preis|kaltmiete|warmmiete|nettokaltmiete|miete)\s*:?\s*([0-9.\u00A0\u202F\u2009,]+)\s*[€]?",
    re.I
)
RE_OBJEKTNR       = re.compile(r"Objekt[-\s]?Nr\.?:\s*([A-Za-z0-9\-_/]+)")
RE_PLZ_ORT_STRICT = re.compile(r"\b(?!0{5})(\d{5})\s+([A-Za-zÄÖÜäöüß][A-Za-zÄÖÜäöüß\-\s]+?)\b(?![A-Za-zÄÖÜäöüß])")

RE_KAUF  = re.compile(r"\bzum\s*kauf\b", re.IGNORECASE)
RE_MIETE = re.compile(r"\bzur\s*miete\b|\b(kaltmiete|warmmiete|nettokaltmiete)\b", re.IGNORECASE)

STOP_STRINGS = ("Ihre Anfrage", "Kontakt", "Exposé anfordern", "Neueste Immobilien")

TAB_LABELS = {
    "Beschreibung":   ("Beschreibung",),
    "Objektangaben":  ("Objektangaben","Objektdaten","Daten"),
    "Ausstattung":    ("Ausstattung","Merkmale"),
    "Lage":           ("Lage","Lagebeschreibung","Umfeld"),
    "Energieausweis": ("Energieausweis","Energie","Energiekennwerte"),
}

SECTION_HEADERS = ("beschreibung", "objektbeschreibung", "ausstattung", "lage", "sonstiges", "weitere informationen")

# ---------------------------------------------------------------------------
# Utils
# ---------------------------------------------------------------------------
def _norm(s: str) -> str:
    return re.sub(r"\s{2,}", " ", (s or "").strip())

def detect_category(page_text: str) -> str:
    if RE_MIETE.search(page_text): return "Mieten"
    if RE_KAUF.search(page_text):  return "Kaufen"
    return "Kaufen"

# -------------------- Preis-Parsing --------------------
def _normalize_numstring(s: str) -> str:
    """Entfernt Tausender (., NBSP, NARROW NBSP, THIN SPACE) und setzt Dezimal-Komma zu Punkt."""
    if not s: return ""
    # Spezial-Leerzeichen raus
    for ch in THIN_SPACES:
        s = s.replace(ch, "")
    s = s.strip()
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")  # 1.234.567,89 -> 1234567.89
    elif "," in s:
        s = s.replace(",", ".")                    # 123456,78 -> 123456.78
    else:
        if "." in s:
            last = s.rsplit(".", 1)[-1]
            if last.isdigit() and len(last) in (3, 6):  # 1.234 / 1.234.567
                s = s.replace(".", "")
    return s

def clean_price_string(raw: str) -> str:
    """Gibt einen hübsch formatierten Preis-String zurück, z. B. '479.000 €'."""
    if not raw: return ""
    m = RE_EUR_NUMBER.search(raw)
    if not m: return ""
    num = _normalize_numstring(m.group(0))
    try:
        val = float(num)
    except Exception:
        return ""
    return f"{val:,.0f} €".replace(",", ".")

def parse_price_to_number(label: str):
    """Wandelt beliebige Preis-Strings robust zu float (Euro)."""
    if not label: return None
    m = RE_EUR_NUMBER.search(label)
    if not m: return None
    num = _normalize_numstring(m.group(0))
    try:
        return float(num)
    except Exception:
        return None

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
    # Kadence / Elementor / generisch
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
        if any(alias.lower() in label.lower() for alias in TAB_LABELS["Objektangaben"]):
            target_panel = panel
            break
    if not target_panel:
        for tbl in soup.select("table"):
            if "kaufpreis" in tbl.get_text(" ", strip=True).lower():
                target_panel = tbl
                break
    if not target_panel:
        return ""

    keys = ("kaufpreis","kaltmiete","warmmiete","nettokaltmiete","miete","preis")
    for tr in target_panel.select("tr"):
        cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th","td"])]
        if len(cells) >= 2 and any(k in cells[0].lower() for k in keys):
            got = clean_price_string(cells[1])
            if got: return got
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
                got = clean_price_string(cells[1])
                if got: return got
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
            if val >= 10000: euros_filtered.append(e)
        except: continue
    if euros_filtered:
        return clean_price_string(euros_filtered[0])
    return ""

def extract_price_near_objnr(soup: BeautifulSoup) -> str:
    """
    Sucht zuerst in unmittelbarer Nähe von 'Objekt-Nr' nach dem Preis
    (Kopfbereich wie im Screenshot).
    """
    obj_nodes = soup.find_all(string=re.compile(r"Objekt[-\s]?Nr", re.I))
    for txtnode in obj_nodes:
        container = txtnode
        # etwas hochklettern, um den visuellen Kopf-Block zu erwischen
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

def extract_price(soup: BeautifulSoup, page_text: str) -> str:
    # 0) Kopfbereich nahe 'Objekt-Nr.'
    p = extract_price_near_objnr(soup)
    if p: return p
    # 1) Objektangaben-Panel
    p = extract_price_from_objektangaben(soup)
    if p: return p
    # 2) JSON-LD
    p = extract_price_from_jsonld(soup)
    if p: return p
    # 3) Zeilenweise Text
    for line in page_text.splitlines():
        m = RE_PRICE_LINE.search(line.strip())
        if m:
            return clean_price_string(m.group(2) + " €")
    # 4) DOM global
    p = extract_price_dom(soup)
    if p: return p
    # 5) oberer Seitenbereich
    p = extract_price_strict_top(page_text)
    if p: return p
    # 6) größte Euro-Zahl
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

# ---------------------------------------------------------------------------
# Beschreibung aus Tabs/Boxen
# ---------------------------------------------------------------------------
def _is_stop_node(node):
    cls = " ".join(node.get("class", []))
    if any(k in cls for k in ("contact", "form", "sidebar", "widget")):
        return True
    txt = node.get_text(" ", strip=True) if hasattr(node, "get_text") else ""
    return any(stop in txt for stop in STOP_STRINGS)

def _harvest_panel(panel):
    lines = []
    for p in panel.select("p"):
        t = _norm(p.get_text(" ", strip=True))
        if t and not any(stop in t for stop in STOP_STRINGS):
            lines.append(t)
    for li in panel.select("ul li, ol li"):
        t = _norm(li.get_text(" ", strip=True))
        if t: lines.append(f"• {t}")
    for tr in panel.select("table tr"):
        cells = [_norm(c.get_text(" ", strip=True)) for c in tr.find_all(["th","td"])]
        if len(cells) >= 2:
            if cells[0] or cells[1]:
                lines.append(f"- {cells[0]}: {cells[1]}")
        elif cells:
            lines.append(" ".join(cells))
    for dt in panel.select("dl dt"):
        dd = dt.find_next_sibling("dd")
        k = _norm(dt.get_text(" ", strip=True))
        v = _norm(dd.get_text(" ", strip=True)) if dd else ""
        if k or v: lines.append(f"- {k}: {v}".strip(" -:"))
    out, seen = [], set()
    for ln in lines:
        if ln and ln not in seen:
            out.append(ln); seen.add(ln)
        if len(out) >= 200: break
    return out

def _collect_tabbed_sections(soup):
    parts = []
    # A) Alle Panels mit role="tabpanel" oder Kadence-Container
    panels = soup.select('[role="tabpanel"], .kt-tabs-content-wrap > *')
    label_map = {}
    if panels:
        nav_labels = {}
        for a in soup.select('a[role="tab"], .kt-tabs-title-list a, .elementor-tab-title'):
            lab = _norm(getattr(a, "get_text", lambda *args, **kw: "")(" ", strip=True))
            target = a.get("href") or a.get("data-tab") or a.get("aria-controls") or ""
            if target and target.startswith("#"):
                nav_labels[target[1:]] = lab
            elif target:
                nav_labels[str(target)] = lab
        for p in panels:
            pid = p.get("id", "")
            label = nav_labels.get(pid, "").lower()
            txt_label = ""
            for nice, aliases in TAB_LABELS.items():
                if label and any(a.lower() in label for a in aliases):
                    txt_label = nice
                    break
            if not txt_label:
                content_preview = p.get_text(" ", strip=True).lower()
                for nice, aliases in TAB_LABELS.items():
                    if any(a.lower() in content_preview for a in aliases):
                        txt_label = nice
                        break
            lines = _harvest_panel(p)
            if lines and txt_label:
                label_map.setdefault(txt_label, []).extend(lines)
    if label_map:
        for order in ("Beschreibung", "Objektangaben", "Ausstattung", "Lage", "Energieausweis"):
            if order in label_map and label_map[order]:
                parts.append(f"{order}:\n" + "\n".join(label_map[order]))
    return parts

def _find_box_heading(soup, text_candidates):
    # Suche h2/h3/h4 oder a/div/span mit genau diesem Text
    for tag in soup.find_all(True):
        try:
            txt = _norm(tag.get_text(" ", strip=True))
        except Exception:
            continue
        if txt and any(txt.lower() == c.lower() for c in text_candidates):
            return tag
    return None

def _collect_box_after_heading(start_el):
    if not start_el:
        return []
    lines = []
    for sib in start_el.find_all_next():
        if sib.name in ("h2","h3","h4"): break
        if sib.has_attr("class") and any(k in " ".join(sib.get("class")) for k in ("kt-tabs", "elementor-tabs", "sidebar", "widget")):
            break
        lines.extend(_harvest_panel(sib))
        if len(lines) >= 200: break
    out, seen = [], set()
    for ln in lines:
        if ln and ln not in seen:
            out.append(ln); seen.add(ln)
    return out

def extract_description(soup: BeautifulSoup) -> str:
    """
    Baut die Beschreibung aus *allen* 5 Bereichen:
    Tabs (Kadence/Elementor): Beschreibung, Objektangaben, Ausstattung, Lage, Energieausweis
    Fallback: Boxen mit denselben Überschriften
    Danach weitere Fallbacks.
    """
    parts = _collect_tabbed_sections(soup)

    if not parts:
        for nice, aliases in TAB_LABELS.items():
            hdr = _find_box_heading(soup, aliases)
            if hdr:
                lines = _collect_box_after_heading(hdr)
                if lines:
                    parts.append(f"{nice}:\n" + "\n".join(lines))

    if parts:
        return ("\n\n".join(parts).strip())[:6000]

    # Fallbacks:
    for c in soup.select(".entry-content, article, .content, .post-content, .single-content, [class*='content'], [class*='text']"):
        ps = [p.get
