# msi_v2_airtable_replace.py
# -*- coding: utf-8 -*-
import sys
import time
import csv
import os
import json
import requests
import re
from urllib.parse import quote, urljoin
from bs4 import BeautifulSoup, Tag

# ===========================================================================
# KONFIGURATION / ENV
# ===========================================================================
BASE = "https://www.msi-hessen.de"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
}

# Airtable ENV
AIRTABLE_TOKEN    = os.getenv("AIRTABLE_TOKEN", "").strip()
AIRTABLE_BASE     = os.getenv("AIRTABLE_BASE",  "").strip()
AIRTABLE_TABLE    = os.getenv("AIRTABLE_TABLE", "").strip()    # optional (Name)
AIRTABLE_TABLE_ID = os.getenv("AIRTABLE_TABLE_ID", "").strip() # bevorzugt (tbl...)
AIRTABLE_VIEW     = os.getenv("AIRTABLE_VIEW", "").strip()     # optional

# OpenAI für Kurzbeschreibung
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()

# Rendering ENV (optional)
#   export MSI_RENDER=1
#   export MSI_RENDER_ENGINE=playwright  # oder 'requests_html'
#   export MSI_RENDER_TIMEOUT=20000
MSI_RENDER         = os.getenv("MSI_RENDER", "0").strip() == "1"
MSI_RENDER_ENGINE  = os.getenv("MSI_RENDER_ENGINE", "playwright").strip().lower()
MSI_RENDER_TIMEOUT = int(os.getenv("MSI_RENDER_TIMEOUT", "20000"))

print(f"[CFG] MSI_RENDER={MSI_RENDER} | ENGINE={MSI_RENDER_ENGINE} | TIMEOUT={MSI_RENDER_TIMEOUT}")

# iFrame-DOMs zwischenspeichern (Haupt-URL -> [frame_html1, ...])
FRAME_HTMLS: dict[str, list[str]] = {}

# ===========================================================================
# REGEX & KONSTANTEN
# ===========================================================================
THIN_SPACES = "\u00A0\u202F\u2009"
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

STOP_STRINGS = (
    "Ihre Anfrage", "Exposé anfordern", "Neueste Immobilien", "Teilen auf",
    "Datenschutz", "Impressum", "designed by wavepoint",
    "Ansprechpartner", "Kontaktieren Sie uns", "Zur Objektanfrage",
    "msi-hessen.de"
)

# Kontakt rausfiltern
RE_PHONE = re.compile(r"\b(?:\+?\d{1,3}[\s/.-]?)?(?:0\d|\d{2,3})[\d\s/.-]{6,}\b")
RE_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")

# ===========================================================================
# HTTP / HTML – immer rendern, wenn MSI_RENDER=1 (inkl. iFrames)
# ===========================================================================
def _simple_fetch(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def _render_with_playwright(url: str, timeout_ms: int) -> tuple[str, list[str]]:
    """Headless-Rendering via Playwright. Liefert (main_html, [frame_htmls...])."""
    from playwright.sync_api import sync_playwright
    frame_htmls = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(locale="de-DE", user_agent=HEADERS["User-Agent"])
        page = context.new_page()
        page.set_default_timeout(timeout_ms)
        page.goto(url, wait_until="domcontentloaded")

        # Tabs/Exposé im DOM?
        for sel in [".sw-yframe .sw-vframe .v-expose .v-tabs-items",
                    ".v-expose .v-tabs-items",
                    ".v-expose"]:
            try:
                page.wait_for_selector(sel, state="attached", timeout=timeout_ms//2)
                break
            except Exception:
                continue

        # „Beschreibung"-Tab aktivieren
        try:
            for t in page.query_selector_all(".v-tab"):
                if "Beschreibung" in (t.inner_text() or ""):
                    t.click()
                    break
        except Exception:
            pass

        # Auf Inhalt warten
        try:
            page.wait_for_selector(
                ".v-expose .v-tabs-items .v-window__container #tab-0 .v-card__text p:not(.h4), "
                ".v-expose .v-tabs-items .v-window__container .v-window-item.v-window-item--active .v-card__text p:not(.h4)",
                state="visible",
                timeout=timeout_ms//2
            )
        except Exception:
            time.sleep(0.8)

        # iFrames auslesen (screenwork/immo)
        try:
            for fr in page.frames:
                fr_url = (fr.url or "").lower()
                if any(k in fr_url for k in ("screenwork", "immo", "expose", "angebote")):
                    try:
                        frame_htmls.append(fr.content())
                    except Exception:
                        pass
        except Exception:
            pass

        try:
            page.wait_for_load_state("networkidle", timeout=timeout_ms//2)
        except Exception:
            pass

        main_html = page.content()
        context.close()
        browser.close()
        return main_html, frame_htmls

def _render_with_requests_html(url: str, timeout_ms: int) -> tuple[str, list[str]]:
    """Einfacher Renderer (iFrames i.d.R. nicht verfügbar)."""
    from requests_html import HTMLSession
    s = HTMLSession()
    r = s.get(url, headers=HEADERS, timeout=30)
    r.html.render(timeout=timeout_ms/1000.0, reload=False, scrolldown=0)
    return r.html.html, []

def soup_get(url: str) -> BeautifulSoup:
    """Wenn MSI_RENDER=1 → rendern (Playwright bevorzugt) und iFrame-HTMLs in FRAME_HTMLS[url] sichern."""
    if not MSI_RENDER:
        return _simple_fetch(url)

    try:
        if MSI_RENDER_ENGINE == "requests_html":
            main_html, frames = _render_with_requests_html(url, MSI_RENDER_TIMEOUT)
        else:
            main_html, frames = _render_with_playwright(url, MSI_RENDER_TIMEOUT)
        FRAME_HTMLS[url] = frames or []
        if main_html:
            soup = BeautifulSoup(main_html, "lxml")
            print(f"[RENDER] ok ({MSI_RENDER_ENGINE}) | frames={len(frames)}")
            return soup
    except Exception as e:
        print(f"[RENDER] Fehler ({MSI_RENDER_ENGINE}): {e}")

    print("[RENDER] Fallback: simple fetch (keine Frames)")
    FRAME_HTMLS[url] = []
    return _simple_fetch(url)

# ===========================================================================
# LISTEN / LINKS
# ===========================================================================
def get_list_page_urls(mode: str, max_pages: int = 50):
    """MSI listet Kauf & Miete gemeinsam unter /kaufen/immobilienangebote/, paginiert mit /page/{n}/"""
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

# ===========================================================================
# UTILS
# ===========================================================================
def _norm(s: str) -> str:
    return re.sub(r"\s{2,}", " ", (s or "").strip())

def detect_category(page_text: str) -> str:
    if RE_MIETE.search(page_text): return "Mieten"
    if RE_KAUF.search(page_text):  return "Kaufen"
    return "Kaufen"

# ===========================================================================
# PREIS
# ===========================================================================
def _normalize_numstring(s: str) -> str:
    if not s: return ""
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
    if not label: return None
    m = RE_EUR_NUMBER.search(label)
    if not m: return None
    num = _normalize_numstring(m.group(0))
    try:
        return float(num)
    except Exception:
        return None

# ===========================================================================
# PREIS-EXTRAKTION (versch. Quellen)
# ===========================================================================
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
                if got:
                    return got
    for tr in soup.select("table tr"):
        cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th","td"])]
        if len(cells) >= 2:
            label = cells[0].lower()
            if any(k in label for k in ("kaufpreis","kaltmiete","warmmiete","nettokaltmiete","miete","preis")):
                got = clean_price_string(cells[1])
                if got:
                    return got
    for li in soup.select("li"):
        txt = li.get_text(" ", strip=True)
        m = RE_PRICE_LINE.search(txt)
        if m:
            return clean_price_string(m.group(2) + " €")
    return ""

def extract_price_strict_top(page_text: str) -> str:
    euros = [e.group(0) for e in RE_EUR_ANY.finditer(page_text)]
    euros_filtered = []
    for e in euros:
        mm = RE_EUR_NUMBER.search(e)
        if not mm:
            continue
        try:
            val = float(_normalize_numstring(mm.group(0)))
            if val >= 10000:
                euros_filtered.append(e)
        except:
            continue
    if euros_filtered:
        return clean_price_string(euros_filtered[0])
    return ""

def extract_price(soup: BeautifulSoup, page_text: str) -> str:
    p = extract_price_from_jsonld(soup)
    if p:
        return p
    p = extract_price_dom(soup)
    if p:
        return p
    p = extract_price_strict_top(page_text)
    if p:
        return p
    euros = [e.group(0) for e in RE_EUR_ANY.finditer(page_text)]
    if euros:
        def to_float(e):
            mm = RE_EUR_NUMBER.search(e)
            if not mm:
                return 0.0
            try:
                return float(_normalize_numstring(mm.group(0)))
            except:
                return 0.0
        best = max(euros, key=to_float)
        return clean_price_string(best)
    return ""

# ===========================================================================
# BESCHREIBUNG – NUR TAB "BESCHREIBUNG"
# ===========================================================================
def _clean_desc_lines(lines):
    out, seen = [], set()
    for t in lines:
        t = _norm(t)
        if not t:
            continue
        if any(s.lower() in t.lower() for s in STOP_STRINGS):
            continue
        if RE_PHONE.search(t) or RE_EMAIL.search(t):
            continue
        if t not in seen:
            out.append(t)
            seen.add(t)
    return out

def _find_expose_scope(soup: BeautifulSoup) -> Tag:
    scope = soup.select_one(".sw-yframe .sw-vframe .v-expose")
    if scope:
        return scope
    return soup.select_one(".v-expose") or soup

def extract_description(soup: BeautifulSoup) -> str:
    """
    Nur den Inhalt des Tabs 'Beschreibung' liefern.
    - scope: .sw-yframe .sw-vframe .v-expose → .v-expose → soup
    - Panel: #tab-0 → aktives .v-window-item.v-window-item--active
    - Container: .v-card__text; sammle alle p:not(.h4) + li
    """
    root = _find_expose_scope(soup)

    candidates = []
    tab0 = root.select_one(".v-tabs-items .v-window__container #tab-0")
    if tab0:
        candidates.append(tab0)
    active = root.select_one(".v-tabs-items .v-window__container .v-window-item.v-window-item--active")
    if active and active not in candidates:
        candidates.append(active)

    if not candidates:
        for box in root.select(".v-card__text"):
            head = box.select_one("p.h4, h4")
            if head and _norm(head.get_text(" ", strip=True)).lower() == "beschreibung":
                candidates.append(box)

    for node in candidates:
        box = node.select_one(".v-card .v-card__text") or node.select_one(".v-card__text") or node
        lines = []
        head = box.select_one("p.h4, h4")
        for p in box.select("p"):
            if p is head or ("h4" in (p.get("class") or [])):
                continue
            txt = _norm(p.get_text(" ", strip=True))
            if txt:
                lines.append(txt)
        for li in box.select("ul li, ol li"):
            t = _norm(li.get_text(" ", strip=True))
            if t:
                lines.append("• " + t)

        lines = _clean_desc_lines(lines)
        if lines:
            return ("\n".join(lines))[:6000]

    return ""

def _find_screenwork_links(soup: BeautifulSoup) -> list[str]:
    """Sucht direkte Links/iframes auf Screenwork/Immo-Exposés."""
    urls = set()
    # iframes
    for ifr in soup.select("iframe[src]"):
        src = (ifr.get("src") or "").strip()
        if any(host in src for host in ("screenwork", "immo")):
            urls.add(src)
    # anchor links
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if any(host in href for host in ("screenwork", "immo")):
            urls.add(href)
    return list(urls)

def get_description(detail_url: str, main_soup: BeautifulSoup) -> str:
    """
    Priorität:
      1) Haupt-DOM (gerendert)
      2) alle gerenderten iFrames (FRAME_HTMLS)
      3) DIREKTER Abruf verlinkter screenwork/immo-HTML-Seiten (ohne Rendering)
    """
    # 1) Haupt-DOM
    desc = extract_description(main_soup)
    if desc:
        return desc

    # Objekt-ID für Debug
    obj = ""
    m = RE_OBJEKTNR.search(main_soup.get_text(" ", strip=True))
    if m:
        obj = m.group(1)

    # 2) iFrames aus dem Renderer
    frames = FRAME_HTMLS.get(detail_url) or []
    if frames:
        for i, html in enumerate(frames):
            try:
                s = BeautifulSoup(html, "lxml")
                desc = extract_description(s)
                if desc:
                    return desc
            except Exception:
                pass
        # wenn nichts gefunden: Debug dumpen (nur erster Frame, um Artefakte klein zu halten)
        try:
            with open(f"debug_{obj or 'na'}_frame0.html", "w", encoding="utf-8") as f:
                f.write(frames[0])
            print(f"[DEBUG] wrote debug_{obj or 'na'}_frame0.html")
        except Exception:
            pass

    # 3) Direkte screenwork/immo-HTMLs (per requests)
    try:
        for sw_url in _find_screenwork_links(main_soup):
            try:
                if not sw_url.startswith("http"):
                    sw_url = urljoin(detail_url, sw_url)
                r = requests.get(sw_url, headers=HEADERS, timeout=30)
                if r.ok and r.text:
                    s2 = BeautifulSoup(r.text, "lxml")
                    desc = extract_description(s2)
                    if desc:
                        return desc
            except Exception:
                continue
    except Exception:
        pass

    # Letzter Debug-Dump vom Haupt-DOM
    try:
        with open(f"debug_{obj or 'na'}_main.html", "w", encoding="utf-8") as f:
            f.write(str(main_soup))
        print(f"[DEBUG] wrote debug_{obj or 'na'}_main.html")
    except Exception:
        pass

    return ""

# ===========================================================================
# ZUSÄTZLICHE DATEN EXTRAKTION
# ===========================================================================
def extract_additional_data(page_text: str) -> dict:
    """Extrahiere zusätzliche Daten für die Kurzbeschreibung"""
    data = {
        "zimmer": "",
        "wohnflaeche": "",
        "grundstueck": "",
        "baujahr": ""
    }
    
    # Zimmer extrahieren
    zimmer_patterns = [
        r"(\d+)\s*Zimmer",
        r"Zimmer[:\s]+(\d+)",
        r"(\d+)-Zimmer",
    ]
    for pattern in zimmer_patterns:
        m = re.search(pattern, page_text, re.IGNORECASE)
        if m:
            data["zimmer"] = m.group(1)
            break
    
    # Wohnfläche extrahieren
    wohnflaeche_patterns = [
        r"(?:ca\.\s*)?(\d+(?:[.,]\d+)?)\s*m²\s*Wohnfläche",
        r"Wohnfläche[:\s]+(?:ca\.\s*)?(\d+(?:[.,]\d+)?)\s*m²",
        r"(\d+(?:[.,]\d+)?)\s*m²\s*Wohnfl",
    ]
    for pattern in wohnflaeche_patterns:
        m = re.search(pattern, page_text, re.IGNORECASE)
        if m:
            data["wohnflaeche"] = m.group(1).replace(",", ".")
            break
    
    # Grundstück extrahieren
    grundstueck_patterns = [
        r"(?:ca\.\s*)?(\d+(?:[.,]\d+)?)\s*m²\s*Grundstück",
        r"Grundstück[:\s]+(?:ca\.\s*)?(\d+(?:[.,]\d+)?)\s*m²",
        r"(\d+(?:[.,]\d+)?)\s*m²\s*(?:großes?\s+)?Grundstück",
    ]
    for pattern in grundstueck_patterns:
        m = re.search(pattern, page_text, re.IGNORECASE)
        if m:
            data["grundstueck"] = m.group(1).replace(",", ".")
            break
    
    # Baujahr extrahieren
    baujahr_patterns = [
        r"Baujahr[:\s]+(\d{4})",
        r"aus\s+(?:dem\s+)?(?:Baujahr\s+)?(\d{4})",
        r"(\d{4})\s+(?:erbaut|gebaut)",
    ]
    for pattern in baujahr_patterns:
        m = re.search(pattern, page_text, re.IGNORECASE)
        if m:
            jahr = int(m.group(1))
            if 1800 <= jahr <= 2030:
                data["baujahr"] = str(jahr)
                break
    
    return data

# ===========================================================================
# GPT KURZBESCHREIBUNG MIT CACHING
# ===========================================================================

# Cache für existierende Kurzbeschreibungen (wird beim Start gefüllt)
KURZBESCHREIBUNG_CACHE = {}  # {objektnummer: kurzbeschreibung}

def load_kurzbeschreibung_cache():
    """Lädt existierende Kurzbeschreibungen aus Airtable in den Cache"""
    global KURZBESCHREIBUNG_CACHE
    
    if not (AIRTABLE_TOKEN and AIRTABLE_BASE and airtable_table_segment()):
        print("[CACHE] Airtable nicht konfiguriert - Cache leer")
        return
    
    try:
        all_ids, all_fields = airtable_list_all()
        for fields in all_fields:
            obj_nr = fields.get("Objektnummer", "").strip()
            kurzbeschreibung = fields.get("Kurzbeschreibung", "").strip()
            if obj_nr and kurzbeschreibung:
                KURZBESCHREIBUNG_CACHE[obj_nr] = kurzbeschreibung
        
        print(f"[CACHE] {len(KURZBESCHREIBUNG_CACHE)} Kurzbeschreibungen aus Airtable geladen")
    except Exception as e:
        print(f"[CACHE] Fehler beim Laden: {e}")

def get_cached_kurzbeschreibung(objektnummer: str) -> str:
    """Holt Kurzbeschreibung aus Cache wenn vorhanden"""
    return KURZBESCHREIBUNG_CACHE.get(objektnummer, "")

# Einheitliche Feldstruktur für Kurzbeschreibung
KURZBESCHREIBUNG_FIELDS = [
    "Objekttyp",
    "Zimmer", 
    "Schlafzimmer",
    "Wohnfläche",
    "Grundstück",
    "Baujahr",
    "Kategorie",
    "Preis",
    "Standort",
    "Energieeffizienz",
    "Besonderheiten"
]

def normalize_kurzbeschreibung(gpt_output: str, scraped_data: dict) -> str:
    """
    Normalisiert die GPT-Ausgabe und füllt fehlende Felder mit Scrape-Daten oder '-'.
    Stellt einheitliche Struktur sicher.
    """
    # Parse GPT Output in Dictionary
    parsed = {}
    for line in gpt_output.strip().split("\n"):
        if ":" in line:
            key, value = line.split(":", 1)
            key = key.strip()
            value = value.strip()
            if value and value != "-":
                parsed[key] = value
    
    # Mapping von Scrape-Feldern zu Kurzbeschreibung-Feldern
    scrape_mapping = {
        "Zimmer": "zimmer",
        "Wohnfläche": "wohnflaeche", 
        "Grundstück": "grundstueck",
        "Baujahr": "baujahr",
        "Kategorie": "kategorie",
        "Preis": "preis",
        "Standort": "standort",
    }
    
    # Fülle fehlende Felder aus Scrape-Daten
    for field, scrape_key in scrape_mapping.items():
        if field not in parsed or not parsed[field] or parsed[field] == "-":
            scrape_value = scraped_data.get(scrape_key, "")
            if scrape_value:
                # Formatiere Preis
                if field == "Preis" and scrape_value:
                    try:
                        preis_num = float(str(scrape_value).replace(".", "").replace(",", ".").replace("€", "").strip())
                        parsed[field] = f"{int(preis_num):,} €".replace(",", ".")
                    except:
                        parsed[field] = str(scrape_value)
                # Formatiere Wohnfläche
                elif field == "Wohnfläche" and scrape_value:
                    if "m²" not in str(scrape_value):
                        parsed[field] = f"{scrape_value} m²"
                    else:
                        parsed[field] = str(scrape_value)
                # Formatiere Grundstück
                elif field == "Grundstück" and scrape_value:
                    if "m²" not in str(scrape_value):
                        parsed[field] = f"{scrape_value} m²"
                    else:
                        parsed[field] = str(scrape_value)
                else:
                    parsed[field] = str(scrape_value)
    
    # Baue einheitliche Ausgabe mit allen Feldern
    output_lines = []
    for field in KURZBESCHREIBUNG_FIELDS:
        value = parsed.get(field, "-")
        if not value or value.strip() == "":
            value = "-"
        output_lines.append(f"{field}: {value}")
    
    return "\n".join(output_lines)

def generate_kurzbeschreibung(beschreibung: str, titel: str, kategorie: str, preis: str, ort: str,
                               zimmer: str = "", wohnflaeche: str = "", grundstueck: str = "", baujahr: str = "",
                               objektnummer: str = "") -> str:
    """
    Generiert eine strukturierte Kurzbeschreibung mit GPT für die KI-Suche.
    Format ist optimiert für Regex/KI-Matching im Chatbot.
    Fehlende Felder werden aus Scrape-Daten ergänzt oder mit '-' gefüllt.
    
    OPTIMIERUNG: Wenn bereits eine Kurzbeschreibung in Airtable existiert, wird diese verwendet.
    """
    
    # CACHE CHECK: Wenn bereits vorhanden, nicht neu generieren!
    if objektnummer:
        cached = get_cached_kurzbeschreibung(objektnummer)
        if cached:
            print(f"[CACHE] Kurzbeschreibung aus Cache verwendet für {objektnummer[:30]}...")
            return cached
    
    # Scrape-Daten für Fallback sammeln
    scraped_data = {
        "kategorie": kategorie,
        "preis": preis,
        "standort": ort,
        "zimmer": zimmer,
        "wohnflaeche": wohnflaeche,
        "grundstueck": grundstueck,
        "baujahr": baujahr,
    }
    
    if not OPENAI_API_KEY:
        print("[WARN] OPENAI_API_KEY nicht gesetzt - erstelle Kurzbeschreibung aus Scrape-Daten")
        # Fallback: Erstelle Kurzbeschreibung nur aus Scrape-Daten
        return normalize_kurzbeschreibung("", scraped_data)
    
    # Baue zusätzliche Daten-Sektion für GPT
    zusatz_daten = []
    if zimmer:
        zusatz_daten.append(f"Zimmer: {zimmer}")
    if wohnflaeche:
        zusatz_daten.append(f"Wohnfläche: {wohnflaeche}")
    if grundstueck:
        zusatz_daten.append(f"Grundstück: {grundstueck}")
    if baujahr:
        zusatz_daten.append(f"Baujahr: {baujahr}")
    
    zusatz_text = "\n".join(zusatz_daten) if zusatz_daten else "Keine zusätzlichen Daten"
    
    prompt = f"""Analysiere diese Immobilienanzeige und erstelle eine strukturierte Kurzbeschreibung für eine Suchfunktion.

TITEL: {titel}
KATEGORIE: {kategorie}
PREIS: {preis if preis else 'Nicht angegeben'}
STANDORT: {ort if ort else 'Nicht angegeben'}

ZUSÄTZLICHE DATEN (aus Scraping):
{zusatz_text}

BESCHREIBUNG:
{beschreibung[:3000]}

Erstelle eine Kurzbeschreibung EXAKT in diesem Format (ALLE Felder müssen vorhanden sein, nutze "-" wenn unbekannt):

Objekttyp: [Einfamilienhaus/Mehrfamilienhaus/Eigentumswohnung/Baugrundstück/Reihenhaus/Doppelhaushälfte/Wohnung/etc. oder "-"]
Zimmer: [Anzahl oder "-"]
Schlafzimmer: [Anzahl oder "-"]
Wohnfläche: [X m² oder "-"]
Grundstück: [X m² oder "-"]
Baujahr: [Jahr oder "-"]
Kategorie: [Kaufen/Mieten]
Preis: [Preis in € oder "-"]
Standort: [PLZ Ort oder "-"]
Energieeffizienz: [Klasse A+ bis H oder "-"]
Besonderheiten: [Kommaseparierte Liste oder "-"]

WICHTIG: 
- ALLE 11 Felder MÜSSEN in der Ausgabe sein
- Nutze "-" für unbekannte/fehlende Werte
- Nutze die ZUSÄTZLICHEN DATEN wenn die Beschreibung keine Info enthält
- Zahlen ohne "ca." (z.B. "180 m²" statt "ca. 180 m²")
- Preis im Format "XXX.XXX €" """

    try:
        headers = {
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": "Du bist ein Experte für Immobilienanalyse. Erstelle präzise, strukturierte Kurzbeschreibungen. Halte dich EXAKT an das vorgegebene Format."},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 500,
            "temperature": 0.1
        }
        
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json=payload,
            timeout=30
        )
        response.raise_for_status()
        
        result = response.json()
        gpt_output = result["choices"][0]["message"]["content"].strip()
        
        # Normalisiere und fülle fehlende Felder
        kurzbeschreibung = normalize_kurzbeschreibung(gpt_output, scraped_data)
        
        print(f"[GPT] Kurzbeschreibung generiert und normalisiert ({len(kurzbeschreibung)} Zeichen)")
        return kurzbeschreibung
        
    except Exception as e:
        print(f"[ERROR] GPT Kurzbeschreibung fehlgeschlagen: {e}")
        # Fallback: Erstelle aus Scrape-Daten
        return normalize_kurzbeschreibung("", scraped_data)

# ===========================================================================
# DETAIL-PARSER
# ===========================================================================
def extract_plz_ort(page_text: str) -> str:
    m = RE_PLZ_ORT_STRICT.search(page_text)
    if not m:
        return ""
    plz, ort = m.group(1), m.group(2)
    ort = re.split(r"[|,•·\-\–—/()]", ort)[0].strip()
    ort = re.sub(r"\s{2,}", " ", ort)
    return f"{plz} {ort}"

def parse_detail(detail_url: str, mode: str):
    soup = soup_get(detail_url)
    page_text = soup.get_text("\n", strip=True)

    h1 = soup.select_one("h1")
    title = h1.get_text(strip=True) if h1 else ""

    # **WICHTIG: Beschreibung über Haupt- und iFrame-/Screenwork-DOMs holen**
    description = get_description(detail_url, soup)

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
    
    # Zusätzliche Daten extrahieren
    additional_data = extract_additional_data(page_text)

    return {
        "Titel":        title,
        "URL":          detail_url,
        "Description":  description,
        "Objektnummer": objektnummer,
        "Preis":        preis_value,
        "Ort":          ort,
        "Bild_URL":     image_url,
        "KategorieDetected": kategorie_detected,
        "AdditionalData": additional_data,
    }

# ===========================================================================
# AIRTABLE – HELFER
# ===========================================================================
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

def sanitize_record_for_airtable(record: dict, allowed_fields: set = None) -> dict:
    # Felder die immer erlaubt sind (auch wenn sie in bestehenden Records leer sind)
    ALWAYS_ALLOWED = {"Kurzbeschreibung"}
    
    out = dict(record)
    if "Preis" in out and (out["Preis"] is None or out["Preis"] == ""):
        out.pop("Preis", None)
    
    # Wenn allowed_fields gesetzt, filtere - aber behalte ALWAYS_ALLOWED
    if allowed_fields:
        all_allowed = allowed_fields | ALWAYS_ALLOWED
        out = {k: v for k, v in out.items() if k in all_allowed}
    
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

def unique_key(fields: dict) -> str:
    obj = (fields.get("Objektnummer") or "").strip()
    if obj:
        return f"obj:{obj}"
    url = (fields.get("Webseite") or "").strip()
    if url:
        return f"url:{url}"
    return f"hash:{hash(json.dumps(fields, sort_keys=True))}"

# ===========================================================================
# RECORD BUILDER
# ===========================================================================
def make_record(row):
    preis_value = parse_price_to_number(row["Preis"])
    kategorie = row["KategorieDetected"]
    additional = row.get("AdditionalData", {})
    
    # Kurzbeschreibung via GPT generieren (mit Cache-Check)
    kurzbeschreibung = generate_kurzbeschreibung(
        beschreibung=row["Description"],
        titel=row["Titel"],
        kategorie=kategorie,
        preis=row["Preis"],
        ort=row["Ort"],
        zimmer=additional.get("zimmer", ""),
        wohnflaeche=additional.get("wohnflaeche", ""),
        grundstueck=additional.get("grundstueck", ""),
        baujahr=additional.get("baujahr", ""),
        objektnummer=row["Objektnummer"]
    )
    
    return {
        "Titel":           row["Titel"],
        "Kategorie":       kategorie,
        "Webseite":        row["URL"],
        "Objektnummer":    row["Objektnummer"],
        "Beschreibung":    row["Description"],
        "Kurzbeschreibung": kurzbeschreibung,
        "Bild":            row["Bild_URL"],
        "Preis":           preis_value,
        "Standort":        row["Ort"],
    }

# ===========================================================================
# SYNC je Kategorie
# ===========================================================================
def sync_category(scraped_rows, category_label: str):
    allowed = airtable_existing_fields()
    print(f"[Airtable] Erkannte Beispiel-Felder: {sorted(list(allowed)) or '(keine – Tabelle evtl. leer)'}")

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
    if to_create:
        airtable_batch_create(to_create)
    if to_update:
        airtable_batch_update(to_update)
    if to_delete_ids:
        airtable_batch_delete(to_delete_ids)

# ===========================================================================
# MAIN
# ===========================================================================
def run(mode="auto"):
    """
    Modi:
      - 'kauf'  : nur Kaufen-Sätze
      - 'miete' : nur Mieten-Sätze
      - 'auto'  : beide erkennen; zwei CSVs & Upserts
    """
    assert mode in ("kauf", "miete", "auto"), "Mode muss 'kauf', 'miete' oder 'auto' sein."
    csv_kauf  = "msi_kauf.csv"
    csv_miete = "msi_miete.csv"

    # OPTIMIERUNG: Lade existierende Kurzbeschreibungen aus Airtable
    print("[INIT] Lade Kurzbeschreibungen-Cache aus Airtable...")
    load_kurzbeschreibung_cache()

    all_rows, seen = [], set()
    for idx, list_url in enumerate(get_list_page_urls("kauf"), 1):
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

                # Skip, wenn Titel „verkauft" enthält
                if re.search(r"\bverkauft\b", row.get("Titel", ""), re.IGNORECASE):
                    print(f"  - {j}/{len(new_links)} SKIPPED (verkauft) | {row.get('Titel','')[:70]}")
                    continue

                record = make_record(row)
                all_rows.append(record)
                print(f"  - {j}/{len(new_links)} {record['Kategorie']:6} | {record['Titel'][:70]} | desc_len={len(record['Beschreibung'])}")
                time.sleep(0.1)
            except Exception as e:
                print(f"    [FEHLER] {url} -> {e}")
                continue
        time.sleep(0.2)

    if not all_rows:
        print("[WARN] Keine Datensätze gefunden.")
        return

    # Split nur zur CSV-Erzeugung nach Kategorie
    rows_kauf  = [r for r in all_rows if r["Kategorie"] == "Kaufen"]
    rows_miete = [r for r in all_rows if r["Kategorie"] == "Mieten"]

    if mode == "kauf":
        rows_miete = []
    if mode == "miete":
        rows_kauf = []

    cols = ["Titel","Kategorie","Webseite","Objektnummer","Beschreibung","Kurzbeschreibung","Bild","Preis","Standort"]
    if rows_kauf:
        with open(csv_kauf, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols, extrasaction='ignore')
            w.writeheader()
            w.writerows(rows_kauf)
        print(f"[CSV] {csv_kauf}: {len(rows_kauf)} Zeilen")
    if rows_miete:
        with open(csv_miete, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols, extrasaction='ignore')
            w.writeheader()
            w.writerows(rows_miete)
        print(f"[CSV] {csv_miete}: {len(rows_miete)} Zeilen")

    if AIRTABLE_TOKEN and AIRTABLE_BASE and airtable_table_segment():
        if rows_kauf:
            sync_category(rows_kauf, "Kaufen")
        if rows_miete:
            sync_category(rows_miete, "Mieten")
    else:
        print("[Airtable] ENV nicht gesetzt – Upload übersprungen.")

# ===========================================================================
if __name__ == "__main__":
    mode = (sys.argv[1].strip().lower() if len(sys.argv) > 1 else "kauf")
    run(mode)
