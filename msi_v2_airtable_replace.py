# msi_v2_airtable_replace.py
# -*- coding: utf-8 -*-
import sys, time, csv, os, json, requests, re
from urllib.parse import quote, urljoin
from bs4 import BeautifulSoup, Tag

# =============================================================================
# KONFIGURATION / ENVIRONMENT
# =============================================================================
BASE = "https://www.msi-hessen.de"
HEADERS = {"User-Agent": "Mozilla/5.0 (MSI Hessen Scraper)", "Accept-Language": "de-DE,de;q=0.9,en;q=0.8"}

# Airtable ENV
AIRTABLE_TOKEN    = os.getenv("AIRTABLE_TOKEN", "").strip()
AIRTABLE_BASE     = os.getenv("AIRTABLE_BASE", "").strip()
AIRTABLE_TABLE    = os.getenv("AIRTABLE_TABLE", "").strip()
AIRTABLE_TABLE_ID = os.getenv("AIRTABLE_TABLE_ID", "").strip()
AIRTABLE_VIEW     = os.getenv("AIRTABLE_VIEW", "").strip()

# Rendering ENV
MSI_RENDER         = os.getenv("MSI_RENDER", "0").strip() == "1"
MSI_RENDER_ENGINE  = os.getenv("MSI_RENDER_ENGINE", "playwright").strip().lower()
MSI_RENDER_TIMEOUT = int(os.getenv("MSI_RENDER_TIMEOUT", "20000"))
FRAME_HTMLS = {}

print(f"[CFG] MSI_RENDER={MSI_RENDER} | ENGINE={MSI_RENDER_ENGINE} | TIMEOUT={MSI_RENDER_TIMEOUT}")

# =============================================================================
# REGEX / HILFSKONSTANTEN
# =============================================================================
THIN_SPACES = "\u00A0\u202F\u2009"
RE_KAUF  = re.compile(r"\bzum\s*kauf\b", re.I)
RE_MIETE = re.compile(r"\bzur\s*miete\b|\b(kaltmiete|warmmiete|nettokaltmiete)\b", re.I)
RE_EUR_NUMBER = re.compile(r"\b\d{1,3}(?:[.\u00A0\u202F\u2009]\d{3})*(?:,\d{2})?\b")
RE_EUR_ANY    = re.compile(r"\b\d{1,3}(?:[.\u00A0\u202F\u2009]\d{3})*(?:,\d{2})?\s*[€EUR]?")
RE_PRICE_LINE = re.compile(
    r"(kaufpreis|preis|kaltmiete|warmmiete|nettokaltmiete|miete)\s*:?\s*([0-9.\u00A0\u202F\u2009,]+)\s*[€]?",
    re.I,
)
RE_PLZ_ORT = re.compile(r"\b(?!0{5})(\d{5})\s+([A-Za-zÄÖÜäöüß\- ]+)")
RE_OBJEKTNR = re.compile(r"Objekt[-\s]?Nr\.?:\s*([A-Za-z0-9\-_/]+)")
RE_PHONE = re.compile(r"\b(?:\+?\d{1,3}[\s/.-]?)?(?:0\d|\d{2,3})[\d\s/.-]{6,}\b")
RE_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
STOP_STRINGS = ("Ihre Anfrage", "Exposé anfordern", "Neueste Immobilien", "Teilen auf",
                "Datenschutz", "Impressum", "Kontaktieren Sie uns", "Zur Objektanfrage",
                "designed by wavepoint")

# =============================================================================
# HTTP + PLAYWRIGHT RENDERING
# =============================================================================
def _simple_fetch(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def _render_with_playwright(url: str, timeout_ms: int) -> tuple[str, list[str]]:
    """Headless-Rendering via Playwright (Chromium)."""
    from playwright.sync_api import sync_playwright
    frames = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(locale="de-DE", user_agent=HEADERS["User-Agent"])
        page = ctx.new_page()
        page.set_default_timeout(timeout_ms)
        page.goto(url, wait_until="domcontentloaded")

        # Tabs sichtbar machen
        for sel in [".v-expose", ".sw-vframe .v-expose"]:
            try:
                page.wait_for_selector(sel, state="attached", timeout=timeout_ms // 2)
                break
            except Exception:
                continue

        # Tab „Beschreibung“ anklicken
        try:
            for t in page.query_selector_all(".v-tab"):
                if "Beschreibung" in (t.inner_text() or ""):
                    t.click()
                    break
        except Exception:
            pass

        # Warten bis Text sichtbar
        try:
            page.wait_for_selector(".v-card__text p:not(.h4)", state="visible", timeout=timeout_ms // 2)
        except Exception:
            time.sleep(1)

        # iFrames auslesen (screenwork/immo)
        for fr in page.frames:
            try:
                u = (fr.url or "").lower()
                if any(k in u for k in ("screenwork", "immo", "expose")):
                    frames.append(fr.content())
            except Exception:
                pass

        html = page.content()
        ctx.close()
        browser.close()
        return html, frames

def soup_get(url: str) -> BeautifulSoup:
    if not MSI_RENDER:
        return _simple_fetch(url)
    try:
        html, frames = _render_with_playwright(url, MSI_RENDER_TIMEOUT)
        FRAME_HTMLS[url] = frames
        print(f"[RENDER] ok | frames={len(frames)}")
        return BeautifulSoup(html, "lxml")
    except Exception as e:
        print(f"[RENDER] Fehler: {e}")
        return _simple_fetch(url)

# =============================================================================
# LISTEN / LINKS
# =============================================================================
def get_list_page_urls(max_pages: int = 50):
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

# =============================================================================
# UTILS / TEXTBEREINIGUNG
# =============================================================================
def _norm(s): return re.sub(r"\s{2,}", " ", (s or "").strip())

def _dump_debug(html: str, label: str, obj_id: str):
    try:
        fn = f"debug_{obj_id}_{label}.html"
        with open(fn, "w", encoding="utf-8") as f: f.write(html)
        print(f"[DEBUG] wrote {fn}")
    except Exception as e:
        print(f"[DEBUG] dump fail: {e}")

def _clean_lines(lines):
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

def _find_scope(soup):
    return soup.select_one(".sw-vframe .v-expose") or soup.select_one(".v-expose") or soup

# =============================================================================
# PREIS-EXTRAKTION
# =============================================================================
def _normalize_numstring(s: str) -> str:
    if not s: return ""
    s = s.strip()
    for ch in THIN_SPACES: s = s.replace(ch, "")
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
    try: val = float(num)
    except Exception: return ""
    return f"{val:,.0f} €".replace(",", ".")

def parse_price_to_number(label: str):
    if not label: return None
    m = RE_EUR_NUMBER.search(label)
    if not m: return None
    num = _normalize_numstring(m.group(0))
    try: return float(num)
    except Exception: return None

def extract_price(soup: BeautifulSoup, page_text: str) -> str:
    # Versuche JSON-LD, DOM und Text
    for script in soup.select('script[type="application/ld+json"]'):
        try: data = json.loads(script.get_text(strip=True))
        except Exception: continue
        node = data if isinstance(data, dict) else (data[0] if isinstance(data, list) else None)
        if not isinstance(node, dict): continue
        price = node.get("price") or node.get("lowPrice") or node.get("highPrice")
        if price:
            try: return f"{float(str(price).replace('.', '').replace(',', '.')):,.0f} €".replace(",", ".")
            except: pass
    for li in soup.select("li"):
        txt = li.get_text(" ", strip=True)
        m = RE_PRICE_LINE.search(txt)
        if m: return clean_price_string(m.group(2) + " €")
    for line in page_text.splitlines():
        m = RE_PRICE_LINE.search(line)
        if m: return clean_price_string(m.group(2) + " €")
    return ""

# =============================================================================
# BESCHREIBUNG – Vuetify Tabs / Screenwork Frames
# =============================================================================
def extract_description(soup: BeautifulSoup) -> str:
    root = _find_scope(soup)
    candidates = [root.select_one("#tab-0"), root.select_one(".v-window-item.v-window-item--active")]
    candidates = [c for c in candidates if c]
    if not candidates:
        for box in root.select(".v-card__text"):
            head = box.select_one("p.h4,h4")
            if head and "beschreibung" in _norm(head.get_text()).lower():
                candidates.append(box)
    for node in candidates:
        box = node.select_one(".v-card__text") or node
        lines = []
        for p in box.select("p"):
            if "h4" in (p.get("class") or []): continue
            t = _norm(p.get_text(" ", strip=True))
            if t: lines.append(t)
        for li in box.select("ul li, ol li"):
            t = _norm(li.get_text(" ", strip=True))
            if t: lines.append("• " + t)
        cl = _clean_lines(lines)
        if cl: return "\n".join(cl)[:6000]
    return ""

def get_description(detail_url: str, soup: BeautifulSoup) -> str:
    desc = extract_description(soup)
    if desc: return desc
    obj = "na"
    m = RE_OBJEKTNR.search(soup.get_text(" ", strip=True))
    if m: obj = m.group(1)
    _dump_debug(str(soup), "main", obj)
    for i, html in enumerate(FRAME_HTMLS.get(detail_url) or []):
        s = BeautifulSoup(html, "lxml")
        desc = extract_description(s)
        if desc: return desc
        _dump_debug(html, f"frame{i}", obj)
    return ""

# =============================================================================
# DETAIL
# =============================================================================
def parse_detail(url: str):
    soup = soup_get(url)
    txt = soup.get_text(" ", strip=True)
    h1 = soup.select_one("h1")
    title = h1.get_text(strip=True) if h1 else ""
    desc = get_description(url, soup)
    m_obj = RE_OBJEKTNR.search(txt)
    obj = m_obj.group(1).strip() if m_obj else ""
    preis = extract_price(soup, txt)
    m_plz = RE_PLZ_ORT.search(txt)
    ort = f"{m_plz.group(1)} {m_plz.group(2)}" if m_plz else ""
    kategorie = "Mieten" if RE_MIETE.search(txt.lower()) else "Kaufen"
    return {"Titel": title, "URL": url, "Description": desc, "Objektnummer": obj, "Preis": preis,
            "Ort": ort, "KategorieDetected": kategorie}

# =============================================================================
# AIRTABLE
# =============================================================================
def airtable_api_url():
    seg = AIRTABLE_TABLE_ID or quote(AIRTABLE_TABLE, safe="")
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{seg}"

def airtable_headers():
    return {"Authorization": f"Bearer {AIRTABLE_TOKEN}", "Content-Type": "application/json"}

def airtable_batch_create(rows):
    url = airtable_api_url()
    for i in range(0, len(rows), 10):
        payload = {"records": [{"fields": r} for r in rows[i:i+10]], "typecast": True}
        r = requests.post(url, headers=airtable_headers(), data=json.dumps(payload), timeout=60)
        print(f"[Airtable] Create {r.status_code}")
        time.sleep(0.3)

# =============================================================================
# MAIN
# =============================================================================
def run(mode="auto"):
    urls = ["https://www.msi-hessen.de/angebote/jetzt-immobilieneigentum-sichernkeine-kaeuferprovision/"]
    all_rows = []
    for u in urls:
        print("Scraping:", u)
        try:
            r = parse_detail(u)
            print("→", r["Titel"], "| Desc len:", len(r["Description"]))
            all_rows.append(r)
        except Exception as e:
            print(f"[ERROR] {u}: {e}")
    if not all_rows:
        print("Keine Datensätze.")
        return
    # CSV
    with open("msi_kauf.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(all_rows[0].keys()))
        w.writeheader(); w.writerows(all_rows)
    print(f"[CSV] {len(all_rows)} Zeilen geschrieben.")
    # Airtable Upload
    if AIRTABLE_TOKEN and AIRTABLE_BASE and (AIRTABLE_TABLE_ID or AIRTABLE_TABLE):
        airtable_batch_create(all_rows)
    else:
        print("[Airtable] ENV nicht gesetzt – Upload übersprungen.")

# =============================================================================
if __name__ == "__main__":
    run("auto")
