# msi_v2_airtable_replace.py
# -*- coding: utf-8 -*-
"""
MSI Hessen Scraper v3
=====================
Scrapt Kauf- und Mietimmobilien von msi-hessen.de und synchronisiert
sie mit einer Airtable-Datenbank.

Seitenstruktur (Stand März 2026):
  - Kategorie + Standort: <p class="h5"><span class="badge">Wohnung zur Miete</span> 36304 Alsfeld</p>
  - Preis + ObjektNr:     .immo-listing__infotext span.text-large / span.lh-large
  - Zimmer/Schlaf/Bad:     .immo-listing__infotext ul.list-bordered li[title]
  - Beschreibung:          JSON-LD "description" (Vue-Tabs nur mit JS-Rendering)
  - Bild:                  .immo-expose__head--image background-image
  - Sidebar-Listings:      .immo-listing__wrapper → AUSSCHLIESSEN bei Extraktion

Nutzung:
  python msi_v2_airtable_replace.py           # auto (Kauf + Miete)
  python msi_v2_airtable_replace.py kauf      # nur Kaufen
  python msi_v2_airtable_replace.py miete     # nur Mieten
"""
import sys
import time
import csv
import os
import json
import requests
import re
from urllib.parse import quote, urljoin
from bs4 import BeautifulSoup

# ===========================================================================
# KONFIGURATION / ENV
# ===========================================================================
BASE = "https://www.msi-hessen.de"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
}

# Airtable ENV
AIRTABLE_TOKEN    = os.getenv("AIRTABLE_TOKEN", "").strip()
AIRTABLE_BASE     = os.getenv("AIRTABLE_BASE",  "").strip()
AIRTABLE_TABLE    = os.getenv("AIRTABLE_TABLE", "").strip()
AIRTABLE_TABLE_ID = os.getenv("AIRTABLE_TABLE_ID", "").strip()
AIRTABLE_VIEW     = os.getenv("AIRTABLE_VIEW", "").strip()

# ===========================================================================
# HTTP
# ===========================================================================
def fetch(url: str) -> BeautifulSoup:
    """Einfacher HTTP-Fetch. Kein JS-Rendering nötig."""
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

# ===========================================================================
# LISTING-SEITEN: Detail-Links sammeln
# ===========================================================================
def get_listing_urls(mode: str, max_pages: int = 50) -> list[str]:
    """
    Generiert Listing-URLs für Kauf und/oder Miete.
    Gibt Liste von (url, listing_mode) Tupeln zurück.
    """
    pages = []
    modes = []
    if mode in ("kauf", "auto"):
        modes.append("buy")
    if mode in ("miete", "auto"):
        modes.append("rent")

    for mt in modes:
        base = f"{BASE}/kaufen/immobilienangebote/?mt={mt}&sort=sort%7Cdesc"
        pages.append(base)
        for i in range(2, max_pages + 1):
            pages.append(f"{BASE}/kaufen/immobilienangebote/page/{i}/?mt={mt}&sort=sort%7Cdesc")
    return pages


def collect_detail_links(list_url: str) -> list[str]:
    """Sammelt alle Detail-Links von einer Listing-Seite."""
    soup = fetch(list_url)
    links = set()
    for a in soup.select('a[href*="/angebote/"]'):
        href = a.get("href", "")
        if not href or href.endswith("/angebote/") or "?" in href:
            continue
        full_url = href if href.startswith("http") else urljoin(BASE, href)
        if "/angebote/" in full_url and full_url.count("/") >= 5:
            links.add(full_url)
    return sorted(links)

# ===========================================================================
# DETAIL-PARSING: Zuverlässige Extraktion aus dem Server-HTML
# ===========================================================================
def _in_sidebar(tag, sidebar_ids: set) -> bool:
    """Prüft ob ein Tag innerhalb eines Sidebar-Wrappers liegt."""
    for parent in tag.parents:
        if id(parent) in sidebar_ids:
            return True
    return False


def _detect_kategorie(text: str) -> str:
    """Erkennt Kauf/Miete aus dem Badge-Text."""
    text_lower = text.lower()
    if "miete" in text_lower or "mieten" in text_lower:
        return "Mieten"
    if "kauf" in text_lower or "kaufen" in text_lower:
        return "Kaufen"
    return "Kaufen"


def _parse_price_to_number(price_str: str):
    """Konvertiert '159.900 €' -> 159900.0 oder None."""
    if not price_str:
        return None
    cleaned = price_str.replace("€", "").replace("\xa0", "").strip()
    # "159.900" -> 159900
    cleaned = re.sub(r"[^\d,.]", "", cleaned)
    if not cleaned:
        return None
    # Deutsche Notation: Punkt als Tausender, Komma als Dezimal
    if "," in cleaned and "." in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    elif "," in cleaned:
        cleaned = cleaned.replace(",", ".")
    else:
        # Nur Punkte: prüfe ob Tausender-Separator
        parts = cleaned.split(".")
        if len(parts) > 1 and len(parts[-1]) == 3:
            cleaned = cleaned.replace(".", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_detail(url: str) -> dict:
    """
    Parst eine MSI-Detailseite und gibt strukturierte Daten zurück.
    Funktioniert ohne JS-Rendering.

    WICHTIG: Sidebar-Listings (.immo-listing__wrapper) werden bei der
    Extraktion ausgeschlossen, da sie eigene Zimmer/Preis-Daten haben.
    """
    soup = fetch(url)

    result = {
        "Titel": "",
        "Typ": "",
        "Kategorie": "",
        "Standort": "",
        "Preis": "",
        "Objektnummer": "",
        "Beschreibung": "",
        "Zimmer": "",
        "Schlafzimmer": "",
        "Badezimmer": "",
        "Wohnflaeche": "",
        "Bild_URL": "",
        "URL": url,
        "Ansprechpartner": "",
    }

    detail_page = soup.select_one(".immobiliendetailseite") or soup

    # Sidebar-Elemente markieren (IDs merken)
    sidebar_ids = set()
    for sw in detail_page.select(".immo-listing__wrapper"):
        sidebar_ids.add(id(sw))

    # --- Titel (h1) ---
    h1 = detail_page.select_one("h1")
    if h1 and not _in_sidebar(h1, sidebar_ids):
        result["Titel"] = " ".join(h1.get_text(" ", strip=True).split())

    # --- Kategorie + Standort ---
    # <p class="h5"><span class="badge badge-secondary">Wohnung zur Miete</span> 36304 Alsfeld</p>
    top_p = detail_page.select_one("p.h5")
    if top_p and not _in_sidebar(top_p, sidebar_ids):
        badge = top_p.select_one("span.badge")
        if badge:
            badge_text = badge.get_text(strip=True)
            result["Kategorie"] = _detect_kategorie(badge_text)
            result["Typ"] = badge_text
            full_text = top_p.get_text(" ", strip=True)
            result["Standort"] = full_text.replace(badge_text, "").strip()
        else:
            full_text = top_p.get_text(" ", strip=True)
            result["Kategorie"] = _detect_kategorie(full_text)
            m = re.search(r"(\d{5})\s+(.+)", full_text)
            result["Standort"] = f"{m.group(1)} {m.group(2).strip()}" if m else full_text.strip()

    # --- Preis + Objektnummer ---
    # Im ERSTEN .immo-listing__infotext (Hauptbereich, nicht Sidebar)
    infotext = None
    for it in detail_page.select(".immo-listing__infotext"):
        if not _in_sidebar(it, sidebar_ids):
            infotext = it
            break

    if infotext:
        # Preis: <span class="text-large font-weight-semibold">159.900 €</span>
        price_span = infotext.select_one("span.text-large")
        if price_span:
            result["Preis"] = price_span.get_text(strip=True)

        # Objektnummer: <span class="lh-large">Objekt-Nr.: 4273</span>
        objnr_span = infotext.select_one("span.lh-large")
        if objnr_span:
            text = objnr_span.get_text(strip=True)
            m = re.search(r"Objekt[-\s]?Nr\.?:\s*(.+)", text)
            if m:
                result["Objektnummer"] = m.group(1).strip()

        # Zimmer/Schlafzimmer/Bad: <li title="3 Zimmer">
        for item in infotext.select("li.list-inline-item[title]"):
            title = (item.get("title") or "").strip()
            if "Zimmer" in title and "Schlaf" not in title and "Bad" not in title:
                result["Zimmer"] = title
            elif "Schlafzimmer" in title:
                result["Schlafzimmer"] = title
            elif "Badezimmer" in title:
                result["Badezimmer"] = title

    # --- Wohnfläche ---
    for span in detail_page.select("span[title]"):
        if _in_sidebar(span, sidebar_ids):
            continue
        title = span.get("title", "")
        if "Wohnfl" in title and "m²" in title:
            result["Wohnflaeche"] = span.get_text(strip=True)
            break

    # --- Beschreibung aus JSON-LD ---
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            data = json.loads(script.get_text(strip=True))
        except (json.JSONDecodeError, ValueError):
            continue
        graph = data.get("@graph", [data] if isinstance(data, dict) else data)
        for node in (graph if isinstance(graph, list) else [graph]):
            if not isinstance(node, dict):
                continue
            if node.get("@type") == "WebPage" and node.get("description"):
                desc = node["description"].strip()
                if desc.endswith("..."):
                    desc = desc[:-3].strip()
                result["Beschreibung"] = desc
                break
        if result["Beschreibung"]:
            break

    # --- Ansprechpartner ---
    for strong in detail_page.select("strong"):
        if _in_sidebar(strong, sidebar_ids):
            continue
        text = strong.get_text(strip=True)
        if text.startswith("Herr ") or text.startswith("Frau "):
            result["Ansprechpartner"] = text
            break

    # --- Bild-URL ---
    head_img = detail_page.select_one(".immo-expose__head--image")
    if head_img:
        style = head_img.get("style", "")
        m = re.search(r"url\(['\"]?(.*?)['\"]?\)", style)
        if m:
            result["Bild_URL"] = m.group(1)

    if not result["Bild_URL"]:
        for img in detail_page.select('img[src*="screenwork"]'):
            if not _in_sidebar(img, sidebar_ids):
                result["Bild_URL"] = img.get("src", "")
                break

    if not result["Bild_URL"]:
        listing_img = detail_page.select_one(".immo-listing__image")
        if listing_img and not _in_sidebar(listing_img, sidebar_ids):
            style = listing_img.get("style", "")
            m = re.search(r"url\(['\"]?(.*?)['\"]?\)", style)
            if m:
                result["Bild_URL"] = m.group(1)

    return result

# ===========================================================================
# KURZBESCHREIBUNG MIT | TRENNZEICHEN
# ===========================================================================
def build_kurzbeschreibung(row: dict) -> str:
    """
    Baut eine kompakte Kurzbeschreibung mit | Trennzeichen.
    Beispiel: Wohnung zum Kauf | 159.900 € | 4 Zimmer | 2 Schlafzimmer | 1 Badezimmer | 34613 Schwalmstadt
    """
    parts = []
    if row.get("Typ"):
        parts.append(row["Typ"])
    if row.get("Preis"):
        parts.append(row["Preis"])
    if row.get("Zimmer"):
        parts.append(row["Zimmer"])
    if row.get("Schlafzimmer"):
        parts.append(row["Schlafzimmer"])
    if row.get("Badezimmer"):
        parts.append(row["Badezimmer"])
    if row.get("Wohnflaeche"):
        parts.append(row["Wohnflaeche"])
    if row.get("Standort"):
        parts.append(row["Standort"])
    if row.get("Objektnummer"):
        parts.append(f"Obj.-Nr. {row['Objektnummer']}")
    return " | ".join(parts)

# ===========================================================================
# RECORD BUILDER
# ===========================================================================
def make_record(row: dict) -> dict:
    """Erstellt einen Airtable-kompatiblen Record aus den gescrapten Daten."""
    preis_value = _parse_price_to_number(row["Preis"])
    return {
        "Titel":            row["Titel"],
        "Kategorie":        row["Kategorie"],
        "Webseite":         row["URL"],
        "Objektnummer":     row["Objektnummer"],
        "Beschreibung":     row["Beschreibung"],
        "Kurzbeschreibung": build_kurzbeschreibung(row),
        "Bild":             row["Bild_URL"],
        "Preis":            preis_value,
        "Standort":         row["Standort"],
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
# SYNC je Kategorie
# ===========================================================================
def sync_category(scraped_rows, category_label: str):
    allowed = airtable_existing_fields()
    print(f"[Airtable] Erkannte Beispiel-Felder: {sorted(list(allowed)) or '(keine)'}")

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

    print(f"[SYNC] {category_label} -> create: {len(to_create)}, update: {len(to_update)}, delete: {len(to_delete_ids)}")
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
      - 'kauf'  : nur Kaufen
      - 'miete' : nur Mieten
      - 'auto'  : beide Kategorien (Kauf + Miete)
    """
    assert mode in ("kauf", "miete", "auto"), "Mode muss 'kauf', 'miete' oder 'auto' sein."
    csv_kauf  = "msi_kauf.csv"
    csv_miete = "msi_miete.csv"

    all_rows, seen = [], set()

    # --- Kauf-Listings crawlen ---
    if mode in ("kauf", "auto"):
        print(f"\n{'=' * 60}")
        print(f"  KAUFEN – Listings sammeln")
        print(f"{'=' * 60}")
        for idx in range(1, 51):
            if idx == 1:
                list_url = f"{BASE}/kaufen/immobilienangebote/?mt=buy&sort=sort%7Cdesc"
            else:
                list_url = f"{BASE}/kaufen/immobilienangebote/page/{idx}/?mt=buy&sort=sort%7Cdesc"
            try:
                detail_links = collect_detail_links(list_url)
            except Exception as e:
                print(f"[WARN] Abbruch Seite {idx}: {e}")
                break
            new_links = [u for u in detail_links if u not in seen]
            if idx > 1 and not new_links:
                print(f"[INFO] Keine neuen Links auf Seite {idx} – Stop.")
                break
            seen.update(new_links)
            print(f"[Seite {idx}] {len(new_links)} neue Detailseiten")
            for j, url in enumerate(new_links, 1):
                try:
                    row = parse_detail(url)
                    if re.search(r"\bverkauft\b", row.get("Titel", ""), re.IGNORECASE):
                        print(f"  - {j}/{len(new_links)} SKIPPED (verkauft) | {row.get('Titel','')[:70]}")
                        continue
                    record = make_record(row)
                    all_rows.append(record)
                    print(f"  - {j}/{len(new_links)} {record['Kategorie']:6} | {record['Titel'][:50]} | {record.get('Standort','')}")
                    time.sleep(0.15)
                except Exception as e:
                    print(f"    [FEHLER] {url} -> {e}")
                    continue
            time.sleep(0.25)

    # --- Miet-Listings crawlen ---
    if mode in ("miete", "auto"):
        print(f"\n{'=' * 60}")
        print(f"  MIETEN – Listings sammeln")
        print(f"{'=' * 60}")
        for idx in range(1, 51):
            if idx == 1:
                list_url = f"{BASE}/kaufen/immobilienangebote/?mt=rent&sort=sort%7Cdesc"
            else:
                list_url = f"{BASE}/kaufen/immobilienangebote/page/{idx}/?mt=rent&sort=sort%7Cdesc"
            try:
                detail_links = collect_detail_links(list_url)
            except Exception as e:
                print(f"[WARN] Abbruch Seite {idx}: {e}")
                break
            new_links = [u for u in detail_links if u not in seen]
            if idx > 1 and not new_links:
                print(f"[INFO] Keine neuen Links auf Seite {idx} – Stop.")
                break
            seen.update(new_links)
            print(f"[Seite {idx}] {len(new_links)} neue Detailseiten")
            for j, url in enumerate(new_links, 1):
                try:
                    row = parse_detail(url)
                    if re.search(r"\bverkauft\b", row.get("Titel", ""), re.IGNORECASE):
                        print(f"  - {j}/{len(new_links)} SKIPPED (verkauft) | {row.get('Titel','')[:70]}")
                        continue
                    record = make_record(row)
                    all_rows.append(record)
                    print(f"  - {j}/{len(new_links)} {record['Kategorie']:6} | {record['Titel'][:50]} | {record.get('Standort','')}")
                    time.sleep(0.15)
                except Exception as e:
                    print(f"    [FEHLER] {url} -> {e}")
                    continue
            time.sleep(0.25)

    if not all_rows:
        print("[WARN] Keine Datensaetze gefunden.")
        return

    rows_kauf  = [r for r in all_rows if r["Kategorie"] == "Kaufen"]
    rows_miete = [r for r in all_rows if r["Kategorie"] == "Mieten"]

    # --- CSV Export ---
    cols = ["Titel", "Kategorie", "Webseite", "Objektnummer", "Beschreibung",
            "Kurzbeschreibung", "Bild", "Preis", "Standort"]
    if rows_kauf:
        with open(csv_kauf, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(rows_kauf)
        print(f"[CSV] {csv_kauf}: {len(rows_kauf)} Zeilen")
    if rows_miete:
        with open(csv_miete, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(rows_miete)
        print(f"[CSV] {csv_miete}: {len(rows_miete)} Zeilen")

    # --- Zusammenfassung ---
    total = len(all_rows)
    print(f"\n{'=' * 60}")
    print(f"  ZUSAMMENFASSUNG: {total} Immobilien ({len(rows_kauf)} Kauf, {len(rows_miete)} Miete)")
    print(f"{'=' * 60}")
    checks = {
        "Titel": sum(1 for r in all_rows if r.get("Titel")),
        "Standort": sum(1 for r in all_rows if r.get("Standort")),
        "Preis": sum(1 for r in all_rows if r.get("Preis")),
        "Objektnummer": sum(1 for r in all_rows if r.get("Objektnummer")),
        "Beschreibung": sum(1 for r in all_rows if r.get("Beschreibung")),
        "Kurzbeschreibung": sum(1 for r in all_rows if r.get("Kurzbeschreibung")),
        "Bild": sum(1 for r in all_rows if r.get("Bild")),
    }
    for field, count in checks.items():
        pct = count / total * 100 if total else 0
        print(f"  {field:20s}: {count}/{total} ({pct:.0f}%)")

    # --- Beispiel Kurzbeschreibung ---
    print(f"\n  Beispiel Kurzbeschreibung:")
    for r in all_rows[:3]:
        print(f"    {r.get('Kurzbeschreibung', '')}")

    # --- Airtable Sync ---
    if AIRTABLE_TOKEN and AIRTABLE_BASE and airtable_table_segment():
        print(f"\n[Airtable] Starte Sync...")
        if rows_kauf:
            sync_category(rows_kauf, "Kaufen")
        if rows_miete:
            sync_category(rows_miete, "Mieten")
        print("[Airtable] Sync abgeschlossen.")
    else:
        print("\n[Airtable] ENV nicht gesetzt – Upload uebersprungen.")

# ===========================================================================
if __name__ == "__main__":
    mode = (sys.argv[1].strip().lower() if len(sys.argv) > 1 else "auto")
    run(mode)
