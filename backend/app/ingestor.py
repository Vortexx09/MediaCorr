import os
import re
import json
import gzip
import time
from bs4 import BeautifulSoup
from readability import Document
from dateutil import parser as date_parser
from warcio.archiveiterator import ArchiveIterator
from concurrent.futures import ThreadPoolExecutor, as_completed

def extract_html(warc_path):
    extracted = []

    with gzip.open(warc_path, "rb") as stream:
        for record in ArchiveIterator(stream):
            if record.rec_type != "response":
                continue

            http_headers = record.http_headers
            if not http_headers:
                continue

            content_type = http_headers.get_header("Content-Type")
            if not content_type or "text/html" not in content_type:
                continue

            try:
                payload = record.content_stream().read()
                html = payload.decode("utf-8", errors="replace")
            except Exception:
                continue

            extracted.append({
                "url": record.rec_headers.get_header("WARC-Target-URI"),
                "timestamp": record.rec_headers.get_header("WARC-Date"),
                "html": html
            })

    return extracted

def parse_html(html: str, fallback_date=None):
    if not html or not isinstance(html, str):
        return {"title": None, "body": None, "published_date": fallback_date}

    try:
        doc = Document(html)
        cleaned_html = doc.summary(html_partial=True)
    except Exception as e:
        print(f"[WARN] Readability falló: {e}")
        cleaned_html = html  # fallback: usar HTML original

    soup = BeautifulSoup(cleaned_html, "lxml")

    # título
    title = None
    if soup.title:
        title = soup.title.get_text(strip=True)
    elif soup.find("h1"):
        title = soup.find("h1").get_text(strip=True)

    # párrafos
    paragraphs = []
    for p in soup.find_all("p"):
        text = p.get_text(strip=True)
        if len(text) > 40:
            paragraphs.append(text)
    body = "\n".join(paragraphs) if paragraphs else None

    # fecha
    published_date = None
    meta_keys = [
        {"property": "article:published_time"},
        {"name": "pubdate"},
        {"name": "publish-date"},
        {"name": "date"},
        {"itemprop": "datePublished"},
    ]
    for attrs in meta_keys:
        tag = soup.find("meta", attrs=attrs)
        if tag and tag.get("content"):
            try:
                published_date = date_parser.parse(tag["content"]).isoformat()
                break
            except Exception:
                pass

    if not published_date:
        time_tag = soup.find("time")
        if time_tag:
            candidate = time_tag.get("datetime") or time_tag.get_text(strip=True)
            try:
                published_date = date_parser.parse(candidate).isoformat()
            except Exception:
                pass

    if not published_date:
        text = soup.get_text(" ")
        match = re.search(r"\b(\d{1,2}\s+de\s+[a-zA-Z]+\s+de\s+\d{4})\b", text)
        if match:
            try:
                published_date = date_parser.parse(match.group(1), fuzzy=True).isoformat()
            except Exception:
                pass

    if not published_date:
        published_date = fallback_date

    return {"title": title, "body": body, "published_date": published_date}

def extract_html_many(warc_dir, max_workers=5):
    results = []

    warc_paths = [ 
        os.path.join(warc_dir, fname)
        for fname in os.listdir(warc_dir) 
        if fname.lower().endswith(".warc") or fname.lower().endswith(".warc.gz") 
    ]

    total = len(warc_paths)
    print(f"[INFO] Se encontraron {total} archivos WARC en {warc_dir}")

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(extract_html, path): path
            for path in warc_paths
        }

        processed = 0
        for future in as_completed(futures):
            warc_path = futures[future]
            try:
                extracted = future.result()
                results.extend(extracted)
            except Exception as e:
                print(f"[WARN] Error procesando {warc_path}: {e}")
            finally:
                processed += 1
                print(f"[PROGRESO] {processed}/{total} archivos procesados")

    elapsed = time.time() - start_time
    print(f"[INFO] extract_html_many completado en {elapsed:.2f} segundos")

    return results


def parse_html_many(html_records, max_workers=5):
    parsed = []

    total = len(html_records)
    print(f"[INFO] Se recibieron {total} registros HTML para parsear")

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(parse_html, r["html"], r.get("timestamp")) for r in html_records]

        processed = 0
        for future in as_completed(futures):
            try:
                parsed.append(future.result())
            except Exception as e:
                print(f"[WARN] Error parseando HTML: {e}")
            finally:
                processed += 1
                print(f"[PROGRESO] {processed}/{total} registros parseados")

    elapsed = time.time() - start_time
    print(f"[INFO] parse_html_many completado en {elapsed:.2f} segundos")

    return parsed

def process_and_save(warc_dir, output_dir="data/news", max_workers=5):
    os.makedirs(output_dir, exist_ok=True)

    warc_paths = [
        os.path.join(warc_dir, fname)
        for fname in os.listdir(warc_dir)
        if fname.lower().endswith(".warc") or fname.lower().endswith(".warc.gz")
    ]

    for i, warc_path in enumerate(warc_paths, 1):
        print(f"[INFO] Procesando archivo {i}/{len(warc_paths)}: {warc_path}")
        extracted = extract_html(warc_path)
        parsed = parse_html_many(extracted, max_workers=max_workers)

        output_file = os.path.join(output_dir, f"news_{i}.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(parsed, f, ensure_ascii=False, indent=2)

        print(f"[INFO] Guardado en {output_file}")
