import os
import json
import time
import requests
from app.config.colombian_domains import COLOMBIAN_DOMAINS
from concurrent.futures import ThreadPoolExecutor, as_completed

CC_INDEX = "https://index.commoncrawl.org/CC-MAIN-2024-10-index"
CC_BASE = "https://data.commoncrawl.org/"

def search_cc_index(domain, from_year, max_records=5):
    results = []

    params = {
        "url": f"{domain}/*economia*",
        "matchType": "domain",
        "from": str(from_year),
        "output": "json"
    }

    with requests.get(CC_INDEX, params=params, stream=True) as response:
        response.raise_for_status()

        for i, line in enumerate(response.iter_lines()):
            if not line:
                continue

            results.append(json.loads(line))

            if i + 1 >= max_records:
                break

    return results

def download_cc_file(record, output_dir="data/raw"):
    os.makedirs(output_dir, exist_ok=True)

    warc_url = CC_BASE + record["filename"]
    offset = int(record["offset"])
    length = int(record["length"])

    headers = {
        "Range": f"bytes={offset}-{offset + length - 1}"
    }

    safe_name = record["filename"].replace("/", "_")
    output_path = os.path.join(
        output_dir,
        f"{safe_name}_{offset}.warc.gz"
    )

    with requests.get(
        warc_url,
        headers=headers,
        stream=True,
        timeout=(10, 120)
    ) as response:

        response.raise_for_status()

        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    return output_path

def collect_records(domains, from_year=2024, max_records=5):
    all_records = []
    total = len(domains)

    start = time.time()

    for i, domain in enumerate(domains, 1):
        print(f"[{i}/{total}] Buscando en dominio: {domain}...")
        records = search_cc_index(domain, from_year, max_records)
        all_records.extend(records)
        print(f"   → {len(records)} registros encontrados en {domain}")

    end = time.time()
    print(f"Búsqueda completada en {end - start:.2f} segundos")

    return all_records


def download_records(records, max_workers=5):
    downloaded = []
    total = len(records)
    start = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(download_cc_file, record): record
            for record in records
        }

        for i, future in enumerate(as_completed(futures), 1):
            record = futures[future]
            try:
                path = future.result()
                downloaded.append(path)
                print(f"[{i}/{total}] Descargado: {record['url']}")
            except Exception as e:
                print(f"[{i}/{total}] Error en {record['url']}: {e}")

    end = time.time()
    print(f"Descarga completada en {end - start:.2f} segundos")

    return downloaded
