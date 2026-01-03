import os
import json
import time
import requests
from app.config.colombian_domains import COLOMBIAN_DOMAINS
from concurrent.futures import ThreadPoolExecutor, as_completed

CC_BASE = "https://data.commoncrawl.org/"

# Lista de índices disponibles para 2024 (ejemplo, ajusta según releases reales)
AVAILABLE_INDICES = {
    2024: [
        "CC-MAIN-2024-10-index",
        "CC-MAIN-2024-26-index",
        "CC-MAIN-2024-38-index",
        "CC-MAIN-2024-51-index",
    ],
}

def search_cc_index(domain, from_year, to_year, max_records=50):
    results = []
    for year in range(from_year, to_year+1):
        indices = AVAILABLE_INDICES.get(year, [])
        for index in indices:
            cc_index_url = f"https://index.commoncrawl.org/{index}"
            params = {
                "url": f"{domain}/",
                "matchType": "prefix",
                "output": "json"
            }
            print(f"[INFO] Consultando {cc_index_url}")
            try:
                with requests.get(cc_index_url, params=params, stream=True, timeout=30) as response:
                    if response.status_code != 200:
                        print(f"[WARN] Índice {index} devolvió {response.status_code}")
                        continue
                    for i, line in enumerate(response.iter_lines()):
                        if not line:
                            continue
                        results.append(json.loads(line))
                        if i+1 >= max_records:
                            break
            except Exception as e:
                print(f"[ERROR] Falló consulta en {index}: {e}")
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
    output_path = os.path.join(output_dir, f"{safe_name}_{offset}.warc.gz")

    with requests.get(warc_url, headers=headers, stream=True, timeout=(10, 120)) as response:
        response.raise_for_status()
        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    return output_path


def collect_records(domains, from_year=2024, to_year=2025, max_records=50):
    all_records = []
    total = len(domains)

    start = time.time()

    for i, domain in enumerate(domains, 1):
        print(f"[{i}/{total}] Buscando en dominio: {domain}...")
        records = search_cc_index(domain, from_year, to_year, max_records)
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
        futures = {executor.submit(download_cc_file, record): record for record in records}

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

def download_sources():
    records = collect_records(
        COLOMBIAN_DOMAINS,
        from_year=2024,
        to_year=2025,
        max_records=100
    )

    downloaded_files = download_records(
        records,
        max_workers=5
    )

    return {
        "records_found": len(records),
        "downloaded": len(downloaded_files)
    }

if __name__ == "__main__":
    from app.config.colombian_domains import COLOMBIAN_DOMAINS

    print("[SOURCES] Starting news collection")

    records = collect_records(
        domains=COLOMBIAN_DOMAINS,
        from_year=2024,
        to_year=2025,
        max_records=100
    )

    print(f"[SOURCES] Found {len(records)} records")

    downloaded = download_records(
        records,
        max_workers=5
    )

    print(f"[SOURCES] Downloaded {len(downloaded)} files")
