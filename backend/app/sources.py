import os
import json
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

from app.config.colombian_domains import COLOMBIAN_DOMAINS

CC_BASE = "https://data.commoncrawl.org/"

# Índices disponibles (ejemplo)
AVAILABLE_INDICES = {
    2023: [
        "CC-MAIN-2023-14-index",
        "CC-MAIN-2023-23-index",
        "CC-MAIN-2023-40-index",
    ],
    2024: [
        "CC-MAIN-2024-10-index",
        "CC-MAIN-2024-26-index",
        "CC-MAIN-2024-38-index",
        "CC-MAIN-2024-51-index",
    ],
    2025: [
        "CC-MAIN-2025-05-index",
    ],
}

# ------------------ K8s PARALLELISM ------------------

JOB_INDEX = int(os.environ.get("JOB_COMPLETION_INDEX", "0"))
JOB_TOTAL = int(os.environ.get("JOB_COMPLETIONS", "1"))

FROM_YEAR = int(os.environ.get("FROM_YEAR", 2024))
TO_YEAR = int(os.environ.get("TO_YEAR", 2025))
MAX_RECORDS = int(os.environ.get("MAX_RECORDS", 50))



def split_work(items, index, total):
    """
    Divide una lista entre N workers de forma determinística
    """
    return items[index::total]


# ------------------ COMMONCRAWL ------------------

def search_cc_index(domain, from_year, to_year, max_records=50):
    results = []

    for year in range(from_year, to_year + 1):
        indices = AVAILABLE_INDICES.get(year, [])
        for index in indices:
            cc_index_url = f"https://index.commoncrawl.org/{index}"
            params = {
                "url": f"{domain}/",
                "matchType": "prefix",
                "output": "json",
            }

            print(f"[INFO] {domain} → {index}")

            try:
                with requests.get(
                    cc_index_url, params=params, stream=True, timeout=30
                ) as response:
                    if response.status_code != 200:
                        print(f"[WARN] {index} → {response.status_code}")
                        continue

                    for i, line in enumerate(response.iter_lines()):
                        if not line:
                            continue
                        results.append(json.loads(line))
                        if i + 1 >= max_records:
                            break

            except Exception as e:
                print(f"[ERROR] {index}: {e}")

    return results


def download_cc_file(record, output_dir="/app/data/raw"):
    os.makedirs(output_dir, exist_ok=True)

    warc_url = CC_BASE + record["filename"]
    offset = int(record["offset"])
    length = int(record["length"])

    headers = {"Range": f"bytes={offset}-{offset + length - 1}"}

    safe_name = record["filename"].replace("/", "_")
    output_path = os.path.join(output_dir, f"{safe_name}_{offset}.warc.gz")

    with requests.get(
        warc_url, headers=headers, stream=True, timeout=(10, 120)
    ) as response:
        response.raise_for_status()
        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    return output_path


# ------------------ PIPELINE ------------------

def collect_records(domains, from_year, to_year, max_records):
    all_records = []
    start = time.time()

    for domain in domains:
        print(f"[SEARCH] {domain}")
        records = search_cc_index(domain, from_year, to_year, max_records)
        print(f"[FOUND] {domain}: {len(records)}")
        all_records.extend(records)

    print(f"[DONE] Search in {time.time() - start:.2f}s")
    return all_records


def download_records(records, max_workers=5):
    downloaded = []
    total = len(records)

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
                print(f"[{i}/{total}] OK {record['url']}")
            except Exception as e:
                print(f"[{i}/{total}] FAIL {record['url']} → {e}")

    return downloaded


# ------------------ ENTRYPOINT ------------------

if __name__ == "__main__":
    print("[SOURCES] Starting sources-job")
    print(f"[SOURCES] Pod index {JOB_INDEX} of {JOB_TOTAL}")

    assigned_domains = split_work(
        COLOMBIAN_DOMAINS,
        JOB_INDEX,
        JOB_TOTAL,
    )

    print(f"[SOURCES] Domains assigned: {assigned_domains}")

    records = collect_records(
        domains=assigned_domains,
        from_year=FROM_YEAR,
        to_year=TO_YEAR,
        max_records=MAX_RECORDS,
    )

    print(f"[SOURCES] Total records found: {len(records)}")

    downloaded = download_records(
        records,
        max_workers=5,
    )

    print(f"[SOURCES] Downloaded files: {len(downloaded)}")
