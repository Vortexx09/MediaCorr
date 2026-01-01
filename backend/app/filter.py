import os
import json
from app.config.keywords import KEYWORDS
from concurrent.futures import ThreadPoolExecutor, as_completed  

def filter_news(parsed_records, keywords=KEYWORDS):
    filtered = []
    for record in parsed_records:
        body = record.get("body")
        title = record.get("title")
        if not body or len(body.split()) < 60:
            continue
        text = f"{title or ''} {body}"
        if any(kw.lower() in text.lower() for kw in keywords):
            filtered.append(record)
    print(f"[INFO] Filtradas {len(filtered)} noticias de {len(parsed_records)}")
    return filtered


def filter_from_files(fname, input_dir="data/news", output_dir="data/filtered", keywords=KEYWORDS):
    path = os.path.join(input_dir, fname)
    with open(path, "r", encoding="utf-8") as f:
        parsed_records = json.load(f)

    filtered = filter_news(parsed_records, keywords)

    if filtered:
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f"filtered_{fname}")
        with open (output_file, "w", encoding="utf-8") as f:
            json.dump(filtered, f, ensure_ascii=False, indent=2)
        return fname, len(filtered)
    else:
        return fname, 0

def filter_many(input_dir="data/news", output_dir="data/filtered", max_workers=5):
    os.makedirs(output_dir, exist_ok=True) 
    
    files = [f for f in os.listdir(input_dir) if f.endswith(".json")] 
    total = len(files) 
    print(f"[INFO] Se encontraron {total} archivos JSON en {input_dir}")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(filter_from_files, fname): fname for fname in files}

        for i, future in enumerate(as_completed(futures), 1):
            fname = futures[future]
            try:
                result = future.result()
                if result[1] > 0:
                    print(f"[{i}/{total}] Procesado {result[0]} → {result[1]} noticias filtradas")
                else:
                    print(f"[{i}/{total}] Procesado {result[0]} → sin coincidencias, no se guardó archivo")
            except Exception as e:
                print(f"[{i}/{total}] Error en {fname}: {e}")