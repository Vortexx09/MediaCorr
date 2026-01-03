import os
import json
from app.config.keywords import KEYWORDS
from concurrent.futures import ThreadPoolExecutor, as_completed  

import unicodedata
import re


def normalize_text(text: str) -> str:
    text = text.lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    return text


def tokenize(text: str) -> set:
    return set(re.findall(r"\b[a-záéíóúñ]+\b", text))


def filter_news(parsed_records, keywords=KEYWORDS, min_words=20):
    filtered = []

    norm_keywords = [normalize_text(k) for k in keywords]

    for record in parsed_records:
        title = record.get("title") or ""
        body = record.get("body") or ""

        if not body:
            continue

        if len(body.split()) < min_words:
            continue

        text = f"{title} {body}"
        norm_text = normalize_text(text)
        tokens = tokenize(norm_text)

        match_score = 0

        for kw in norm_keywords:
            kw_tokens = tokenize(kw)

            # Coincidencia parcial (morfología básica)
            if any(t.startswith(k) or k.startswith(t) for t in tokens for k in kw_tokens):
                match_score += 1

        # Umbral flexible
        if match_score >= 1:
            record["_filter_score"] = match_score
            filtered.append(record)

    print(f"[INFO] Filtradas {len(filtered)} noticias de {len(parsed_records)}")
    return filtered

def filter_from_files(fname, input_dir="data/parsed", output_dir="data/filtered", keywords=KEYWORDS):
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

def filter_many(input_dir="data/parsed", output_dir="data/filtered", max_workers=5):
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

if __name__ == "__main__":
    print("[FILTER] Starting filtering stage")

    filter_many(
        input_dir="data/parsed",
        output_dir="data/filtered",
        max_workers=5
    )

    print("[FILTER] Filtering completed")
