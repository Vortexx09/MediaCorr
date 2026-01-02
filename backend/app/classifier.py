import os
import json
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

from pysentimiento import create_analyzer


# Cargar modelo UNA sola vez (muy importante)
analyzer = create_analyzer(
    task="sentiment",
    lang="es"
)


def normalize_score(output) -> float:
    """
    Convierte probabilidades del modelo a un score continuo [-1, 1]
    """
    probs = output.probas

    pos = probs.get("POS", 0.0)
    neg = probs.get("NEG", 0.0)

    return round(pos - neg, 4)


def classify_record(record: Dict) -> Dict:
    title = record.get("title") or ""
    body = record.get("body") or ""

    text = f"{title}. {body}".strip()

    if not text or len(text.split()) < 20:
        record["sentiment_label"] = "neutral"
        record["sentiment_score"] = 0.0
        return record

    result = analyzer.predict(text)

    label_map = {
        "POS": "positive",
        "NEG": "negative",
        "NEU": "neutral"
    }

    record["sentiment_label"] = label_map.get(result.output, "neutral")
    record["sentiment_score"] = normalize_score(result)

    return record


def classify_many(records: List[Dict], max_workers: int = 4) -> List[Dict]:
    classified = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(classify_record, record)
            for record in records
        ]

        for future in as_completed(futures):
            try:
                classified.append(future.result())
            except Exception as e:
                print(f"[WARN] Error clasificando noticia: {e}")

    return classified


def analyze_file(
    fname: str,
    input_dir: str = "data/filtered",
    output_dir: str = "data/sentiment",
    max_workers: int = 4
):
    input_path = os.path.join(input_dir, fname)

    with open(input_path, "r", encoding="utf-8") as f:
        records = json.load(f)

    if not records:
        return fname, 0

    classified = classify_many(records, max_workers=max_workers)

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"sentiment_{fname}")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(classified, f, ensure_ascii=False, indent=2)

    return fname, len(classified)


def analyze_many(
    input_dir: str = "data/filtered",
    output_dir: str = "data/sentiment",
    max_workers: int = 4
):
    files = [f for f in os.listdir(input_dir) if f.endswith(".json")]
    total = len(files)

    print(f"[INFO] Se encontraron {total} archivos para análisis de sentimiento")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(analyze_file, fname, input_dir, output_dir): fname
            for fname in files
        }

        for i, future in enumerate(as_completed(futures), 1):
            fname = futures[future]
            try:
                _, count = future.result()
                print(f"[{i}/{total}] {fname} → {count} noticias clasificadas")
            except Exception as e:
                print(f"[{i}/{total}] Error en {fname}: {e}")
