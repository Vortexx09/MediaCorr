import os
import json
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pysentimiento import create_analyzer


JOB_INDEX = int(os.environ.get("JOB_COMPLETION_INDEX", "0"))
JOB_TOTAL = int(os.environ.get("JOB_COMPLETIONS", "1"))


def split_work(items, index, total):
    return [item for i, item in enumerate(items) if i % total == index]


analyzer = create_analyzer(task="sentiment", lang="es")


def normalize_score(output):
    p = output.probas
    return round(p.get("POS", 0.0) - p.get("NEG", 0.0), 4)


def classify_record(record: Dict) -> Dict:
    text = f"{record.get('title','')} {record.get('body','')}".strip()
    if len(text.split()) < 20:
        record["sentiment_label"] = "neutral"
        record["sentiment_score"] = 0.0
        return record

    result = analyzer.predict(text)
    record["sentiment_label"] = {
        "POS": "positive",
        "NEG": "negative",
        "NEU": "neutral"
    }.get(result.output, "neutral")
    record["sentiment_score"] = normalize_score(result)
    return record


def analyze_many(
    input_dir="data/filtered",
    output_dir="data/sentiment",
    max_workers=4
):
    files = sorted(f for f in os.listdir(input_dir) if f.endswith(".json"))
    files = split_work(files, JOB_INDEX, JOB_TOTAL)

    print(f"[CLASSIFIER] Pod {JOB_INDEX+1}/{JOB_TOTAL} → {len(files)} archivos")

    os.makedirs(output_dir, exist_ok=True)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for fname in files:
            futures[executor.submit(process_file, fname, input_dir, output_dir)] = fname

        for f in as_completed(futures):
            fname, count = f.result()
            print(f"[OK] {fname} → {count} noticias")


def process_file(fname, input_dir, output_dir):
    with open(os.path.join(input_dir, fname)) as f:
        records = json.load(f)

    classified = [classify_record(r) for r in records]

    out = os.path.join(output_dir, f"sentiment_{fname}")
    with open(out, "w") as f:
        json.dump(classified, f, ensure_ascii=False, indent=2)

    return fname, len(classified)


if __name__ == "__main__":
    analyze_many()
