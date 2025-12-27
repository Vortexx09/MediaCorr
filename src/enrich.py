
import os
import json
import glob
import hashlib
from datetime import timezone
from dateutil import parser as dateparser
from tqdm import tqdm
import pandas as pd

# -------------------------------
# Utilities
# -------------------------------

def normalize_timestamp(dt_str: str) -> str | None:
    """
    Parse arbitrary date strings and return ISO-8601 UTC.
    Fallback: None (we'll drop rows with no timestamp in aggregation later).
    """
    if not dt_str:
        return None
    try:
        dt = dateparser.parse(dt_str)
        if not dt:
            return None
        # Ensure timezone-aware and convert to UTC
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.isoformat()
    except Exception:
        return None

def content_hash(text: str) -> str:
    """
    Simple dedup key using SHA-256 of normalized text.
    """
    norm = " ".join(text.lower().split())
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()

# -------------------------------
# Simple rule-based classification
# -------------------------------

# Minimal keyword sets (Spanish + English).
# This is intentionally small—expand later.
EVENT_RULES = {
    "regulation_policy": [
        "decreto", "ley", "regulación", "regulacion", "normativa",
        "política", "politica", "gobierno", "ministerio",
        "regulation", "policy", "decree", "law", "government"
    ],
    "corporate_earnings": [
        "ganancias", "resultados", "utilidades", "balance", "ingresos", "beneficios",
        "earnings", "results", "profit", "revenue", "quarter", "q1", "q2", "q3", "q4"
    ],
    "conflict_security": [
        "conflicto", "protesta", "paro", "violencia", "ataque", "huelga", "seguridad",
        "conflict", "protest", "strike", "violence", "attack", "security"
    ],
    "currency_devaluation": [
        "devaluación", "tipo de cambio", "dólar", "dolar", "peso", "cotización", "tasa",
        "devaluation", "exchange rate", "fx", "currency", "usd", "cop"
    ],
    "energy": [
        "petróleo", "petroleo", "gas", "energía", "energia", "minería", "mineria", "hidrocarburos",
        "oil", "brent", "gas", "energy", "mining", "hydrocarbons"
    ],
    "banking_credit": [
        "crédito", "credito", "banco", "bancario", "tarjeta", "interés", "interes",
        "credit", "bank", "banking", "interest", "loan", "mortgage"
    ],
}

# Basic entity hints (add or remove as you wish)
KNOWN_ENTITIES = {
    "ORG": [
        "Ecopetrol", "Grupo Aval", "Bancolombia", "Nutresa", "ISA",
        "Banco de la República", "Ministerio de Hacienda"
    ],
    "GPE": [
        "Colombia", "Bogotá", "Bogota", "Medellín", "Medellin", "Barranquilla"
    ]
}

def classify_events(text: str) -> list[dict]:
    """
    Return list of event classes matched by keyword rules.
    """
    t = text.lower()
    hits = []
    for cls, keywords in EVENT_RULES.items():
        for kw in keywords:
            if kw in t:
                hits.append({"class": cls, "confidence": 0.6})
                break
    return hits

def detect_entities(text: str) -> list[dict]:
    """
    Naive entity detection by substring match from a small known list.
    Replace later with spaCy or a transformer model.
    """
    ents = []
    for etype, names in KNOWN_ENTITIES.items():
        for name in names:
            if name in text:
                ents.append({"type": etype, "text": name})
    return ents

# -------------------------------
# Simple sentiment (lexicon-based)
# -------------------------------

POS_WORDS_ES = {"bueno", "positiva", "crecimiento", "mejora", "alza", "ganancia", "fortaleza", "avanza"}
NEG_WORDS_ES = {"malo", "negativa", "caída", "caida", "pérdida", "perdida", "debilidad", "crisis", "baja"}
POS_WORDS_EN = {"good", "positive", "growth", "improve", "gain", "strength", "rally"}
NEG_WORDS_EN = {"bad", "negative", "drop", "loss", "weakness", "crisis", "fall"}

def simple_sentiment(text: str, lang: str) -> float:
    """
    Very naive sentiment score: (pos - neg) / (pos + neg + 1).
    Range roughly [-1, +1].
    """
    t = text.lower()
    if lang == "es":
        pos = sum(1 for w in POS_WORDS_ES if w in t)
        neg = sum(1 for w in NEG_WORDS_ES if w in t)
    else:
        pos = sum(1 for w in POS_WORDS_EN if w in t)
        neg = sum(1 for w in NEG_WORDS_EN if w in t)
    return (pos - neg) / (pos + neg + 1.0)

# -------------------------------
# Main processing
# -------------------------------

def process_jsonl(path: str) -> list[dict]:
    """
    Read one JSONL file (from your parser) and emit enriched dicts.
    """
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            doc = json.loads(line)
            text = doc.get("text", "")
            if not text or len(text) < 300:
                continue

            lang = doc.get("language", "und")
            published = normalize_timestamp(doc.get("published_at"))
            dedup = content_hash(text)

            events = classify_events(text)
            ents = detect_entities(text)
            sent = simple_sentiment(text, lang)

            out.append({
                "id": doc.get("id"),
                "source": doc.get("source", "ccnews"),
                "url": doc.get("url"),
                "title": doc.get("title", ""),
                "language": lang,
                "published_at_utc": published,
                "text_len": len(text),
                "dedup_key": dedup,
                "events_json": json.dumps(events, ensure_ascii=False),
                "entities_json": json.dumps(ents, ensure_ascii=False),
                "sentiment_doc": float(sent),
                "warc_file": doc.get("warc_file")
            })
    return out

def run(input_dir: str, output_path: str) -> None:
    files = sorted(glob.glob(os.path.join(input_dir, "*.jsonl")))
    if not files:
        raise FileNotFoundError(f"No JSONL files in {input_dir}")

    all_rows = []
    for path in tqdm(files, desc="Enriching JSONL"):
        rows = process_jsonl(path)
        all_rows.extend(rows)

    if not all_rows:
        raise RuntimeError("No rows produced. Check input and min length filter.")

    df = pd.DataFrame(all_rows)

    # Deduplicate by dedup_key; keep the longest text version if repeated
    # (We only have text_len, not text content here)
    df = df.sort_values("text_len", ascending=False).drop_duplicates(subset=["dedup_key"])

    # Save to Parquet
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    print(f"✅ Wrote {len(df)} enriched rows → {output_path}")

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Local enrichment worker")
    ap.add_argument("--input_dir", default="data/parsed", help="Directory with parsed JSONL files")
    ap.add_argument("--output", default="data/enriched/enriched.parquet", help="Output parquet path")
    args = ap.parse_args()
    run(args.input_dir, args.output)
