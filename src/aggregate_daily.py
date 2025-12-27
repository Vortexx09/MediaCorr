
import os
import json
import argparse
import pandas as pd
from collections import Counter

# Configure which event classes to track explicitly (must match what enrich.py emits)
EVENT_CLASSES = [
    "regulation_policy",
    "corporate_earnings",
    "conflict_security",
    "currency_devaluation",
    "energy",
    "banking_credit",
]

def parse_events_json(ev_json: str) -> list[dict]:
    if not ev_json:
        return []
    try:
        return json.loads(ev_json)
    except Exception:
        return []

def extract_classes(ev_list: list[dict]) -> set[str]:
    if not ev_list:
        return set()
    return {e.get("class") for e in ev_list if isinstance(e, dict) and e.get("class")}

def aggregate_daily(enriched_path: str, out_features_path: str, lang_filter: str | None) -> None:
    # Load enriched parquet
    df = pd.read_parquet(enriched_path)

    # Optional language filter (e.g., 'es')
    if lang_filter:
        df = df[df["language"] == lang_filter]

    # Ensure we have timestamps
    df = df.dropna(subset=["published_at_utc"])
    if df.empty:
        raise RuntimeError("No rows with published_at_utc after filtering.")

    # Derive day key YYYY-MM-DD from ISO timestamp
    df["day"] = df["published_at_utc"].str.slice(0, 10)

    # Parse events into a column of sets for aggregation
    df["event_classes"] = df["events_json"].apply(lambda s: extract_classes(parse_events_json(s)))

    # Begin aggregation
    rows = []
    for day, g in df.groupby("day"):
        # Sentiment stats
        sent_mean = float(g["sentiment_doc"].mean())
        sent_median = float(g["sentiment_doc"].median())
        sent_min = float(g["sentiment_doc"].min())
        sent_max = float(g["sentiment_doc"].max())

        # Count event classes
        counter = Counter()
        for s in g["event_classes"]:
            for c in s:
                counter[c] += 1

        row = {
            "day": day,
            "n_articles": int(len(g)),
            "sentiment_mean": sent_mean,
            "sentiment_median": sent_median,
            "sentiment_min": sent_min,
            "sentiment_max": sent_max,
        }
        # Add explicit columns for tracked classes; others go to a catch-all if needed
        for cls in EVENT_CLASSES:
            row[f"evt_{cls}_count"] = int(counter.get(cls, 0))

        # Optionally: number of docs with any events detected
        row["n_with_events"] = int(sum(1 for s in g["event_classes"] if s))

        rows.append(row)

    feat = pd.DataFrame(rows).sort_values("day")

    # Save features to parquet
    os.makedirs(os.path.dirname(out_features_path), exist_ok=True)
    feat.to_parquet(out_features_path, index=False)
    print(f"✅ Wrote daily features for {len(feat)} days → {out_features_path}")

def main():
    ap = argparse.ArgumentParser(description="Aggregate enriched articles into daily features.")
    ap.add_argument("--input", default="data/enriched/enriched.parquet", help="Path to enriched parquet")
    ap.add_argument("--output", default="data/features/daily_features.parquet", help="Output features parquet")
    ap.add_argument("--lang", default=None, help="Optional language filter (e.g., 'es')")
    args = ap.parse_args()

    aggregate_daily(args.input, args.output, args.lang)

if __name__ == "__main__":
    main()
