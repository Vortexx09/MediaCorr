
#!/usr/bin/env python3
"""
Join daily news features with ICOLCAP index data and plot ALL event & sentiment features.

Outputs:
- correlation_to_returns.csv (Pearson correlations vs returns)
- For each feature:
  - returns_vs_<feature>.png
  - xcf_<feature>.png
- Combined cross-correlation table: xcf_all.csv

CSV loader patch:
- Accepts ICOLCAP CSV with headers like: Date, Price, Open, High, Low, Vol., Change %
- Parses dates in MM/DD/YYYY and normalizes to YYYY-MM-DD for join
"""

import os
import re
import argparse
import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

warnings.filterwarnings("ignore")

# -----------------------------
# Utilities
# -----------------------------
def compute_returns(idx: pd.DataFrame, col_close: str = "close") -> pd.DataFrame:
    """Compute log returns from close levels."""
    idx = idx.copy()
    idx["ret"] = np.log(idx[col_close] / idx[col_close].shift(1))
    return idx


def cross_correlation(x: pd.Series, y: pd.Series, max_lag: int = 5) -> pd.DataFrame:
    """
    Compute simple cross-correlation of y_t with lagged x (x leading y),
    for lags 0..max_lag. Returns a DataFrame with lag and correlation.
    """
    rows = []
    for lag in range(0, max_lag + 1):
        x_lagged = x.shift(lag) if lag > 0 else x
        valid = pd.concat([x_lagged, y], axis=1).dropna()
        corr = valid.iloc[:, 0].corr(valid.iloc[:, 1]) if len(valid) > 1 else np.nan
        rows.append({"lag": lag, "corr": corr})
    return pd.DataFrame(rows)


def is_plottable_feature(series: pd.Series) -> bool:
    """Skip features that are all NaN or constant (e.g., all zeros)."""
    s = series.dropna()
    if s.empty:
        return False
    return s.nunique() > 1


def safe_name(name: str) -> str:
    """Sanitize feature name for filenames."""
    return re.sub(r"[^A-Za-z0-9_\-]+", "_", name)


# -----------------------------
# Patched CSV loader (handles Date/Price & MM/DD/YYYY)
# -----------------------------
def load_index_csv(path: str) -> pd.DataFrame:
    """
    Load index CSV with at least columns: Date and Price (or 'close').
    Accepts 'MM/DD/YYYY' and normalizes to 'YYYY-MM-DD' for 'day' key.
    Ignores extra columns (Open, High, Low, Vol., Change %).
    """
    df = pd.read_csv(path)

    # Flexible column mapping (case-insensitive)
    lower_map = {c.lower(): c for c in df.columns}

    # Map date column (expect 'Date')
    date_col = lower_map.get("date", None)
    if date_col is None:
        raise ValueError("CSV must include a 'Date' column.")

    # Map close/price column
    close_col = lower_map.get("close", None)
    if close_col is None:
        close_col = lower_map.get("price", None)
    if close_col is None:
        raise ValueError("CSV must include a 'Price' or 'Close' column.")

    # Parse date: prefer explicit MM/DD/YYYY; fallback to general parsing
    df["date"] = pd.to_datetime(df[date_col], format="%m/%d/%Y", errors="coerce")
    if df["date"].isna().any():
        df["date"] = pd.to_datetime(df[date_col], dayfirst=False, errors="coerce")
    if df["date"].isna().any():
        bad_examples = df[df["date"].isna()][date_col].head(5).tolist()
        raise ValueError(f"Failed to parse some dates. Examples: {bad_examples}")

    # Normalize close/price column to float (strip thousands separators)
    close_series = (
        df[close_col]
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.strip()
        .replace({"": np.nan})
    )
    # Handle possible European format like '1.379,58'
    if (close_series.str.contains(r"\d+,\d+", regex=True).sum() >
            close_series.str.contains(r"\d+\.\d+", regex=True).sum()):
        close_series = (
            close_series.str.replace(".", "", regex=False)
                         .str.replace(",", ".", regex=False)
        )

    df["close"] = close_series.astype(float)
    df = df.dropna(subset=["close"]).copy()
    df["day"] = df["date"].dt.strftime("%Y-%m-%d")
    return df[["date", "day", "close"]]


def load_features(path: str) -> pd.DataFrame:
    feat = pd.read_parquet(path)
    if "day" not in feat.columns:
        raise ValueError("Features parquet must include 'day' column (YYYY-MM-DD).")
    return feat


# -----------------------------
# Main analysis: plot ALL features
# -----------------------------
def main(
    features_path: str,
    index_csv_path: str,
    out_dir: str,
    max_lag: int,
    include_articles: bool = True,
):
    os.makedirs(out_dir, exist_ok=True)

    # Load
    feat = load_features(features_path)
    idx = load_index_csv(index_csv_path)
    idx = compute_returns(idx).dropna(subset=["ret"])

    # Join
    df = feat.merge(idx[["day", "ret"]], on="day", how="inner").sort_values("day")
    if df.empty:
        raise RuntimeError("No overlap between features and index days after join. "
                           "Check date ranges and formats.")
    print(f"Joined rows: {len(df)}  |  Date range: {df['day'].min()} → {df['day'].max()}")

    # Identify features to plot:
    # - All event counts: columns starting with 'evt_' and ending '_count'
    event_features = [c for c in df.columns if c.startswith("evt_") and c.endswith("_count")]
    # - Sentiment metrics: columns starting with 'sentiment_'
    sentiment_features = [c for c in df.columns if c.startswith("sentiment_")]
    # - Optionally include n_articles (volume proxy)
    extra_features = []
    if include_articles and "n_articles" in df.columns:
        extra_features.append("n_articles")

    features_to_plot = event_features + sentiment_features + extra_features
    if not features_to_plot:
        raise RuntimeError("No event/sentiment features found to plot. "
                           "Verify aggregate_daily.py output.")

    print("Features to plot:", features_to_plot)

    # ---- Correlation table vs returns
    numeric_cols = [c for c in df.columns if c not in ["day"] and pd.api.types.is_numeric_dtype(df[c])]
    corr = df[numeric_cols].corr()
    corr_to_ret = corr[["ret"]].sort_values(by="ret", ascending=False)
    corr_path = os.path.join(out_dir, "correlation_to_returns.csv")
    corr_to_ret.to_csv(corr_path)
    print(f"Saved correlations → {corr_path}")

    # ---- Loop and plot ALL features
    xcf_all_rows = []
    for feature in features_to_plot:
        series = df[feature]

        if not is_plottable_feature(series):
            print(f"Skip '{feature}' (constant or empty after join).")
            continue

        # Cross-correlation (feature leads returns)
        xcf = cross_correlation(series, df["ret"], max_lag=max_lag)
        xcf["feature"] = feature
        xcf_all_rows.extend(xcf.to_dict("records"))

        # Plot returns vs feature
        fig, ax1 = plt.subplots(figsize=(10, 5))
        ax1.plot(df["day"], df["ret"], color="tab:blue", label="ICOLCAP returns")
        ax1.set_ylabel("Returns (log)", color="tab:blue")
        ax1.tick_params(axis="y", labelcolor="tab:blue")
        ax1.set_xticks(range(len(df["day"])))
        ax1.set_xticklabels(df["day"], rotation=45, ha="right")

        ax2 = ax1.twinx()
        ax2.plot(df["day"], series, color="tab:red", label=feature)
        ax2.set_ylabel(feature, color="tab:red")
        ax2.tick_params(axis="y", labelcolor="tab:red")
        plt.title(f"Returns vs {feature}")
        fig.tight_layout()
        fname1 = os.path.join(out_dir, f"returns_vs_{safe_name(feature)}_.png")
        plt.savefig(fname1, dpi=150)
        plt.close(fig)

        # Plot cross-correlation bars
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.bar(xcf["lag"], xcf["corr"], color="tab:green")
        ax.set_xlabel("Lag (days) — feature leads returns")
        ax.set_ylabel("Correlation")
        ax.set_title(f"Cross-correlation: {feature} → returns")
        fig.tight_layout()
        fname2 = os.path.join(out_dir, f"xcf_{safe_name(feature)}_.png")
        plt.savefig(fname2, dpi=150)
        plt.close(fig)

        print(f"Saved plots for '{feature}':")
        print(f" - {fname1}")
        print(f" - {fname2}")

    # Save combined XCF table
    if xcf_all_rows:
        xcf_all = pd.DataFrame(xcf_all_rows)[["feature", "lag", "corr"]]
        xcf_all_path = os.path.join(out_dir, "xcf_all.csv")
        xcf_all.to_csv(xcf_all_path, index=False)
        print(f"Saved combined cross-correlation → {xcf_all_path}")

    print("\n✅ Completed plotting ALL event & sentiment features.")


# -----------------------------
# CLI
# -----------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Plot ALL event & sentiment features vs ICOLCAP returns.")
    ap.add_argument("--features", default="data/features/daily_features.parquet",
                    help="Path to daily features parquet")
    ap.add_argument("--icolcap", default="data/market/icolcap.csv",
                    help="Path to ICOLCAP CSV (headers: Date, Price, ...)")
    ap.add_argument("--out_dir", default="data/analysis",
                    help="Output directory for analysis artifacts")
    ap.add_argument("--max_lag", type=int, default=5,
                    help="Max lag for cross-correlation")
    ap.add_argument("--include_articles", action="store_true",
                    help="Include n_articles in plots if available")
    args = ap.parse_args()

    main(
        features_path=args.features,
        index_csv_path=args.icolcap,
        out_dir=args.out_dir,
        max_lag=args.max_lag,
        include_articles=args.include_articles,
    )
