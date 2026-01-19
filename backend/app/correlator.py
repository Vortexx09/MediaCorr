import os
import json
import pandas as pd
import numpy as np

from app.utils.causality import lagged_correlation, granger_test
from app.utils.visualization import (
    plot_lagged_correlation,
    plot_rolling_correlation,
    plot_sentiment_vs_market
)

# ---------- K8S SHARDING ----------
JOB_INDEX = int(os.environ.get("JOB_COMPLETION_INDEX", "0"))
JOB_TOTAL = int(os.environ.get("JOB_COMPLETIONS", "1"))


def split_work(items, index, total):
    return [item for i, item in enumerate(items) if i % total == index]


# ---------- LOADERS ----------
def load_sentiment_files(input_dir: str) -> pd.DataFrame:
    records = []
    for fname in os.listdir(input_dir):
        if fname.endswith(".json"):
            with open(os.path.join(input_dir, fname), "r", encoding="utf-8") as f:
                records.extend(json.load(f))

    df = pd.DataFrame(records)
    df["published_date"] = pd.to_datetime(df["published_date"], errors="coerce", utc=True)
    df = df.dropna(subset=["published_date", "sentiment_score"])
    df["date"] = df["published_date"].dt.date
    return df


def aggregate_daily_sentiment(df: pd.DataFrame) -> pd.DataFrame:
    daily = (
        df.groupby("date")
        .agg(
            avg_sentiment=("sentiment_score", "mean"),
            news_count=("sentiment_score", "count"),
            negative_ratio=("sentiment_label", lambda x: (x == "negative").mean())
        )
        .reset_index()
    )
    daily["date"] = pd.to_datetime(daily["date"], utc=True)
    return daily


def load_icolcap(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df.columns = [c.lower().strip() for c in df.columns]

    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True).dt.normalize()
    df = df.sort_values("date")
    df["daily_return"] = np.log(df["close"]).diff()
    return df


# ---------- MAIN ANALYSIS ----------
def run_full_analysis(
    sentiment_dir="data/sentiment",
    icolcap_csv="data/market/icolcap.csv",
    output_dir="data/analysis",
    max_lags=None
):
    os.makedirs(output_dir, exist_ok=True)

    sentiment = load_sentiment_files(sentiment_dir)
    daily = aggregate_daily_sentiment(sentiment)
    market = load_icolcap(icolcap_csv)

    df = daily.merge(market, on="date", how="inner").dropna()

    # Experimentos posibles
    all_lags = max_lags or list(range(1, 11))
    my_lags = split_work(all_lags, JOB_INDEX, JOB_TOTAL)

    print(f"[CORRELATOR] Pod {JOB_INDEX+1}/{JOB_TOTAL} → lags {my_lags}")

    report = {}

    for lag in my_lags:
        lag_corr = lagged_correlation(
            df,
            sentiment_col="avg_sentiment",
            target_col="daily_return",
            max_lag=lag
        )

        lag_name = f"lagged_correlation_lag_{lag}"
        lag_corr.to_csv(os.path.join(output_dir, f"{lag_name}.csv"), index=False)

        plot_lagged_correlation(
            lag_corr,
            title=f"Correlación (lag {lag})",
            output_path=os.path.join(output_dir, f"{lag_name}.png")
        )

        report[lag_name] = lag_corr.to_dict(orient="records")

        granger = granger_test(
            df,
            sentiment_col="avg_sentiment",
            target_col="daily_return",
            max_lag=lag
        )

        granger.to_csv(
            os.path.join(output_dir, f"granger_lag_{lag}.csv"),
            index=False
        )

        report[f"granger_lag_{lag}"] = granger.to_dict(orient="records")

    # Solo el pod 0 genera gráficos globales
    if JOB_INDEX == 0:
        plot_sentiment_vs_market(
            df,
            sentiment_col="avg_sentiment",
            market_col="daily_return",
            title="ICOLCAP vs Sentimiento promedio",
            output_path=os.path.join(output_dir, "sentiment_vs_icolcap.png")
        )

        plot_rolling_correlation(
            df,
            sentiment_col="avg_sentiment",
            target_col="daily_return",
            window=20,
            output_path=os.path.join(output_dir, "rolling_correlation.png")
        )

    with open(os.path.join(output_dir, f"summary_part_{JOB_INDEX}.json"), "w") as f:
        json.dump(report, f, indent=2)

    return report


if __name__ == "__main__":
    print("[CORRELATOR] Starting correlation analysis (K8s parallel)")
    run_full_analysis()
    print("[CORRELATOR] Analysis completed")
