import os
import json
import pandas as pd
import numpy as np
from app.utils.causality import lagged_correlation, granger_test
from app.utils.visualization import plot_lagged_correlation, plot_rolling_correlation, plot_sentiment_vs_market
from scipy.stats import pearsonr, spearmanr

def load_sentiment_files(input_dir: str) -> pd.DataFrame:
    records = []

    for fname in os.listdir(input_dir):
        if not fname.endswith(".json"):
            continue

        with open(os.path.join(input_dir, fname), "r", encoding="utf-8") as f:
            data = json.load(f)
            records.extend(data)

    if not records:
        raise ValueError("No se encontraron registros de sentimiento")

    df = pd.DataFrame(records)

    df["published_date"] = pd.to_datetime(
        df["published_date"],
        errors="coerce",
        utc=True
    )
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

    df["date"] = pd.to_datetime(
        df["date"],
        errors="coerce",
        utc=True
    ).dt.normalize()
    df = df.sort_values("date")

    df["daily_return"] = np.log(df["close"]).diff()

    return df

def run_full_analysis(
    sentiment_dir="data/sentiment",
    icolcap_csv="data/market/icolcap.csv",
    output_dir="data/analysis",
    max_lag=5
):
    sentiment = load_sentiment_files(sentiment_dir)
    daily = aggregate_daily_sentiment(sentiment)
    market = load_icolcap(icolcap_csv)

    df = daily.merge(market, on="date", how="inner").dropna()

    plot_sentiment_vs_market(
        df,
        sentiment_col="avg_sentiment",
        market_col="daily_return",
        title="ICOLCAP vs Sentimiento promedio de noticias",
        output_path=os.path.join(output_dir, "sentiment_vs_icolcap.png")
    )

    os.makedirs(output_dir, exist_ok=True)

    report = {}

    lag_corr = lagged_correlation(
        df,
        sentiment_col="avg_sentiment",
        target_col="daily_return",
        max_lag=max_lag
    )

    lag_corr_path = os.path.join(output_dir, "lagged_correlation.csv")
    lag_corr.to_csv(lag_corr_path, index=False)

    plot_lagged_correlation(
        lag_corr,
        title="Correlación sentimiento vs retorno ICOLCAP",
        output_path=os.path.join(output_dir, "lagged_correlation.png")
    )

    report["lagged_correlation"] = lag_corr.to_dict(orient="records")

    granger = granger_test(
        df,
        sentiment_col="avg_sentiment",
        target_col="daily_return",
        max_lag=max_lag
    )

    granger_path = os.path.join(output_dir, "granger_results.csv")
    granger.to_csv(granger_path, index=False)

    report["granger_test"] = granger.to_dict(orient="records")

    plot_rolling_correlation(
        df,
        sentiment_col="avg_sentiment",
        target_col="daily_return",
        window=20,
        output_path=os.path.join(output_dir, "rolling_correlation.png")
    )

    with open(os.path.join(output_dir, "summary.json"), "w") as f:
        json.dump(report, f, indent=2)

    return report

if __name__ == "__main__":
    print("[CORRELATOR] Starting correlation analysis")

    report = run_full_analysis(
        sentiment_dir="data/sentiment",
        icolcap_csv="data/market/icolcap.csv",
        output_dir="data/analysis",
        max_lag=5
    )

    print("[CORRELATOR] Analysis completed")
    print(report)
