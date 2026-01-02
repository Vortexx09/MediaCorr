import os
import matplotlib.pyplot as plt
import pandas as pd


def plot_lagged_correlation(
    df: pd.DataFrame,
    title: str,
    output_path: str
):
    plt.figure(figsize=(8, 5))
    plt.plot(df["lag"], df["correlation"], marker="o")
    plt.axhline(0, linestyle="--")
    plt.xlabel("Rezago (días)")
    plt.ylabel("Correlación")
    plt.title(title)
    plt.grid(True)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path, dpi=150)
    plt.close()


def plot_rolling_correlation(
    df: pd.DataFrame,
    sentiment_col: str,
    target_col: str,
    window: int,
    output_path: str
):
    rolling_corr = (
        df[sentiment_col]
        .rolling(window)
        .corr(df[target_col])
    )

    plt.figure(figsize=(10, 5))
    plt.plot(df["date"], rolling_corr)
    plt.axhline(0, linestyle="--")
    plt.xlabel("Fecha")
    plt.ylabel("Correlación móvil")
    plt.title(f"Rolling correlation ({window} días)")
    plt.grid(True)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path, dpi=150)
    plt.close()

def plot_sentiment_vs_market(
    df: pd.DataFrame,
    sentiment_col: str,
    market_col: str,
    title: str,
    output_path: str
):
    fig, ax1 = plt.subplots(figsize=(12, 5))

    # ICOLCAP (eje izquierdo)
    line1, = ax1.plot(
        df["date"],
        df[market_col],
        label="Retorno ICOLCAP",
        linewidth=2,
        color="orange"
    )
    ax1.set_xlabel("Fecha")
    ax1.set_ylabel("Retorno ICOLCAP (log)", color="orange")
    ax1.tick_params(axis='y', labelcolor="orange")
    ax1.grid(True)

    # Sentimiento (eje derecho)
    ax2 = ax1.twinx()
    line2, = ax2.plot(
        df["date"],
        df[sentiment_col],
        linestyle="--",
        label="Sentimiento promedio",
        color="#47acf0"
    )
    ax2.set_ylabel("Sentimiento promedio")
    ax2.tick_params(axis='y')

    # Leyenda combinada
    lines = [line1, line2]
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc="upper left")

    plt.title(title)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path, dpi=150)
    plt.close()
