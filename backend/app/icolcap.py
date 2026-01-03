import os
import yfinance as yf

def download_icolcap_csv(_start="2024-01-01", _end="2025-01-01"):
    output_dir = "data/market"
    output_file = "icolcap.csv"
    output_path = os.path.join(output_dir, output_file)

    os.makedirs(output_dir, exist_ok=True)

    ticker = yf.Ticker("ICOLCAP.CL")
    df = ticker.history(
        start=_start,
        end=_end,
        interval="1d"
    )

    df.to_csv(output_path)

    print(f"Archivo guardado en: {output_path}")

if __name__ == "__main__":
    print("[ICOLCAP] Downloading ICOLCAP data")

    download_icolcap_csv(
        start_date="2024-01-01",
        end_date="2025-01-01",
    )

    print("[ICOLCAP] ICOLCAP data saved")
