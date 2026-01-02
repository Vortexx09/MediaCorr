from fastapi import FastAPI
from app.config.colombian_domains import COLOMBIAN_DOMAINS
from app.sources import collect_records, download_records
from app.utils.finance import download_icolcap_csv
from app.ingestor import process_and_save
from app.filter import filter_many
from app.classifier import analyze_many
from app.correlator import run_full_analysis


app = FastAPI()

@app.post("/icolcap")
def download_icolcap(_start="2024-01-01", _end="2025-01-01"):
    download_icolcap_csv(_start, _end)

    return {
        "status": "ok", "message": f"Archivo ICOLCAP descargado"
    }

@app.post("/download")
def download_news():
    records = collect_records(
        COLOMBIAN_DOMAINS,
        from_year=2024,
        to_year=2025,
        max_records=100
    )

    downloaded_files = download_records(
        records,
        max_workers=5
    )

    return {
        "records_found": len(records),
        "downloaded": len(downloaded_files)
    }

@app.post("/process") 
def process_news( warc_dir: str = "data/raw", output_dir: str = "data/parsed", max_workers: int = 5 ): 
    process_and_save(warc_dir, output_dir=output_dir, max_workers=max_workers) 
    
    return { "status": "ok", "message": f"Archivos procesados desde {warc_dir} y guardados en {output_dir}" }

@app.post("/filter")
def filter_news():
    filter_many()

    return { "status": "ok", "message": f"Archivos procesados" }

@app.post("/sentiment")
def sentiment_news():
    analyze_many()
    return {
        "status": "ok",
        "message": "Análisis de sentimiento completado"
    }


@app.post("/analysis")
def full_analysis():
    report = run_full_analysis()
    return {
        "status": "ok",
        "message": "Análisis de causalidad y visualización completados",
        "summary": report
    }