from fastapi import FastAPI
from app.config.colombian_domains import COLOMBIAN_DOMAINS
from app.sources import collect_records, download_records
from app.ingestor import extract_html_many, parse_html_many, process_and_save
from app.filter import filter_many

app = FastAPI()

@app.post("/download")
def download_news():
    records = collect_records(
        COLOMBIAN_DOMAINS,
        from_year=2024,
        max_records=50
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