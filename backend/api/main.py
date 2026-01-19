from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from api.kube.jobs import run_job, job_status
from api.kube.manifests import (
    sources_job,
    ingestor_job,
    filter_job,
    classifier_job,
    correlator_job,
    icolcap_job
)
import os

app = FastAPI(
    title="MediaCorr Controller API",
    description="API para orquestar el pipeline MediaCorr en Kubernetes",
    version="1.0.0"
)

ANALYSIS_DIR = "/app/data/analysis"
os.makedirs(ANALYSIS_DIR, exist_ok=True)

app.mount(
    "/static",
    StaticFiles(directory=ANALYSIS_DIR),
    name="static"
)


# ---------- MARKET DATA ----------

@app.post("/icolcap")
def download_icolcap(_start="2024-01-01", _end="2025-01-01"):
    return run_job("icolcap-job", icolcap_job(_start, _end))


# ---------- PIPELINE STAGES ----------

@app.post("/download")
def download_news(_parallelism=3, _from=2024, _to=2025, _records=50):
    return run_job("sources-job", sources_job(_parallelism, _from, _to, _records))


@app.post("/process")
def process_news(_parallelism=3):
    return run_job("ingestor-job", ingestor_job(_parallelism))


@app.post("/filter")
def filter_news(_parallelism=3):
    return run_job("filter-job", filter_job(_parallelism))


@app.post("/sentiment")
def sentiment_news(_parallelism=3):
    return run_job("classifier-job", classifier_job(_parallelism))


@app.post("/analysis")
def full_analysis(_parallelism=3):
    return run_job("correlator-job", correlator_job(_parallelism))

@app.get(
    "/analysis/sentiment-vs-market",
    responses={200: {"content": {"image/png": {}}}},
)
def get_sentiment_vs_market_plot():
    return FileResponse(
        "data/analysis/sentiment_vs_icolcap.png",
        media_type="image/png"
    )


# ---------- STATUS ----------

@app.get("/status/{job_name}")
def get_status(job_name: str):
    return job_status(job_name)
