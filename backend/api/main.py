from fastapi import FastAPI
from api.kube.jobs import run_job, job_status
from api.kube.manifests import (
    sources_job,
    ingestor_job,
    filter_job,
    classifier_job,
    correlator_job,
    icolcap_job
)

app = FastAPI(
    title="MediaCorr Controller API",
    description="API para orquestar el pipeline MediaCorr en Kubernetes",
    version="1.0.0"
)

# ---------- MARKET DATA ----------

@app.post("/icolcap")
def download_icolcap():
    return run_job("icolcap-job", icolcap_job())


# ---------- PIPELINE STAGES ----------

@app.post("/download")
def download_news():
    return run_job("sources-job", sources_job())


@app.post("/process")
def process_news():
    return run_job("ingestor-job", ingestor_job())


@app.post("/filter")
def filter_news():
    return run_job("filter-job", filter_job())


@app.post("/sentiment")
def sentiment_news():
    return run_job("classifier-job", classifier_job())


@app.post("/analysis")
def full_analysis():
    return run_job("correlator-job", correlator_job())


# ---------- STATUS ----------

@app.get("/status/{job_name}")
def get_status(job_name: str):
    return job_status(job_name)
