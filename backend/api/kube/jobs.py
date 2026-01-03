from kubernetes import client
from .client import get_batch_client

NAMESPACE = "mediacorr"

def run_job(job_name: str, job_manifest: dict):
    batch = get_batch_client()

    try:
        batch.create_namespaced_job(
            namespace=NAMESPACE,
            body=job_manifest
        )
        return {"status": "created", "job": job_name}
    except client.exceptions.ApiException as e:
        if e.status == 409:
            return {"status": "already_exists", "job": job_name}
        raise

def job_status(job_name: str):
    batch = get_batch_client()
    job = batch.read_namespaced_job(job_name, NAMESPACE)

    return {
        "name": job.metadata.name,
        "succeeded": job.status.succeeded,
        "failed": job.status.failed,
        "active": job.status.active
    }
