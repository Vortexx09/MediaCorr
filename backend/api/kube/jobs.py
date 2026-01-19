import time
from kubernetes import client
from .client import get_batch_client

NAMESPACE = "mediacorr"


def wait_until_job_deleted(batch, job_name, timeout=30):
    start = time.time()

    while True:
        try:
            batch.read_namespaced_job(job_name, NAMESPACE)
        except client.exceptions.ApiException as e:
            if e.status == 404:
                return  # ya no existe
            raise

        if time.time() - start > timeout:
            raise RuntimeError(f"Timeout esperando eliminación del job {job_name}")

        time.sleep(1)


def run_job(job_name: str, job_manifest):
    batch = get_batch_client()

    try:
        batch.create_namespaced_job(
            namespace=NAMESPACE,
            body=job_manifest
        )
        return {"status": "created", "job": job_name}

    except client.exceptions.ApiException as e:
        if e.status != 409:
            raise

        batch.delete_namespaced_job(
            name=job_name,
            namespace=NAMESPACE,
            propagation_policy="Foreground"
        )

        wait_until_job_deleted(batch, job_name)

        batch.create_namespaced_job(
            namespace=NAMESPACE,
            body=job_manifest
        )

        return {"status": "restarted", "job": job_name}


def job_status(job_name: str):
    batch = get_batch_client()
    job = batch.read_namespaced_job(job_name, NAMESPACE)

    return {
        "name": job.metadata.name,
        "succeeded": job.status.succeeded,
        "failed": job.status.failed,
        "active": job.status.active
    }
