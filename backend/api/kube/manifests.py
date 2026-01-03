from kubernetes import client

PVC_NAME = "mediacorr-pvc"


# ---------- COMMON HELPERS ----------

def data_volume():
    return client.V1Volume(
        name="data-volume",
        persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
            claim_name=PVC_NAME
        )
    )


def data_volume_mount():
    return client.V1VolumeMount(
        name="data-volume",
        mount_path="/app/data"
    )


def base_job(
    name: str,
    image: str,
    command: list[str]
):
    return client.V1Job(
        metadata=client.V1ObjectMeta(name=name),
        spec=client.V1JobSpec(
            backoff_limit=1,
            template=client.V1PodTemplateSpec(
                spec=client.V1PodSpec(
                    restart_policy="Never",
                    containers=[
                        client.V1Container(
                            name=name,
                            image=image,
                            image_pull_policy="IfNotPresent",
                            command=command,
                            volume_mounts=[data_volume_mount()]
                        )
                    ],
                    volumes=[data_volume()]
                )
            )
        )
    )


# ---------- JOB DEFINITIONS ----------

def sources_job():
    return base_job(
        name="sources-job",
        image="mediacorr-sources:latest",
        command=["python", "-m", "app.sources"]
    )


def ingestor_job():
    return base_job(
        name="ingestor-job",
        image="mediacorr-ingestor:latest",
        command=["python", "-m", "app.ingestor"]
    )


def filter_job():
    return base_job(
        name="filter-job",
        image="mediacorr-filter:latest",
        command=["python", "-m", "app.filter"]
    )


def classifier_job():
    return base_job(
        name="classifier-job",
        image="mediacorr-classifier:latest",
        command=["python", "-m", "app.classifier"]
    )


def correlator_job():
    return base_job(
        name="correlator-job",
        image="mediacorr-correlator:latest",
        command=["python", "-m", "app.correlator"]
    )


def icolcap_job():
    return base_job(
        name="icolcap-job",
        image="mediacorr-icolcap:latest",
        command=["python", "-m", "app.icolcap"]
    )
