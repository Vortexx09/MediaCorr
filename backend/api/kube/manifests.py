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
    command: list[str],
    parallelism: int = 1,
    env: dict | None = None,
):
    env_vars = []
    if env:
        env_vars = [
            client.V1EnvVar(name=k, value=str(v))
            for k, v in env.items()
        ]

    return client.V1Job(
        metadata=client.V1ObjectMeta(name=name),
        spec=client.V1JobSpec(
            completions=parallelism,
            parallelism=parallelism,
            completion_mode="Indexed",
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
                            env=env_vars,
                            volume_mounts=[data_volume_mount()],
                        )
                    ],
                    volumes=[data_volume()],
                )
            ),
        ),
    )


# ---------- JOB DEFINITIONS ----------
def icolcap_job(_start, _end):
    return base_job(
        name="icolcap-job",
        image="mediacorr-icolcap:latest",
        command=["python", "-m", "app.icolcap"],
        env={
            "START": _start,
            "END": _end
        }
    )


def sources_job(_parallelism, _from, _to, _records):
    return base_job(
        name="sources-job",
        image="mediacorr-sources:latest",
        command=["python", "-m", "app.sources"],
        parallelism=int(_parallelism),
        env={
            "FROM_YEAR": _from,
            "TO_YEAR": _to,
            "MAX_RECORDS": _records
        }
    )

def ingestor_job(_parallelism):
    return base_job(
        name="ingestor-job",
        image="mediacorr-ingestor:latest",
        command=["python", "-m", "app.ingestor"],
        parallelism=int(_parallelism)
    )

def filter_job(_parallelism):
    return base_job(
        name="filter-job",
        image="mediacorr-filter:latest",
        command=["python", "-m", "app.filter"],
        parallelism=int(_parallelism)
    )


def classifier_job(_parallelism):
    return base_job(
        name="classifier-job",
        image="mediacorr-classifier:latest",
        command=["python", "-m", "app.classifier"],
        parallelism=int(_parallelism)
    )


def correlator_job(_parallelism):
    return base_job(
        name="correlator-job",
        image="mediacorr-correlator:latest",
        command=["python", "-m", "app.correlator"],
        parallelism=int(_parallelism)
    )
