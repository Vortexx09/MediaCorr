from kubernetes import client, config

def get_batch_client():
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config()

    return client.BatchV1Api()
