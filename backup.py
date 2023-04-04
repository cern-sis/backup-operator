import logging

import kopf
from kubernetes import client, config


@kopf.on.create("Backup")
def create_cronjob(spec, body, **kwargs):
    config.load_incluster_config()
    api = client.BatchV1beta1Api()

    job_name = body["metadata"]["name"]
    cron_job_name = job_name + "-cronjob"

    buckets = spec["buckets"]
    logging.info(buckets)
    # Define the CronJob object
    cron_job = client.V1beta1CronJob(
        api_version="batch/v1beta1",
        kind="CronJob",
        metadata=client.V1ObjectMeta(name=cron_job_name),
        spec=client.V1beta1CronJobSpec(
            schedule=spec["schedule"],
            job_template=client.V1beta1JobTemplateSpec(
                spec=client.V1JobSpec(
                    template=client.V1PodTemplateSpec(
                        spec=client.V1PodSpec(
                            containers=[
                                client.V1Container(
                                    name="backup",
                                    image="inspirehep/cronjob-controller:f37d5e03b360932c96a94a9f6ec8fb7ea36e7e3d",
                                    resources=client.V1ResourceRequirements(
                                        limits={
                                            "cpu": spec["jobResources"]["cpu"],
                                            "memory": spec["jobResources"]["memory"],
                                        },
                                        requests={
                                            "cpu": spec["jobResources"]["cpu"],
                                            "memory": spec["jobResources"]["memory"],
                                        },
                                    ),
                                    env=[
                                        client.V1EnvVar(
                                            name="RCLONE_CONFIG_MEYRIN_TYPE",
                                            value=spec["source"]["remoteType"],
                                        ),
                                        client.V1EnvVar(
                                            name="RCLONE_CONFIG_MEYRIN_PROVIDER",
                                            value=spec["source"]["provider"],
                                        ),
                                        client.V1EnvVar(
                                            name="RCLONE_CONFIG_MEYRIN_ENDPOINT",
                                            value=spec["source"]["endPoint"],
                                        ),
                                        client.V1EnvVar(
                                            name="RCLONE_CONFIG_S3_TYPE",
                                            value=spec["destination"]["remoteType"],
                                        ),
                                        client.V1EnvVar(
                                            name="RCLONE_CONFIG_S3_PROVIDER",
                                            value=spec["destination"]["provider"],
                                        ),
                                        client.V1EnvVar(
                                            name="RCLONE_CONFIG_S3_ENDPOINT",
                                            value=spec["destination"]["endPoint"],
                                        ),
                                        client.V1EnvVar(
                                            name="MEYRIN_SECRET_KEY",
                                            value=spec["source"]["secretName"],
                                        ),
                                        client.V1EnvVar(
                                            name="PREVESSIN_SECRET_KEY",
                                            value=spec["destination"]["secretName"],
                                        ),
                                        client.V1EnvVar(
                                            name="BUCKET_LIST", value=["buckets"]
                                        ),
                                    ],
                                )
                            ],
                            restart_policy="Never",
                        )
                    )
                )
            ),
        ),
    )

    # Create the CronJob
    api.create_namespaced_cron_job(namespace=spec["namespace"], body=cron_job)
    return {"message": "CronJob {} created".format(cron_job_name)}
