import logging

import kopf
from kubernetes import client, config


@kopf.on.create("Backup")
def create_cronjob(spec, body, **kwargs):
    config.load_incluster_config()
    api = client.BatchV1beta1Api()

    job_name = body["metadata"]["name"]
    cron_job_name = job_name + "-cronjob"

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
    logging.info("cronjob is created")

    return {"message": "CronJob {} created".format(cron_job_name)}
