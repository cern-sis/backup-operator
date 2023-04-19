import kopf
from kubernetes import client, config


@kopf.on.create("Backup")
def create_cronjob(spec, body, **kwargs):
    config.load_incluster_config()
    api = client.BatchV1beta1Api()
    v1 = client.CoreV1Api()
    rbac_v1 = client.RbacAuthorizationV1Api()
    # create service account and rolebinding for the cronjob
    service_account = client.V1ServiceAccount(
        metadata=client.V1ObjectMeta(name="cronjob-service-account")
    )
    v1.create_namespaced_service_account(
        namespace=spec["namespace"], body=service_account
    )

    # define role binding
    role_binding = client.V1RoleBinding(
        metadata=client.V1ObjectMeta(name="cronjob-role-binding"),
        subjects=[
            client.V1Subject(kind="ServiceAccount", name="cronjob-service-account")
        ],
        role_ref=client.V1RoleRef(
            kind="ClusterRole",
            name="backup-operator-cronjob-role",
            api_group="rbac.authorization.k8s.io",
        ),
    )

    rbac_v1.create_namespaced_role_binding(
        namespace=spec["namespace"], body=role_binding
    )
    job_name = body["metadata"]["name"]
    cron_job_name = job_name + "-cronjob"

    buckets = spec["buckets"]
    buckets_string = ",".join(buckets)
    # Define the CronJob object
    cron_job = client.V1beta1CronJob(
        api_version="batch/v1beta1",
        kind="CronJob",
        metadata=client.V1ObjectMeta(name=cron_job_name),
        spec=client.V1beta1CronJobSpec(
            schedule=spec["schedule"],
            concurrency_policy="Forbid",
            job_template=client.V1beta1JobTemplateSpec(
                spec=client.V1JobSpec(
                    template=client.V1PodTemplateSpec(
                        spec=client.V1PodSpec(
                            service_account_name="cronjob-service-account",
                            containers=[
                                client.V1Container(
                                    name="backup",
                                    image="inspirehep/cronjob-controller:d7e175a08f49f935344764a52e9ddc17a0ae98c6",
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
                                            name="INVENIO_S3_ACCESS_KEY",
                                            value_from=client.V1EnvVarSource(
                                                secret_key_ref=client.V1SecretKeySelector(
                                                    name=spec["source"]["secretName"],
                                                    key="INVENIO_S3_ACCESS_KEY",
                                                ),
                                            ),
                                        ),
                                        client.V1EnvVar(
                                            name="INVENIO_S3_SECRET_KEY",
                                            value_from=client.V1EnvVarSource(
                                                secret_key_ref=client.V1SecretKeySelector(
                                                    name=spec["source"]["secretName"],
                                                    key="INVENIO_S3_SECRET_KEY",
                                                ),
                                            ),
                                        ),
                                        client.V1EnvVar(
                                            name="RCLONE_CONFIG_S3_ACCESS_KEY_ID",
                                            value_from=client.V1EnvVarSource(
                                                secret_key_ref=client.V1SecretKeySelector(
                                                    name=spec["destination"][
                                                        "secretName"
                                                    ],
                                                    key="RCLONE_CONFIG_S3_ACCESS_KEY_ID",
                                                ),
                                            ),
                                        ),
                                        client.V1EnvVar(
                                            name="RCLONE_CONFIG_S3_SECRET_ACCESS_KEY",
                                            value_from=client.V1EnvVarSource(
                                                secret_key_ref=client.V1SecretKeySelector(
                                                    name=spec["destination"][
                                                        "secretName"
                                                    ],
                                                    key="RCLONE_CONFIG_S3_SECRET_ACCESS_KEY",
                                                ),
                                            ),
                                        ),
                                        client.V1EnvVar(
                                            name="BUCKET_LIST", value=buckets_string
                                        ),
                                        client.V1EnvVar(
                                            name="DRY_RUN", value=spec["dry-run"]
                                        ),
                                        client.V1EnvVar(
                                            name="NAMESPACE", value=spec["namespace"]
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
