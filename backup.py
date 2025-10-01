import kopf
from kubernetes import client, config

config.load_incluster_config()
v1 = client.CoreV1Api()
batch_v1 = client.BatchV1Api()
rbac_v1 = client.RbacAuthorizationV1Api()
cronjob_image = "inspirehep/cronjob-controller"


def create_service_account(v1, namespace):
    try:
        v1.read_namespaced_service_account("cronjob-service-account", namespace)
        # service account exists - continue
    except client.exceptions.ApiException as e:
        # create service account for the cronjob if doesn't exist
        if e.status == 404:
            service_account = client.V1ServiceAccount(
                metadata=client.V1ObjectMeta(name="cronjob-service-account")
            )
            v1.create_namespaced_service_account(
                namespace=namespace, body=service_account
            )
        else:
            raise e


def create_rolebinding(rbac_v1, namespace):
    try:
        rbac_v1.read_namespaced_role_binding("cronjob-role-binding", namespace)
    except client.exceptions.ApiException as e:
        if e.status == 404:
            role_binding = client.V1RoleBinding(
                metadata=client.V1ObjectMeta(name="cronjob-role-binding"),
                subjects=[
                    client.V1Subject(
                        kind="ServiceAccount", name="cronjob-service-account"
                    )
                ],
                role_ref=client.V1RoleRef(
                    kind="ClusterRole",
                    name="backup-operator-cronjob-role",
                    api_group="rbac.authorization.k8s.io",
                ),
            )
            rbac_v1.create_namespaced_role_binding(
                namespace=namespace, body=role_binding
            )
        else:
            raise e


def container_env(client, spec, cronjob_name):
    buckets = spec["buckets"]
    buckets_string = ",".join(buckets)
    jobs = str(spec["jobs"])
    dry_run = str(spec["dry-run"])
    env = [
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
            value=spec["source"]["endpoint"],
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
            value=spec["destination"]["endpoint"],
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
                    name=spec["destination"]["secretName"],
                    key="RCLONE_CONFIG_S3_ACCESS_KEY_ID",
                ),
            ),
        ),
        client.V1EnvVar(
            name="RCLONE_CONFIG_S3_SECRET_ACCESS_KEY",
            value_from=client.V1EnvVarSource(
                secret_key_ref=client.V1SecretKeySelector(
                    name=spec["destination"]["secretName"],
                    key="RCLONE_CONFIG_S3_SECRET_ACCESS_KEY",
                ),
            ),
        ),
        client.V1EnvVar(name="MEMORY_LIMIT", value=spec["limit"]["memory"]),
        client.V1EnvVar(name="CPU_LIMIT", value=spec["limit"]["cpu"]),
        client.V1EnvVar(name="MEMORY_REQUEST", value=spec["request"]["memory"]),
        client.V1EnvVar(name="CPU_REQUEST", value=spec["request"]["cpu"]),
        client.V1EnvVar(name="BUCKET_LIST", value=buckets_string),
        client.V1EnvVar(name="PARENT_NAME", value=cronjob_name),
        client.V1EnvVar(name="DRY_RUN", value=dry_run),
        client.V1EnvVar(
            name="NAMESPACE",
            value_from=client.V1EnvVarSource(
                field_ref=client.V1ObjectFieldSelector(field_path="metadata.namespace")
            ),
        ),
        client.V1EnvVar(name="TOTAL_JOBS", value=jobs),
    ]
    return env


def container_specs(client, spec, cronjob_name):
    containers = [
        client.V1Container(
            name="backup",
            image=f"{cronjob_image}:482f34eeeab307ec6cc7a0a9480d30b7a3bdd066",
            # resources=client.V1ResourceRequirements(
            #     limits={
            #         "cpu": "2",
            #         "memory": "2Gi",
            #     },
            #     requests={
            #         "cpu": "1",
            #         "memory": "1Gi",
            #     },
            # ),
            env=container_env(client, spec, cronjob_name),
        )
    ]
    return containers


@kopf.on.create("s3-Backup")
def create_cronjob(spec, body, **kwargs):
    namespace = body["metadata"]["namespace"]
    create_service_account(v1, namespace)
    create_rolebinding(rbac_v1, namespace)

    job_name = body["metadata"]["name"]
    cron_job_name = job_name + "-cronjob"

    # Define the CronJob object
    cron_job = client.V1CronJob(
        api_version="batch/v1",
        kind="CronJob",
        metadata=client.V1ObjectMeta(name=cron_job_name),
        spec=client.V1CronJobSpec(
            schedule=spec["schedule"],
            suspend=spec["suspend"],
            concurrency_policy="Forbid",
            job_template=client.V1JobTemplateSpec(
                spec=client.V1JobSpec(
                    template=client.V1PodTemplateSpec(
                        spec=client.V1PodSpec(
                            service_account_name="cronjob-service-account",
                            containers=container_specs(
                                client,
                                spec,
                                cron_job_name,
                            ),
                            restart_policy="Never",
                        )
                    )
                )
            ),
        ),
    )

    # cleanup the cronjob when CRD is deleted
    kopf.adopt(cron_job)
    # Create the CronJob
    batch_v1.create_namespaced_cron_job(namespace=namespace, body=cron_job)
    return {"message": f"CronJob {cron_job_name} created"}


@kopf.on.update("s3-Backup")
def update_cronjob(spec, body, **kwargs):
    cron_job_name = body["metadata"]["name"] + "-cronjob"
    namespace = body["metadata"]["namespace"]
    try:
        cronjob = batch_v1.read_namespaced_cronjob_job(
            name=cron_job_name, namespace=namespace
        )
    except client.rest.ApiException as e:
        print(f"Exception when reading cronjob: {e}")
        return

    # update cronjob specs
    cronjob.spec.job_template.spec.template.spec.container = container_specs(
        client, spec, cron_job_name
    )

    try:
        batch_v1.patch_namespaced_cron_job(
            name=cron_job_name, namespace=namespace, body=cronjob
        )
        print(f"Cronjob {cron_job_name} in namespace {namespace} has been updated")
    except client.rest.ApiException as e:
        print(f"Exception when handling patch {e}")
        return
