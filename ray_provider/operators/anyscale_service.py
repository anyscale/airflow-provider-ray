import time
from typing import Optional, Sequence

from airflow.utils.context import Context
from airflow.exceptions import AirflowException
from airflow.utils.log.secrets_masker import mask_secret

from ray_provider.utils import push_to_xcom
from ray_provider.operators.base import AnyscaleBaseOperator
from ray_provider.sensors.anyscale_service import AnyscaleServiceSensor

from anyscale.shared_anyscale_utils.utils.byod import BYODInfo
from anyscale.sdk.anyscale_client.models.create_production_service import CreateProductionService

_POKE_INTERVAL = 60


class AnyscaleApplyServiceOperator(AnyscaleBaseOperator):
    """
    An Operator that puts a service. This operator will create a service
        with the given name if it doesn't exist, and will otherwise update the service.
    :param name: Name of the service. (templated)
    :param project_id: Id of the project this job will start clusters in. (templated)
    :param entrypoint: A script that will be run to start your service.
        This command will be run in the root directory of the specified runtime env. Eg. 'python script.py'. (templated)
    :param healthcheck_url: Healthcheck url. (templated)
    :param build_id: The id of the cluster env build.
        This id will determine the docker image your service is run on. (templated)
    :param docker: Docker image for BYOD. (templated)
    :param max_retries: The number of retries this job will attempt on failure.
        Set to None to set infinite retries. (templated)
    :param access: Whether service can be accessed by public internet traffic.
        Possible values: ['private', 'public'] (templated)
    :param description: Description of the Service. (templated)
    :param runtime_env: A ray runtime env json. (templated)
        Your entrypoint will be run in the environment specified by this runtime env. (templated)
    :param compute_config_id: The id of the compute configuration that you want to use.
        This id will specify the resources required for your job. (templated)
    :param ray_version: Ray version (only used for BYOD). (templated)
    :param python_version: Python version (only used for BYOD). (templated)
    :param wait_for_completion: If True, waits for creation of the service to complete. (default: True)
    """

    template_fields: Sequence[str] = [
        "name",
        "auth_token",
        "project_id",
        "entrypoint",
        "build_id",
        "docker",
        "description",
        "runtime_env",
        "compute_config_id",
        "ray_version",
        "python_version",
        "access",
    ]

    def __init__(
        self,
        name: str,
        project_id: str,
        entrypoint: str,
        healthcheck_url: str,
        build_id: str = None,
        docker: str = None,
        max_retries: int = 0,
        access: str = "private",
        description: str = None,
        runtime_env: dict = None,
        compute_config_id: str = None,
        ray_version: Optional[str] = None,
        python_version: Optional[str] = None,
        wait_for_completion: Optional[bool] = True,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.name = name
        self.docker = docker
        self.access = access
        self.entrypoint = entrypoint
        self.project_id = project_id
        self.description = description
        self.max_retries = max_retries
        self.runtime_env = runtime_env
        self.healthcheck_url = healthcheck_url
        self.compute_config_id = compute_config_id
        self.ray_version = ray_version or "1.13.0"
        self.python_version = python_version or "py38"
        self.build_id = build_id

        self.wait_for_completion = wait_for_completion
        self._ignore_keys = []

    def _get_build_id(self) -> str:

        build_id = None

        if self.docker:

            build_id = BYODInfo(
                docker_image_name=self.docker,
                python_version=self.python_version,
                ray_version=self.ray_version,
            ).encode()

        if self.build_id:
            if self.docker:
                self.log.info(
                    "docker is ignored when cluster_environment_build_id is provided.")

            build_id = self.build_id

        if build_id is None:
            raise AirflowException(
                "at least cluster_environment_build_id or docker must be provided.")

        return build_id

    def execute(self, context: Context) -> None:

        build_id = self._get_build_id()

        create_production_service = CreateProductionService(
            name=self.name,
            access=self.access,
            description=self.description,
            project_id=self.project_id,
            healthcheck_url=self.healthcheck_url,
            config={
                "entrypoint": self.entrypoint,
                "build_id": build_id,
                "runtime_env": self.runtime_env,
                "compute_config_id": self.compute_config_id,
                "max_retries": self.max_retries,
            },
        )

        production_service = self.sdk.apply_service(
            create_production_service).result

        self.log.info("production service %s created", production_service.id)

        if self.wait_for_completion:
            while not AnyscaleServiceSensor(
                task_id="wait_service",
                service_id=production_service.id,
                auth_token=self.auth_token,
            ).poke(context):

                time.sleep(_POKE_INTERVAL)

            self.log.info("service available at %s", production_service.url)

        xcom_payload = production_service.to_dict()
        xcom_payload["token"] = mask_secret(xcom_payload["token"])
        push_to_xcom(xcom_payload, context, self._ignore_keys)
