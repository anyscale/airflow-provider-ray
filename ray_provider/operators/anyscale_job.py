import time
from typing import Optional, Sequence
from ray_provider.utils import push_to_xcom

from airflow.utils.context import Context
from airflow.exceptions import AirflowException

from ray_provider.operators.base import AnyscaleBaseOperator
from ray_provider.sensors.anyscale_job import AnyscaleProductionJobSensor

from anyscale.shared_anyscale_utils.utils.byod import BYODInfo
from anyscale.sdk.anyscale_client.models.create_production_job import CreateProductionJob


class AnyscaleCreateProductionJobOperator(AnyscaleBaseOperator):
    """
    An operator that creates an Production Job.
    :param name: Name of the job. (templated)
    :param project_id: Id of the project this job will start clusters in. (templated)
    :param entrypoint: A script that will be run to start your job.
        This command will be run in the root directory of
        the specified runtime env. Eg. 'python script.py' (templated)
    :param build_id: The id of the cluster env build.
        This id will determine the docker image your job is run on. (templated)
    :param docker: Docker image for BYOD. (templated)
    :param max_retries: The number of retries this job will attempt on failure.
        Set to None to set infinite retries. (templated)
    :param description: Description of the job. (templated)
    :param runtime_env: A ray runtime env json. (templated)
        Your entrypoint will be run in the environment specified by this runtime env. (templated)
    :param compute_config_id: The id of the compute configuration that you want to use.
        This id will specify the resources required for your job. (templated)
    :param ray_version: Ray version (only used for BYOD). (templated) (default: "1.13.0")
    :param python_version: Python version (only used for BYOD). (templated) (default: "py38")
    :param wait_for_completion: If True, waits for creation of the cluster to complete. (default: True)
    :param poke_interval: Poke interval that the operator will use to check if the cluster is started. (default: 60)
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
    ]

    def __init__(
        self,
        name: str,
        project_id: str,
        entrypoint: str,
        build_id: str = None,
        docker: str = None,
        max_retries: int = 1,
        description: str = None,
        runtime_env: dict = None,
        compute_config_id: str = None,
        ray_version: Optional[str] = "1.13.0",
        python_version: Optional[str] = "py38",
        wait_for_completion: Optional[bool] = True,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.name = name
        self.docker = docker
        self.entrypoint = entrypoint
        self.project_id = project_id
        self.description = description
        self.max_retries = max_retries
        self.runtime_env = runtime_env
        self.compute_config_id = compute_config_id
        self.ray_version = ray_version
        self.python_version = python_version
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
                "at least build_id or docker must be provided.")

        return build_id

    def execute(self, context: Context) -> None:
        build_id = self._get_build_id()

        create_production_job = CreateProductionJob(
            name=self.name,
            description=self.description,
            project_id=self.project_id,
            config={
                "entrypoint": self.entrypoint,
                "build_id": build_id,
                "runtime_env": self.runtime_env,
                "compute_config_id": self.compute_config_id,
                "max_retries": self.max_retries,
            },
        )

        production_job = self.sdk.create_job(
            create_production_job).result

        self.log.info(f"production job {production_job.id} created")

        if self.wait_for_completion:
            while not AnyscaleProductionJobSensor(
                task_id="wait_job",
                production_job_id=production_job.id,
                auth_token=self.auth_token,
            ).poke(context):

                time.sleep(self.poke_interval)

        push_to_xcom(production_job.to_dict(), context,
                     ignore_keys=self._ignore_keys)
