import time

from typing import List, Optional, Sequence

from ray_provider.utils import push_to_xcom
from ray_provider.operators.base import AnyscaleBaseOperator
from ray_provider.sensors.anyscale_cluster import AnyscaleClusterSensor

from airflow.utils.context import Context
from airflow.exceptions import AirflowException

from anyscale.shared_anyscale_utils.utils.byod import BYODInfo
from anyscale.sdk.anyscale_client.models.cluster import Cluster


class AnyscaleCreateClusterOperator(AnyscaleBaseOperator):
    """
    An operator that creates a Cluster.

    :param name: Name of the Cluster. (templated)
    :param cluster_environment_build_id: Cluster Environment Build ID that the Cluster is using. (templated)
    :param docker: Docker image for BYOD. (templated)
    :param project_id: Project that this Cluster belongs to. If none, the Cluster will use the default Project. (templated)
    :param ray_version: Ray version (only used for BYOD). (templated) (default: "1.13.0")
    :param python_version: Python version (only used for BYOD). (templated) (default: "py38")
    :param compute_config_id: Cluster Compute that the Cluster is using. (templated)
    """

    template_fields: Sequence[str] = [
        "name",
        "auth_token",
        "cluster_environment_build_id",
        "docker",
        "project_id",
        "ray_version",
        "python_version",
        "compute_config_id",
    ]

    def __init__(
        self,
        *,
        name: str,
        cluster_environment_build_id: str = None,
        docker: str = None,
        project_id: str = None,
        ray_version: Optional[str] = "1.13.0",
        python_version: Optional[str] = "py38",
        compute_config_id: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.name = name
        self.project_id = project_id
        self.docker = docker
        self.cluster_environment_build_id = cluster_environment_build_id

        self.ray_version = ray_version
        self.python_version = python_version
        self.compute_config_id = compute_config_id

        self._ignore_keys = [
            "services_urls",
            "ssh_authorized_keys",
            "ssh_private_key",
            "user_service_token",
            "access_token",
        ]

    def _search_clusters(self) -> List[Cluster]:
        clusters_query = {
            "name": {
                "equals": self.name,
            },
            "project_id": self.project_id,
        }

        clusters: List[Cluster] = self.sdk.search_clusters(
            clusters_query=clusters_query).results
        return clusters

    def _get_cluster_environment_build_id(self) -> str:

        cluster_environment_build_id = None

        if self.docker:

            cluster_environment_build_id = BYODInfo(
                docker_image_name=self.docker,
                python_version=self.python_version,
                ray_version=self.ray_version,
            ).encode()

        if self.cluster_environment_build_id:
            if self.docker:
                self.log.info(
                    "docker is ignored when cluster_environment_build_id is provided.")

            cluster_environment_build_id = self.cluster_environment_build_id

        if cluster_environment_build_id is None:
            raise AirflowException(
                "at least cluster_environment_build_id or docker must be provided.")

        return cluster_environment_build_id

    def execute(self, context: Context) -> None:

        clusters = self._search_clusters()

        if clusters:
            self.log.info(
                "cluster with name %s in %s already exists", self.name, self.project_id)
            cluster = clusters[0].to_dict()
            push_to_xcom(cluster, context, self._ignore_keys)
            return

        cluster_environment_build_id = self._get_cluster_environment_build_id()

        create_cluster = {
            "name": self.name,
            "project_id": self.project_id,
            "cluster_compute_id": self.compute_config_id,
            "cluster_environment_build_id": cluster_environment_build_id,
        }

        cluster: Cluster = self.sdk.create_cluster(create_cluster).result

        self.log.info("cluster created with id: %s", cluster.id)
        push_to_xcom(cluster.to_dict(), context, self._ignore_keys)


class AnyscaleStartClusterOperator(AnyscaleBaseOperator):
    """
    An operator that starts a cluster.

    :param cluster_id: ID of the Cluster to start. (templated)
    :param start_cluster_options: Options to set when starting a cluster. (templated)
    :param wait_for_completion: If True, waits for creation of the cluster to complete. (default: True)
    :param poke_interval: Poke interval that the operator will use to check if the cluster is started. (default: 60)
    """

    template_fields: Sequence[str] = [
        "auth_token",
        "cluster_id",
        "start_cluster_options"
    ]

    def __init__(
        self,
        *,
        cluster_id: str,
        start_cluster_options: Optional[dict] = None,
        wait_for_completion: Optional[bool] = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cluster_id = cluster_id

        self.start_cluster_options = start_cluster_options

        if self.start_cluster_options is None:
            self.start_cluster_options = {}

        self.wait_for_completion = wait_for_completion

        self._ignore_keys = []

    def execute(self, context: Context) -> None:

        self.log.info("starting cluster %s", self.cluster_id)

        cluster_operation = self.sdk.start_cluster(
            cluster_id=self.cluster_id,
            start_cluster_options=self.start_cluster_options
        ).result

        if self.wait_for_completion:
            while not AnyscaleClusterSensor(
                task_id="wait_cluster",
                cluster_id=self.cluster_id,
                auth_token=self.auth_token,
            ).poke(context):

                time.sleep(self.poke_interval)

        push_to_xcom(cluster_operation.to_dict(), context, self._ignore_keys)


class AnyscaleTerminateClusterOperator(AnyscaleBaseOperator):
    """
    An operator that initializes workflow to transition the Cluster into the Terminated state.
    :param cluster_id: ID of the Cluster to terminate. (templated)
    :param terminate_cluster_options: Options to set when terminating a Cluster. (templated)
    :param wait_for_completion: If True, waits for creation of the cluster to complete. (default: True)
    :param poke_interval: Poke interval that the operator will use to check if the cluster is terminated. (default: 60)
    """

    template_fields: Sequence[str] = [
        "cluster_id",
        "auth_token",
        "terminate_cluster_options",
    ]

    def __init__(
        self,
        *,
        cluster_id: str,
        terminate_cluster_options: Optional[dict] = None,
        wait_for_completion: Optional[bool] = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cluster_id = cluster_id

        self.terminate_cluster_options = terminate_cluster_options

        if self.terminate_cluster_options is None:
            self.terminate_cluster_options = {}

        self.wait_for_completion = wait_for_completion
        self._ignore_keys = []

    def execute(self, context: Context) -> None:

        cluster_operation = self.sdk.terminate_cluster(
            cluster_id=self.cluster_id,
            terminate_cluster_options=self.terminate_cluster_options).result

        self.log.info("terminating cluster %s", self.cluster_id)

        if self.wait_for_completion:
            while not AnyscaleClusterSensor(
                task_id="wait_cluster",
                cluster_id=self.cluster_id,
                auth_token=self.auth_token,
            ).poke(context):

                time.sleep(self.poke_interval)

        push_to_xcom(cluster_operation.to_dict(), context, self._ignore_keys)
