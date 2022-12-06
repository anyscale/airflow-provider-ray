from typing import Sequence

from airflow.utils.context import Context
from ray_provider.sensors.base import AnyscaleBaseSensor


class AnyscaleClusterSensor(AnyscaleBaseSensor):
    """
    A Sensor that pokes the state of a cluster and returns when it reaches goal state.

    :param cluster_id: ID of the Cluster to retreive. (templated)
    """

    template_fields: Sequence[str] = [
        "auth_token",
        "cluster_id",
    ]

    def __init__(
        self,
        *,
        cluster_id: str,
        **kwargs,
    ):

        super().__init__(**kwargs)
        self.cluster_id = cluster_id

    def _log_services(self, response):
        services = response.result.services_urls

        if services:
            self.log.info("service urls:")
            for name, service in services.to_dict().items():
                self.log.info("%s: %s", name, service)

    def _log_head_node(self, response):
        head_node_info = response.result.head_node_info

        if head_node_info:
            self.log.info("head node info:")
            for name, info in head_node_info.to_dict().items():
                self.log.info("%s: %s", name, info)

    def poke(self, context: Context) -> bool:

        response = self.sdk.get_cluster(self.cluster_id)

        state = response.result.state
        goal_state = response.result.goal_state

        self.log.info("current state: %s, goal state: %s", state, goal_state)

        if goal_state is not None and goal_state != state:
            return False

        self.log.info("cluster reached goal state: %s", state)
        self._log_head_node(response)
        self._log_services(response)

        return True
