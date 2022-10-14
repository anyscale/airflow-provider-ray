from typing import Sequence

from airflow.utils.context import Context
from airflow.exceptions import AirflowException
from ray_provider.sensors.base import AnyscaleBaseSensor


class AnyscaleProductionJobSensor(AnyscaleBaseSensor):
    """
    A Sensor that pokes the state of a production job and returns when it reaches goal state.
    :param production_job_id: ID of the production job. (templated)
    """

    template_fields: Sequence[str] = [
        "production_job_id",
        "auth_token",
    ]

    def __init__(
        self,
        *,
        production_job_id: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.production_job_id = production_job_id

    def _fetch_logs(self):
        result = self.sdk.get_production_job_logs(
            self.production_job_id).result

        return result.logs

    def poke(self, context: Context) -> bool:

        production_job = self.sdk.get_production_job(
            production_job_id=self.production_job_id).result

        state = production_job.state

        self.log.info("current state: %s, goal state %s",
                      state.current_state, state.goal_state)

        operation_message = state.operation_message
        if operation_message:
            self.log.info(operation_message)

        if state.current_state in ("OUT_OF_RETRIES", "TERMINATED", "ERRORED"):
            raise AirflowException(
                "job ended with status {}, error: {}".format(
                    state.current_state,
                    state.error,
                )
            )

        if state.current_state != state.goal_state:
            return False

        self.log.info(
            "job %s reached goal state %s", state.production_job_id, state.goal_state)

        took = state.state_transitioned_at - production_job.created_at

        self.log.info("duration: %s", took.total_seconds())

        try:
            logs = self._fetch_logs()
            self.log.info("logs: \n %s", logs)

        except Exception:
            self.log.warning("logs not found for %s", self.production_job_id)

        return True
