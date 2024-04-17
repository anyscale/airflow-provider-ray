from typing import Sequence

from airflow.utils.context import Context

from airflow.exceptions import AirflowException
from ray_provider.sensors.base import AnyscaleBaseSensor


class AnyscaleSessionCommandSensor(AnyscaleBaseSensor):
    """
    A Sensor that pokes the state of a session command and returns when it reaches goal state.
    :param session_command_id: ID of the Session Command to retrieve.	
    """

    template_fields: Sequence[str] = [
        "session_command_id",
        "auth_token",
    ]

    def __init__(
        self,
        session_command_id: str,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.session_command_id = session_command_id

    def poke(self, context: Context) -> bool:

        session_command = self.sdk.get_session_command(
            self.session_command_id).result

        status_code = session_command.status_code

        if status_code is None:
            return False

        took = session_command.finished_at - session_command.created_at

        self.log.info("duration: %s", took.total_seconds())
        self.log.info(
            "session command %s ended with status code %s",
            self.session_command_id,
            session_command.status_code
        )

        if status_code != 0:
            raise AirflowException("session command ended with errors")

        return True
