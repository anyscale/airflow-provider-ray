import time
from typing import Optional, Sequence

from airflow.utils.context import Context
from ray_provider.utils import push_to_xcom
from ray_provider.operators.base import AnyscaleBaseOperator

from ray_provider.sensors.anyscale_session_command import AnyscaleSessionCommandSensor

_POKE_INTERVAL = 60


class AnyscaleCreateSessionCommandOperator(AnyscaleBaseOperator):
    """
    An Operator that creates and executes a shell command on a session.
        Makes no assumption about the details of the shell command.
    :param session_id: ID of the Session to execute this command on. (templated)
    :param shell_command: Shell command string that will be executed. (templated)
    :param wait_for_completion: If True, waits for creation of the cluster to complete. (default: True)
    """

    template_fields: Sequence[str] = [
        "session_id",
        "auth_token",
        "shell_command",
    ]

    def __init__(
        self,
        *,
        session_id: str,
        shell_command: str,
        wait_for_completion: Optional[bool] = True,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.session_id = session_id
        self.shell_command = shell_command
        self.wait_for_completion = wait_for_completion
        self._ignore_keys = []

    def execute(self, context: Context):

        create_session_command = {
            "session_id": self.session_id,
            "shell_command": self.shell_command,
        }

        session_command_response = self.sdk.create_session_command(
            create_session_command).result

        self.log.info("session command with id %s created",
                      session_command_response.id)

        if self.wait_for_completion:
            while not AnyscaleSessionCommandSensor(
                task_id="wait_session_command",
                session_command_id=session_command_response.id,
                auth_token=self.auth_token,
            ).poke(context):

                time.sleep(_POKE_INTERVAL)

        push_to_xcom(session_command_response.to_dict(),
                     context, self._ignore_keys)
