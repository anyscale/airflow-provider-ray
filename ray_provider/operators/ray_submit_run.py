from airflow.models import BaseOperator
from hooks.ray_cli import RayCliHook, Command

from typing import List, Optional


class RaySubmitRunOperator(BaseOperator):
    """Submit run on a Ray cluster."""

    ui_color = "#08a5ec"

    template_fields = (
        "script",
        "script_args",
        "options",
        "cluster_config_overrides",
    )

    def __init__(
        self,
        command: str,
        script: str,
        script_args: Optional[List[str]] = [],
        options: Optional[List[str]] = [],
        ray_conn_id: Optional[str] = "ray_default",
        aws_conn_id: Optional[str] = "aws_default",
        cluster_config_overrides: Optional[dict] = {},
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.command = command
        self.script = script
        self.script_args = script_args
        self.options = options
        self.ray_conn_id = ray_conn_id
        self.aws_conn_id = aws_conn_id
        self.cluster_config_overrides = cluster_config_overrides

    def get_hook(self):
        return RayCliHook(
            command=Command.SUBMIT,
            script=self.script,
            script_args=self.script_args,
            options=self.options,
            ray_conn_id=self.ray_conn_id,
            aws_conn_id=self.aws_conn_id,
            cluster_config_overrides=self.cluster_config_overrides,
        )

    def execute(self, context):
        self.hook = self.get_hook()
        self.hook.run_cli()
