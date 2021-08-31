from airflow.models import BaseOperator
from hooks.ray_cli import RayCliHook

from typing import List


class RayCliOperator(BaseOperator):
    """Run Ray CLI commands."""

    ui_color = "#08a5ec"
    
    template_fields = (
        "cluster_config",
        "script",
    )

    def __init__(
        self,
        command: str,
        options: List[str] = None,
        cluster_config: str = None,
        script: str = None,
        script_args: List[str] = None,
        aws_conn_id: str = "aws_default",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.command = command
        self.options = options
        self.cluster_config = cluster_config
        self.script = script
        self.script_args = script_args
        self.aws_conn_id = aws_conn_id

    def get_hook(self):
        return RayCliHook(
            command=self.command,
            options=self.options,
            cluster_config=self.cluster_config,
            script=self.script,
            script_args=self.script_args,
            aws_conn_id=self.aws_conn_id,
        )

    def execute(self, context):
        self.hook = self.get_hook()
        self.hook.run()
