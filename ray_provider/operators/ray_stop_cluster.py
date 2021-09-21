from airflow.models import BaseOperator
from hooks.ray_cli import RayCliHook, Command

from typing import List, Optional


class RayStopClusterOperator(BaseOperator):
    """Stop Ray cluster.

    :param command: [description]
    :type command: str
    :param script: [description]
    :type script: str
    :param script_args: [description], defaults to []
    :type script_args: Optional[List[str]], optional
    :param options: [description], defaults to []
    :type options: Optional[List[str]], optional
    :param ray_conn_id: [description], defaults to "ray_default"
    :type ray_conn_id: Optional[str], optional
    :param aws_conn_id: [description], defaults to "aws_default"
    :type aws_conn_id: Optional[str], optional
    :param cluster_config_overrides: [description], defaults to {}
    :type cluster_config_overrides: Optional[dict], optional
    :param verbose: [description], defaults to True
    :type verbose: Optional[bool], optional
    """

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
        verbose: Optional[bool] = True,
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
        self.verbose = verbose

    def get_hook(self):
        return RayCliHook(
            command=Command.DOWN,
            script=self.script,
            script_args=self.script_args,
            options=self.options,
            ray_conn_id=self.ray_conn_id,
            aws_conn_id=self.aws_conn_id,
            cluster_config_overrides=self.cluster_config_overrides,
            verbose=self.verbose,
        )

    def execute(self, context):
        self.hook = self.get_hook()
        self.hook.run_cli()
