from airflow.models import BaseOperator
from hooks.ray import RayHook

from typing import List, Union, Optional


class RayStopClusterOperator(BaseOperator):
    """Stop Ray cluster."""

    ui_color = "#08a5ec"
    
    template_fields = (
        "cluster_config",
        "script",
        "script_args",
    )

    def __init__(
        self,
        cluster_config: Union[dict, str],
        script: str,
        script_args: Optional[List[str]] = None,
        aws_conn_id: Optional[str] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.cluster_config = cluster_config
        self.script = script
        self.script_args = script_args
        self.aws_conn_id = aws_conn_id

    def get_hook(self):
        return RayHook(
            cluster_config=self.cluster_config,
            script=self.script,
            script_args=self.script_args,
            aws_conn_id=self.aws_conn_id,
        )

    def execute(self, context):
        self.hook = self.get_hook()
        self.hook.stop_cluster()
