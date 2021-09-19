from airflow.models import BaseOperator
from hooks.ray import RayHook

from typing import List, Union, Optional


class RayStartClusterOperator(BaseOperator):
    """Start Ray cluster."""

    ui_color = "#08a5ec"
    
    template_fields = (
        "cluster_config",
    )

    def __init__(
        self,
        cluster_config: Union[dict, str],
        aws_conn_id: Optional[str] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.cluster_config = cluster_config
        self.aws_conn_id = aws_conn_id

    def get_hook(self):
        return RayHook(
            cluster_config=self.cluster_config,
            aws_conn_id=self.aws_conn_id,
        )

    def execute(self, context):
        self.hook = self.get_hook()
        self.hook.start_cluster()
