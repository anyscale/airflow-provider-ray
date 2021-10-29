from airflow.models import BaseOperator
from hooks.ray_cli import RayCliHook, Command

from typing import List, Optional


class RaySubmitRunOperator(BaseOperator):
    """Submit run on a Ray cluster.

    :param script: Location of your ray python script. If you are storing this value in s3, please include an
            aws_conn_id Connection for accessing s3.
    :type script: str
    :param script_args: List of arguments for your python script, defaults to []
    :type script_args: Optional[List[str]], optional
    :param options: Options for your ray command. For documentation on options please look here
            https://docs.ray.io/en/latest/package-ref.html#the-ray-command-line-api, defaults to []
    :type options: Optional[List[str]], optional
    :param ray_conn_id: Connection ID for accessing ray cluster config from extra field as json, defaults to "ray_default"
    :type ray_conn_id: Optional[str], optional
    :param aws_conn_id: Connection ID for accessing s3 to retrieve script, defaults to "aws_default"
    :type aws_conn_id: Optional[str], optional
    :param cluster_config_overrides: Dict of overrides to call on the cluster config extracted from the ray connection extra field. 
            Ex. cluster_config.update(cluster_config_overrides) , defaults to {}
    :type cluster_config_overrides: Optional[dict], optional
    :param verbose: Bool to print standard out from the ray cli, defaults to True
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
        self.script = script
        self.script_args = script_args
        self.options = options
        self.ray_conn_id = ray_conn_id
        self.aws_conn_id = aws_conn_id
        self.cluster_config_overrides = cluster_config_overrides
        self.verbose = verbose

    def get_hook(self):
        return RayCliHook(
            command=Command.SUBMIT,
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
