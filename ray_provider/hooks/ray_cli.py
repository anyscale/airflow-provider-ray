import yaml
import subprocess
from pathlib import Path

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
from airflow.hooks.S3_hook import S3Hook


from typing import List, Optional


class Command:
    UP = "up"
    DOWN = "down"
    SUBMIT = "submit"


class RayCliHook(BaseHook):
    """Simple wrapper around the Ray CLI.

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
    ):
        supported_cli_cmds = [Command.UP, Command.DOWN, Command.SUBMIT]
        if command not in supported_cli_cmds:
            raise AirflowException(f"Unsupported command: {command}")

        self.command = command
        self.script = script
        self.script_args = script_args
        self.options = options
        self.ray_conn_id = ray_conn_id
        self.aws_conn_id = aws_conn_id
        self.cluster_config_overrides = cluster_config_overrides
        self.verbose = verbose

        self.ray_dir = Path("/tmp/ray")
        if not self.ray_dir.exists():
            self.ray_dir.mkdir()
        self.cluster_config_path = self._get_cluster_config()
        self.script_path = self._get_script()

    def _get_cluster_config(self) -> str:
        ray_conn = self.get_connection(self.ray_conn_id)
        cluster_config = ray_conn.extra_dejson.copy()
        cluster_config.update(self.cluster_config_overrides)

        cluster_config_path = self.ray_dir / "cluster-config.yaml"
        with cluster_config_path.open("w") as f:
            yaml.dump(cluster_config, f)

        return str(cluster_config_path)

    def _get_script(self):
        if self.script.startswith("s3://"):
            self.log.info("Using remote script: %s", self.script)
            s3hook = S3Hook(aws_conn_id=self.aws_conn_id)
            bucket, key = S3Hook.parse_s3_url(self.script)
            data = s3hook.read_key(key, bucket_name=bucket)
            path = Path(self.script)

            script_path = self.ray_dir / path.name
            with script_path.open("w") as f:
                f.write(data)

            return str(script_path)
        else:
            self.log.info("Using local script: %s", self.script)
            return self.script

    def _prepare_cli_cmd(self):
        """Prepare cli command following the template below:

        ray [OPTIONS] COMMAND CLUSTER_CONFIG_FILE SCRIPT [SCRIPT_ARGS]"""

        cli_cmd = ["ray"]
        cli_cmd.extend(self.options)
        cli_cmd.append(self.command)
        cli_cmd.append(self.cluster_config_path)
        cli_cmd.append(self.script_path)
        cli_cmd.extend(self.script_args)

        return cli_cmd

    def run_cli(self):
        cmd = self._prepare_cli_cmd()

        if self.verbose:
            self.log.info("%s", " ".join(cmd))

        p = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        stdout = ""
        while True:
            line = p.stdout.readline()
            if not line:
                break
            stdout += line.decode("UTF-8")
            if self.verbose:
                self.log.info(line.decode("UTF-8").strip())
        p.wait()

        if p.returncode:
            raise AirflowException(stdout)

        return stdout
