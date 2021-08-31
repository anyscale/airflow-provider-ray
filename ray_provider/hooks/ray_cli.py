import yaml
import subprocess
from pathlib import Path

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
from airflow.hooks.S3_hook import S3Hook


from typing import List


class RayCliHook(BaseHook):
    """Simple wrapper around the Ray CLI."""

    def __init__(
        self,
        command: str,
        options: List[str] = None,
        cluster_config: str = None,
        script: str = None,
        script_args: List[str] = None,
        aws_conn_id: str = "aws_default",
    ):
        self.command = command
        self.options = options
        self.cluster_config = cluster_config
        self.script = script
        self.script_args = script_args
        self.aws_conn_id = aws_conn_id

        # Make working directory
        self.ray_dir = Path.cwd() / "ray"
        self.ray_dir.mkdir()
        if self.cluster_config:
            self._get_cluster_config()
        if script:
            self._get_script()

        self.supported_cli_cmds = ["up", "down", "submit"]


    def _get_cluster_config(self) -> None:
        if self.cluster_config.startswith("s3://"):
            self.log.info("Using remote cluster config: %s", self.cluster_config)
            s3hook = S3Hook(aws_conn_id=self.aws_conn_id)
            bucket, key = S3Hook.parse_s3_url(self.cluster_config)
            data = s3hook.read_key(key, bucket_name=bucket)
            cluster_yaml = yaml.safe_load(data)
            
            path = Path(self.cluster_config)
            self.cluster_config_path = self.ray_dir / path.name
            with self.cluster_config_path.open("w") as f:
                yaml.dump(cluster_yaml, f)
        else:
            self.log.info("Using local cluster config: %s", self.cluster_config)

    def _get_script(self):
        if self.script.startswith("s3://"):
            self.log.info("Using remote script: %s", self.script)
            s3hook = S3Hook(aws_conn_id=self.aws_conn_id)
            bucket, key = S3Hook.parse_s3_url(self.script)
            data = s3hook.read_key(key, bucket_name=bucket)
            
            path = Path(self.script)
            self.script_path = self.ray_dir / path.name
            with self.script_path.open("w") as f:
                f.write(data)
        else:
            self.log.info("Using local script: %s", self.script)

    def _prepare_cli_cmd(self):
        """Prepare cli command following the template below:
        
        ray COMMAND [OPTIONS] [CLUSTER_CONFIG] [SCRIPT] [SCRIPT_ARGS]"""

        cli_cmd = ["ray", self.command]
        cli_cmd.extend(self.options)
        cli_cmd.append(str(self.cluster_config_path))
        if self.script:
            cli_cmd.append(str(self.script_path))
            cli_cmd.extend(self.script_args)

        return cli_cmd

    def run_cli(self, verbose=True):
        cmd = self._prepare_cli_cmd()

        if verbose:
            self.log.info("%s", " ".join(cmd))

        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,)

        stdout = ""
        while True:
            line = p.stdout.readline()
            if not line:
                break
            stdout += line.decode("UTF-8")
            if verbose:
                self.log.info(line.decode("UTF-8").strip())
        p.wait()

        if p.returncode:
            raise AirflowException(stdout)

        return stdout

    def run(self):
        if self.command in self.supported_cli_cmds:
            return self.run_cli()
        else:
            raise AirflowException(f"Unsupported command: {self.command}")
