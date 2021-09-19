from ray.autoscaler.sdk import (
    create_or_update_cluster,
    run_on_cluster,
    teardown_cluster,
    rsync,
)

from typing import Union, List, Optional
from airflow.hooks.S3_hook import S3Hook
import yaml
import json
from pathlib import Path


class RayHook:
    def __init__(
        self,
        cluster_config: Optional[Union[dict, str]] = None,
        script: Optional[str] = None,
        script_args: Optional[List[str]] = None,
        aws_conn_id: Optional[str] = None,
    ):
        self.cluster_config = cluster_config
        self.script = script
        self.script_args = script_args
        self.aws_conn_id = aws_conn_id

        self.ray_tmp_dir = Path("/tmp/ray")
        if self.cluster_config:
            self.cluster_config = self._get_cluster_config()
        if self.script:
            self.script = self._get_script()

    def _load_cluster_config(self, data: str, file_type: str = "yaml") -> dict:
        if file_type == "yaml":
            return yaml.safe_load(data)
        elif file_type == "json":
            return json.loads(data)

    def _write_script(self, data: str, file_name: str) -> str:
        path = self.ray_tmp_dir / file_name
        path.write_text(data=data)

        return str(path)

    def _get_cluster_config(self) -> dict:
        if isinstance(self.cluster_config, dict):
            return self.cluster_config
        elif isinstance(self.cluster_config, str):
            path = Path(self.cluster_config)
            if self.cluster_config.startswith("s3://"):
                s3hook = S3Hook(aws_conn_id=self.aws_conn_id)
                bucket, key = S3Hook.parse_s3_url(url=self.cluster_config)
                data = s3hook.read_key(key=key, bucket_name=bucket)
                return self._load_cluster_config(data=data, file_type=path.suffix)
            else:
                data = path.read_text()
                return self._load_cluster_config(data=data, file_type=path.suffix)

    def _get_script(self) -> str:
        if self.cluster_config.startswith("s3://"):
            path = Path(self.cluster_config)
            s3hook = S3Hook(aws_conn_id=self.aws_conn_id)
            bucket, key = S3Hook.parse_s3_url(url=self.cluster_config)
            data = s3hook.read_key(key=key, bucket_name=bucket)
            return self._write_script(
                data=data,
                file_name=path.name,
            )
        else:
            return self.script

    def start_cluster(self):
        create_or_update_cluster(cluster_config=self.cluster_config)

    def stop_cluster(self):
        teardown_cluster(cluster_config=self.cluster_config)

    def submit_job(self):
        source_script = self.script
        target_script = Path(self.script).name
        rsync(
            cluster_config=self.cluster_config,
            source=source_script,
            target=target_script,
        )

        cmd = ["python"]
        cmd.extend(target_script)
        cmd.extend(self.script_args)

        run_on_cluster(cluster_config=self.cluster_config, cmd=" ".join(cmd))
