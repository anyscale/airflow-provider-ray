from datetime import datetime

from airflow.decorators import dag
from airflow.utils.trigger_rule import TriggerRule

from ray_provider.operators.anyscale_cluster import (
    AnyscaleCreateClusterOperator,
    AnyscaleStartClusterOperator,
    AnyscaleTerminateClusterOperator,
)
from ray_provider.operators.anyscale_cluster import AnyscaleCreateSessionCommandOperator


AUTH_TOKEN = "<auth_token>"
DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": 0,
}


@dag(
    schedule_interval=None,
    start_date=datetime(2022, 9, 30),
    tags=["demo"],
    default_args=DEFAULT_ARGS,
)
def anyscale_cluster():

    cluster = AnyscaleCreateClusterOperator(
        task_id="create_cluster",
        name="<name>",
        project_id="<project_id>",
        compute_config_id="<compute_config_id>",
        cluster_environment_build_id="<cluster_environment_build_id>",
        auth_token=AUTH_TOKEN,
    )

    start = AnyscaleStartClusterOperator(
        task_id="start_cluster",
        cluster_id=cluster.output["id"],
        auth_token=AUTH_TOKEN,
        wait_for_completion=True,
    )

    job = AnyscaleCreateSessionCommandOperator(
        task_id="submit_job",
        session_id=cluster.output["id"],
        shell_command="python3 -c 'import ray'",
        auth_token=AUTH_TOKEN,
        wait_for_completion=True,
    )

    terminate = AnyscaleTerminateClusterOperator(
        task_id="terminate_cluster",
        auth_token=AUTH_TOKEN,
        cluster_id=cluster.output["id"],
        wait_for_completion=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    cluster >> start >> job >> terminate


dag = anyscale_cluster()
