"""
Test connection to specified Ray versions.

Set the Ray version to test and the expected host ip then run:

    AIRFLOW_CONN_RAY_CLUSTER_CONNECTION=http://@192.168.1.75:10001 \
    RAY_VERSION=1.5.1 \
    python3 -m unittest tests.hooks.test_ray_versions.TestRayConnection

"""

import logging
import os
import pytest
import unittest
from unittest import mock

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import DAG, DagRun, TaskInstance as TI
from airflow.utils.state import State
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow import settings


log = logging.getLogger(__name__)

DEFAULT_DATE = timezone.utcnow()
RAY_VERSION = os.environ.get('RAY_VERSION', '1.4.1')
CURRENT_DIR_PACKAGE = os.getcwd()


class TestRayConnection(unittest.TestCase):
    """Test connection to Ray cluster.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        with create_session() as session:
            log.info("Creating session at: %s" % session.bind)
            session.query(DagRun).delete()
            session.query(TI).delete()

    def setUp(self) -> None:
        # Airflob db session
        self.session = settings.Session()

    def test_venv(self):

        @dag(default_args={}, schedule_interval=None, start_date=days_ago(1))
        def dag_tests_venv():
            """DAG to execute virtual environment task.
            """
            @task.virtualenv(
                use_dill=True,
                system_site_packages=False,
                requirements=[
                    f'ray[default]=={RAY_VERSION}', 'aioredis<2', CURRENT_DIR_PACKAGE],
            )
            def task_1():
                """Python function that runs in virtualenv.
                """
                import ray
                import subprocess
                import sys
                import time

                from ray_provider.hooks.ray_client import RayClientHook

                print(f'Testing connecting Ray version: {ray.__version__}')

                # Set Ray executable for this virtual environment
                VENV_RAY = f"{sys.exec_prefix}/bin/ray"

                # Start Ray as a subprocess
                subprocess.run(f"{VENV_RAY} stop --force".split(' '))
                time.sleep(5)
                subprocess.run(
                    f"{VENV_RAY} start --num-cpus=8 --object-store-memory=7000000000 --head".split(' '))
                time.sleep(5)

                # Create Ray Airflow Hook
                hook = RayClientHook(ray_conn_id="ray_cluster_connection")

                # Connect Ray Hook
                hook.connect()

                # Assert successful connection
                assert ray.util.client.ray.is_connected() == True

                # Disconnect Ray Hook
                hook.disconnect()

                # Stop Ray subprocess
                subprocess.run(f"{VENV_RAY} stop --force".split(' '))

            task_1_output = task_1()

        the_dag = dag_tests_venv()
        _tis = [TI(task=task, execution_date=DEFAULT_DATE,
                   state=State.RUNNING) for task in the_dag.tasks]

        # Create dagrun
        from airflow.utils.types import DagRunType
        tis = list(map(self.session.merge, _tis))
        dr = the_dag.create_dagrun(  # Creates `task_instance` record
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        _tis[0]._run_raw_task()
