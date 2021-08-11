"""
Unittest module for Airflow Ray Provider decorator

1. Reference an active Ray endpoint in the `AIRFLOW_CONN_RAY_CLUSTER_CONNECTION` environment variable.
2. Reference a Google Cloud Credentials JSON in scope of the Ray cluster using the `GOOGLE_APPLICATION_CREDENTIALS` environment variable.
3. Reference the name of an existing GCS bucket accessible to the GCS credentials above.
4. Run each test (individually to prevent Agent race contidions) with:


    AWS_ACCESS_KEY_ID=... \
    AWS_SECRET_ACCESS_KEY=... \
    S3_BUCKET_NAME=astro-ray \
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/google/creds.json \
    GCS_BUCKET_NAME=astro-ray \
    AIRFLOW_CONN_RAY_CLUSTER_CONNECTION=http://@192.168.1.69:10001 python3 -m unittest tests.decorators.test_decorators.TestDagrun.test_on_execute_callback_3rd_try

"""

from datetime import datetime
import logging
import os
import pytest
import unittest
from unittest import mock

from airflow.utils import timezone
from airflow.models import DAG, DagRun, TaskInstance as TI

from airflow.utils.types import DagRunType
from airflow.utils.state import State
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow import settings

import ray

from ray_provider.xcom.ray_backend import RayBackend, get_or_create_kv_store, KVStore
from ray_provider.hooks.ray_client import RayClientHook
from ray_provider.decorators.ray_decorators import RayPythonOperator

DEFAULT_DATE = timezone.utcnow()
log = logging.getLogger(__name__)


@mock.patch.dict('os.environ',  AIRFLOW__CORE__XCOM_BACKEND="ray_provider.xcom.ray_backend.RayBackend")
class TestDagrun(unittest.TestCase):
    """
    Test Ray Decorator

    Tests fault tolerance logic under different DAG execution scenarios.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        with create_session() as session:
            log.info("Creating session at: %s" % session.bind)
            session.query(DagRun).delete()
            session.query(TI).delete()

    def setUp(self) -> None:

        # Load DAG from dags/
        from ray_provider.example_dags.demo_fault_tolerance import demo
        self.dag = demo()

        # Create TIs
        self._tis = [TI(task=task, execution_date=DEFAULT_DATE, state=State.RUNNING)
                     for task in self.dag.tasks]

        # Airflob db session
        self.session = settings.Session()

        # Connect to Ray
        ray.util.disconnect()
        RayClientHook(ray_conn_id='ray_cluster_connection').connect()

        # Reset the KV Actor for each test
        self.actor_name = "ray_kv_store"  # self._testMethodName
        try:
            actor_ray_kv_store = ray.get_actor(self.actor_name)
            # Kill existing actor
            ray.kill(actor_ray_kv_store)
        except:
            pass

        # To-do: wrap `_KvStoreActor` only during testing
        _KVStore = KVStore(self.actor_name, allow_new=True)

        self.actor_ray_kv_store = ray.get_actor(self.actor_name)

    @classmethod
    def tearDownClass(cls):
        # Kill the KV Actor after each test
        try:
            actor_ray_kv_store = ray.get_actor("ray_kv_store")
            # Kill existent actor
            ray.kill(actor_ray_kv_store)
        except:
            pass

    def tearDown(self):
        super().tearDown()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    @mock.patch.object(RayPythonOperator, '_upstream_tasks')
    def test_on_execute_callback_3rd_try(self, _upstream_tasks_mock):
        """Scenario where a task retries and Fault Tolerance runs in 
        `on_retry_callback`.
        """
        context = self._tis[1].get_template_context()
        context['ti']._try_number = 3
        self._tis[1].task.on_retry_callback(context)

        _upstream_tasks_mock.assert_called()

    def test_task_execution_without_retry(self):
        """Scenario where a task executes successfully.
        """

        # Set try number
        self._tis[1]._try_number = 0

        # Write TIs to db
        tis = list(map(self.session.merge, self._tis))

        # Create dagrun
        dr = self.dag.create_dagrun(  # Creates `task_instance` record
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        self.session.commit()

        # Execute task (Writes to xcom)
        self._tis[0]._run_raw_task()
        self._tis[1]._run_raw_task()

        out0 = self._tis[0].xcom_pull(key='return_value')
        out1 = self._tis[1].xcom_pull(key='return_value')

        assert self._tis[0].state == State.SUCCESS
        assert self._tis[1].state == State.SUCCESS

    def test_task_execution_with_retry_and_object_in_ray(self):
        """Scenario where a task retries and the target object is found in Ray.
        """

        # Set try number
        self._tis[1]._try_number = 3

        # Write TIs to db
        tis = list(map(self.session.merge, self._tis))

        # Create dagrun
        dr = self.dag.create_dagrun(  # Creates `task_instance` record
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        self.session.commit()

        # Execute task (Writes to xcom)
        self._tis[0]._run_raw_task()
        self._tis[1]._run_raw_task()

        # Retrieve values from xcom
        out0 = self._tis[0].xcom_pull(
            key='return_value', task_ids=self._tis[0].task_id)
        out1 = self._tis[1].xcom_pull(
            key='return_value', task_ids=self._tis[1].task_id)

        # Assert task states
        assert self._tis[0].state == State.SUCCESS
        assert self._tis[1].state == State.SUCCESS

        # Retrieve `_KvStoreActor` store
        store = ray.get(self.actor_ray_kv_store.show_store.remote())

        # Assert task output values exist in `_KvStoreActor` store
        assert out0 in store
        assert out1 in store

    def _task_execution_with_retry_and_object_not_in_ray(self):
        """Scenario where the target object is not found in Ray and is therefore 
        sought and found in GCS.
        """

        # Write TIs to db
        tis = list(map(self.session.merge, self._tis))

        # Create dagrun
        dr = self.dag.create_dagrun(  # Creates `task_instance` record
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        self.session.commit()

        # Execute task (Writes to xcom)
        self._tis[0]._run_raw_task()
        self._tis[1]._run_raw_task()
        self._tis[2]._run_raw_task()

        # Retrieve values from xcom
        out0 = self._tis[0].xcom_pull(
            key='return_value', task_ids=self._tis[0].task_id)
        out1 = self._tis[1].xcom_pull(
            key='return_value', task_ids=self._tis[1].task_id)
        out2 = self._tis[2].xcom_pull(
            key='return_value', task_ids=self._tis[2].task_id)

        # `on_retry_callback` writes to GCS prior to dropping object from Ray store
        # Set try number
        self._tis[2]._try_number = 3
        self._tis[2].set_state('up_for_retry')
        # Execute task
        self._tis[2].task.on_retry_callback(
            context=self._tis[2].get_template_context())

        # Drop objects from Ray store
        self.actor_ray_kv_store.drop.remote(out0)
        self.actor_ray_kv_store.drop.remote(out1)
        self.actor_ray_kv_store.drop.remote(out2)

        # Retrieve `_KvStoreActor` store v2
        store2 = ray.get(self.actor_ray_kv_store.show_store.remote())

        # Assert task output values don't exist in `_KvStoreActor` store
        assert out0 not in store2
        assert out1 not in store2

        # Execute task
        self._tis[2]._run_raw_task()

        # Retrieve `_KvStoreActor` store v2
        store3 = ray.get(self.actor_ray_kv_store.show_store.remote())

        assert len(store3) == 3

    @mock.patch.dict('os.environ',  CHECKPOINTING_CLOUD_STORAGE="GCS")
    def test_retry_with_object_in_gcs(self):
        self._task_execution_with_retry_and_object_not_in_ray()

    @mock.patch.dict('os.environ',  CHECKPOINTING_CLOUD_STORAGE="AWS")
    def test_retry_with_object_in_aws(self):
        self._task_execution_with_retry_and_object_not_in_ray()

    def test_gcs_conn(self):
        """Test that Ray Actor connects to GCS.
        """
        self.actor_ray_kv_store
        self.actor_ray_kv_store.gcs_blob.remote('demo', 'load_data')

    @mock.patch.dict('os.environ',  CHECKPOINTING_CLOUD_STORAGE="GCS")
    def test_checkpoint_flag(self):

        # breakpoint()
        # Write TIs to db
        tis = list(map(self.session.merge, self._tis))

        # Create dagrun
        dr = self.dag.create_dagrun(  # Creates `task_instance` record
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        self.session.commit()

        # Execute task (Writes to xcom)
        self._tis[0]._run_raw_task()
        self._tis[1]._run_raw_task()
        self._tis[2]._run_raw_task()

        # Execute task
        self._tis[1].task.on_success_callback(
            context=self._tis[1].get_template_context())
