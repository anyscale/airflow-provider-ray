"""Test Ray decorator under distinct retry scenarios.

Run tests individually:

    python3 -m unittest ray_provider.tests.decorators.test_decorators.TestDagrun.test_task_execution_with_retry_and_object_in_ray

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

from ray_provider.decorators import ray_task
import ray

from ray_provider.xcom.ray_backend import RayBackend, get_or_create_kv_store, KVStore
from ray_provider.hooks.ray_client import RayClientHook
from ray_provider.tests import wrap_all_methods_with_counter, call_counter

DEFAULT_DATE = timezone.utcnow()
log = logging.getLogger(__name__)


@mock.patch.dict('os.environ',  AIRFLOW__CORE__XCOM_BACKEND="ray_provider.xcom.ray_backend.RayBackend")
@mock.patch.dict('os.environ',  AIRFLOW_CONN_RAY_CLUSTER_CONNECTION="http://@192.168.1.69:10001")
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

    @mock.patch.dict('os.environ',  AIRFLOW_CONN_RAY_CLUSTER_CONNECTION="http://@192.168.1.69:10001")
    @mock.patch.dict('os.environ',  GOOGLE_APPLICATION_CREDENTIALS="/Users/p/code/gcs/astronomer-ray-demo-87cd7cd7e58f.json")
    @mock.patch.dict('os.environ',  GCS_BUCKET_NAME="astro-ray")
    def setUp(self) -> None:

        # Load DAG from dags/
        from ray_provider.example_dags.demo import demo
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

    def test_counter(self):
        # Verify the counter works. Counts number of method calls.
        self.actor_ray_kv_store.ping.remote()
        self.actor_ray_kv_store.ping.remote()
        counter_dict = ray.get(self.actor_ray_kv_store.call_count.remote())
        assert counter_dict == {'__init__': 1, 'ping': 2, 'call_count': 1}

    @mock.patch.object(RayClientHook, 'connect')
    @mock.patch.object(ray, 'get_actor')
    def test_on_execute_callback_3rd_try(self, mock_connect, mock_get_actor):
        # If not first try, run Fault Tolerance

        context = self._tis[1].get_template_context()
        context['ti']._try_number = 3
        self.ti2.task.on_execute_callback(context)

        mock_connect.assert_called()
        mock_get_actor.assert_called()

    @mock.patch.object(RayClientHook, 'connect')
    @mock.patch.object(ray, 'get_actor')
    def test_on_execute_callback_1st_try(self, mock_connect, mock_get_actor):
        # If first try, continue without Fault Tolerance

        context = self._tis[1].get_template_context()
        context['ti']._try_number = 1
        self._tis[1].task.on_execute_callback(context)

        mock_connect.assert_not_called()
        mock_get_actor.assert_not_called()

    def test_fault_tolerance(self):
        context = self._tis[1].get_template_context()
        context['ti']._try_number = 3
        self._tis[1].task.on_execute_callback(context, self.session)

    def test_task_execution_without_retry(self):

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

        counter_dict = ray.get(self.actor_ray_kv_store.call_count.remote())

        assert counter_dict == {'__init__': 1,
                                'execute': 2,
                                'put': 2,
                                'get': 1,
                                'call_count': 1
                                }

    def test_task_execution_with_retry(self):

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

        # Retrieve count of `_KvStoreActor` method executions
        counter_dict = ray.get(self.actor_ray_kv_store.call_count.remote())

        # Assert `_KvStoreActor` method call count
        assert counter_dict == {'__init__': 1,
                                'execute': 2,
                                'put': 2,
                                'recover_objects': 1,
                                'recover_object': 1,
                                'exists_in_ray': 1,
                                'get': 3,
                                'call_count': 1
                                }

    def test_task_execution_with_retry_and_object_in_ray(self):

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

        # Retrieve count of `_KvStoreActor` method executions
        counter_dict = ray.get(self.actor_ray_kv_store.call_count.remote())

        # Retrieve `_KvStoreActor` store
        store = ray.get(self.actor_ray_kv_store.show_store.remote())

        # Assert task output values exist in `_KvStoreActor` store
        assert out0 in store
        assert out1 in store

        # Assert `_KvStoreActor` method call count
        assert counter_dict == {'__init__': 1,
                                'execute': 2,
                                'put': 2,
                                'recover_objects': 1,
                                'recover_object': 1,
                                'exists_in_ray': 1,
                                'get': 2,
                                'call_count': 1
                                }

    def test_task_execution_with_retry_and_object_not_in_ray(self):

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
        self._tis[1]._run_raw_task()

        # Retrieve values from xcom
        out0 = self._tis[0].xcom_pull(
            key='return_value', task_ids=self._tis[0].task_id)
        out1 = self._tis[1].xcom_pull(
            key='return_value', task_ids=self._tis[1].task_id)

        # Drop objects from ray store
        self.actor_ray_kv_store.drop.remote(out0)
        self.actor_ray_kv_store.drop.remote(out1)

        # Retrieve `_KvStoreActor` store v2
        store2 = ray.get(self.actor_ray_kv_store.show_store.remote())

        # Assert task output values don't exist in `_KvStoreActor` store
        assert out0 not in store2
        assert out1 not in store2

        breakpoint()

        # Execute task
        self._tis[1]._run_raw_task()

        # Retrieve `counter_dict` v2
        counter_dict2 = ray.get(self.actor_ray_kv_store.call_count.remote())

        # Assert {'exists_in_gcs': 1} appears on counter dict
        assert 'exists_in_gcs' in counter_dict2
        assert counter_dict2['exists_in_gcs'] == 1

    # def test_task_execution_with_retry_and_object_in_gcs
    # def test_task_execution_with_retry_and_object_dne

    def test_gcs_conn(self):
        breakpoint()
        self.actor_ray_kv_store
        ray.get(self.actor_ray_kv_store.gcs_blob.remote('demo', 'load_data'))
