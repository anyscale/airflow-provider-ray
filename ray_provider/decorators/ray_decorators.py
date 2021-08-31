import copy
import logging
import functools
from typing import Callable, Optional
from typing import Dict, Optional, Callable, List, Any
import os

from airflow.decorators.base import task_decorator_factory
from airflow.decorators.python import _PythonDecoratedOperator
from airflow.models.xcom_arg import XComArg
from airflow.models.renderedtifields import RenderedTaskInstanceFields
from airflow.operators.python import PythonOperator
from airflow.utils.db import provide_session
from airflow.utils.session import provide_session
from airflow.exceptions import AirflowException
from airflow.models import DagRun
from airflow.utils.types import DagRunType
import ray

from ray_provider.xcom.ray_backend import get_or_create_kv_store, KVStore
from airflow.models.xcom import BaseXCom

log = logging.getLogger(__name__)


RAY_STORE_IDENTIFIER = 'ray_kv_store'


def ray_wrapped(f, ray_conn_id="ray_default", eager=False):

    @functools.wraps(f)
    def wrapper(*args, **kwargs) -> "ray.ObjectRef":
        log.info("[wrapper] Got executor.")

        executor = get_or_create_kv_store(
            identifier=RAY_STORE_IDENTIFIER, allow_new=True
        )

        log.info(f"[wrapper] Launching task with {args}, {kwargs}.")
        ret_str = executor.execute(f, args=args, kwargs=kwargs, eager=eager)
        log.info("[wrapper] Remote task finished.")
        return ret_str

    return wrapper


def ray_task(
    python_callable: Optional[Callable] = None,
    ray_conn_id: str = "ray_default",
    ray_worker_pool: str = "ray_worker_pool",
    eager: bool = False,
    checkpoint: bool = False
):
    """Wraps a function to be executed on the Ray cluster.
    """

    @functools.wraps(python_callable)
    def wrapper(f):

        return task(
            ray_wrapped(f, ray_conn_id, eager=eager),
            pool=ray_worker_pool,
            checkpoint=checkpoint
        )

    return wrapper


class RayPythonOperator(PythonOperator):
    """Subclass `PythonOperator` to customize execution and pre-execution 
    behavior.

    a) `__init__` holds XComArgs to enable assignment of recovered objects
    b) `execute` forces Task failure if upstream objects fail to recover
    c) `pre_execute` recovers upstream objects for a given Task in retry state
    """

    def __init__(self, *,
                 python_callable: Callable,
                 op_args: Optional[List] = None,
                 op_kwargs: Optional[Dict] = None,
                 templates_dict: Optional[Dict] = None,
                 templates_exts: Optional[List[str]] = None,
                 **kwargs) -> None:

        # Store task XComArgs
        if all(isinstance(arg, XComArg) for arg in self.op_args):
            self.ray_xcomarg_op_args = self.op_args
            self.ray_xcomarg_op_kwargs = self.op_kwargs

        # Flag if this task should be checkpointed on success.
        # Pop item to prevent passing it to `PythonOperator` superclass
        self.checkpoint = kwargs.pop('checkpoint')

        # If specified, enable checkpointing on success for this Task
        if self.checkpoint:
            kwargs['on_success_callback'] = self.checkpoint_on_success_callback

        # Enable checkpointing of upstream if this Task retries
        kwargs['on_retry_callback'] = self.checkpoint_on_retry_callback

        # Indicate whether upstream task arguments were retrieved
        self.upstream_not_retrieved = False

        super().__init__(python_callable=python_callable,
                         op_args=op_args,
                         op_kwargs=op_kwargs,
                         templates_dict=templates_dict,
                         templates_exts=templates_exts,
                         **kwargs
                         )

    def execute(self, context: Dict):

        # Fail task if object retrieval fails
        if self.upstream_not_retrieved:
            raise AirflowException('Failed to retrieve upstream object.')

        return super(RayPythonOperator, self).execute(context)

    @provide_session
    def pre_execute(self, context, session=None):
        ti = context.get('ti')
        task = ti.task

        # If task is running for the first time, don't recover upstream objects
        if ti._try_number <= 1 or ti.state != 'up_for_retry':
            return

        # Retrieve cloud storage flag from environment variable
        cloud_storage = os.getenv('CHECKPOINTING_CLOUD_STORAGE', None)

        # If adequate cloud storage not specified, don't recover upstream objects
        if cloud_storage not in ['GCS', 'AWS']:
            return

        log.info(f"Checkpointing output of upstream Tasks to {cloud_storage}.")

        # Retrieve the KV Actor
        actor_ray_kv_store = KVStore("ray_kv_store").get_actor("ray_kv_store")

        # List upstream task ids
        upstream_tasks = self._upstream_tasks(task.task_id, task.dag)

        # Retrieve upstream object ids from xcom
        upstream_objects = [self._retrieve_obj_id_from_xcom(
            task_id,
            task.dag.dag_id,
            context.get('dag_run').execution_date) for task_id in upstream_tasks]

        # Retrieve object refs from Ray kv store
        run_id = DagRun.generate_run_id(
            DagRunType.MANUAL, context.get('dag_run').execution_date)
        recovered_obj_refs = ray.get(
            actor_ray_kv_store.recover_objects.remote(
                upstream_objects,
                run_id,
                cloud_storage=cloud_storage))

        # Set recovered objects as current Task's XComArgs
        for task_id, obj_ref in recovered_obj_refs.items():

            # Flag if object retrieval failed
            if obj_ref == -404:
                self.upstream_not_retrieved = True

            if 'ObjectRef' in str(obj_ref):
                BaseXCom.set(
                    key='return_value',
                    value=str(obj_ref),
                    execution_date=ti.execution_date,
                    task_id=task_id,
                    dag_id=task.dag.dag_id,
                    session=session
                )

        # Reassign XComArg objects
        self.op_args = self.ray_xcomarg_op_args
        self.op_kwargs = self.ray_xcomarg_op_kwargs

        # Render XComArg object with newly assigned values
        self.render_template_fields(context)

        # Write to `rendered_task_instance_fields` table
        RenderedTaskInstanceFields.write(
            RenderedTaskInstanceFields(ti=ti, render_templates=False))
        RenderedTaskInstanceFields.delete_old_records(ti.task_id, ti.dag_id)

    @staticmethod
    @provide_session
    def _retrieve_obj_id_from_xcom(task_id, dag_id, execution_date, session=None):

        obj_ref_key = session.query(BaseXCom).filter(
            BaseXCom.key == 'return_value',
            BaseXCom.task_id == task_id,
            BaseXCom.dag_id == dag_id,
            BaseXCom.execution_date == execution_date) \
            .order_by(BaseXCom.timestamp.desc()).first()

        return (dag_id, task_id, obj_ref_key.value if bool(obj_ref_key) else None)

    @staticmethod
    def _upstream_tasks(task_id, dag, path=[]):
        """List upstream tasks recursively.
        """

        def _recurse_upstream_tasks(task_id, dag):
            r = [task_id]
            for child in dag.get_task(task_id)._upstream_task_ids:
                r.extend(_recurse_upstream_tasks(child, dag))
            return r

        upstream_tasks = set(_recurse_upstream_tasks(task_id, dag))
        upstream_tasks.remove(task_id)
        return upstream_tasks

    def checkpoint_on_success_callback(self, context):
        # Retrieve cloud storage flag from environment variable
        cloud_storage = os.getenv('CHECKPOINTING_CLOUD_STORAGE', None)
        log.info(f"Checkpointing Task output to {cloud_storage}.")

        # If adequate cloud storage not specified, don't recover upstream objects
        if cloud_storage not in ['GCS', 'AWS']:
            return

        # Retrieve object id from xcom
        dag_id, task_id, obj_ref = RayPythonOperator._retrieve_obj_id_from_xcom(
            context.get('ti').task_id,
            context.get('dag').dag_id,
            context.get('dag_run').execution_date)

        # Retrieve the KV Actor
        actor_ray_kv_store = KVStore("ray_kv_store").get_actor("ray_kv_store")

        # Checkpoint output objects to cloud storage
        run_id = DagRun.generate_run_id(
            DagRunType.MANUAL, context.get('dag_run').execution_date)

        actor_ray_kv_store.checkpoint_object.remote(
            dag_id, task_id, run_id, obj_ref, cloud_storage)

    def checkpoint_on_retry_callback(self, context):
        """When a task is set to retry, store the output of its upstream tasks.
        """

        # Retrieve cloud storage flag from environment variable
        cloud_storage = os.getenv('CHECKPOINTING_CLOUD_STORAGE', None)

        # If adequate cloud storage not specified, don't recover upstream objects
        if cloud_storage not in ['GCS', 'AWS']:
            return

        # List upstream task ids
        upstream_tasks = RayPythonOperator._upstream_tasks(
            context.get('ti').task_id, context.get('dag'))

        # Retrieve upstream object ids from xcom

        upstream_objects = [RayPythonOperator._retrieve_obj_id_from_xcom(
            task_id,
            context.get('dag').dag_id,
            context.get('dag_run').execution_date) for task_id in upstream_tasks]

        # Retrieve the KV Actor
        actor_ray_kv_store = KVStore("ray_kv_store").get_actor("ray_kv_store")

        # Checkpoint all upstream objects to cloud storage
        run_id = DagRun.generate_run_id(
            DagRunType.MANUAL, context.get('dag_run').execution_date)
        for dag_id, task_id, obj_ref in upstream_objects:
            actor_ray_kv_store.checkpoint_object.remote(
                dag_id,
                task_id,
                run_id,
                obj_ref,
                cloud_storage)


class _RayDecoratedOperator(_PythonDecoratedOperator, RayPythonOperator):
    """Supplant execution and pre-execution methods of _PythonDecoratedOperator
    with those defined by `RayPythonOperator`.
    """
    pass


class _RayTaskDecorator:
    def __call__(
        self, python_callable: Optional[Callable] = None, multiple_outputs: Optional[bool] = None, **kwargs
    ):
        """
        Python operator decorator. Wraps a function into an Airflow operator.
        Accepts kwargs for operator kwarg. This decorator can be reused in a single DAG.
        :param python_callable: Function to decorate
        :type python_callable: Optional[Callable]
        :param multiple_outputs: if set, function return value will be
            unrolled to multiple XCom values. List/Tuples will unroll to xcom values
            with index as key. Dict will unroll to xcom values with keys as XCom keys.
            Defaults to False.
        :type multiple_outputs: bool
        """

        return task_decorator_factory(
            python_callable=python_callable,
            multiple_outputs=multiple_outputs,
            decorated_operator_class=_RayDecoratedOperator,
            ** kwargs,
        )


task = _RayTaskDecorator()
