import inspect
import logging
import functools
from typing import Callable, Optional

from airflow.operators.python import task
from airflow.utils.db import provide_session
from ray_provider.hooks.ray_client import RayClientHook
from ray_provider.xcom.ray_backend import RayBackend, get_or_create_kv_store

import ray


log = logging.getLogger(__name__)


def ray_wrapped(f, ray_conn_id="ray_default", eager=False):

    @functools.wraps(f)
    def wrapper(*args, **kwargs) -> "ray.ObjectRef":
        log.info("[wrapper] Got executor.")

        executor = get_or_create_kv_store(
            identifier=RayBackend.store_identifier, allow_new=True
        )

        log.info(f"[wrapper] Launching task (with {args}, {kwargs}.")
        ret_str = executor.execute(f, args=args, kwargs=kwargs, eager=eager)
        log.info("[wrapper] Remote task finished")
        return ret_str

    return wrapper


@provide_session
def _retrieve_obj_id_from_xcom(task_id, dag_id, session=None):
    # To-do: incorporate execution date in filter
    # To-do: consolidate helper functions

    obj_ref_key = session.query(RayBackend).filter(
        RayBackend.key == 'return_value',
        RayBackend.task_id == task_id,
        RayBackend.dag_id == dag_id) \
        .order_by(RayBackend.timestamp.desc()).first()

    return (dag_id, task_id, obj_ref_key.value if bool(obj_ref_key) else None)


def _upstream_tasks(task_id, context, path=[]):
    # To-do: consolidate helper functions
    path = path + [task_id]
    upstream_tasks = context['dag'].get_task(
        task_id)._upstream_task_ids

    if len(upstream_tasks) == 0:
        return [path]

    paths = []
    for upstream_task in upstream_tasks:
        paths = paths + _upstream_tasks(
            upstream_task,
            context,
            path=path)

    return paths


def ray_task(
    python_callable: Optional[Callable] = None,
    ray_conn_id: str = "ray_default",
    ray_worker_pool: str = "ray_worker_pool",
    eager: bool = False,
):
    """Wraps a function to be executed on the Ray cluster.
    """

    @provide_session
    def on_execute_callback(context, session=None):

        # If this run is not a retry, skip
        if context.get('ti')._try_number < 2:
            return

        # Connect to 'airflow' namespace to access scoped actors
        RayClientHook(ray_conn_id='ray_cluster_connection').connect()

        # Retrieve the KV Actor
        actor_ray_kv_store = ray.get_actor("ray_kv_store")

        # List upstream task ids
        immediately_upstream_tasks = context['ti'].task._upstream_task_ids
        upstream_tasks = _upstream_tasks(
            context.get('ti').task_id, context)[0][1:]

        # Retrieve relevant object ids from xcom
        upstream_objects = [_retrieve_obj_id_from_xcom(task_id, context.get(
            'dag').dag_id, session) for task_id in upstream_tasks]

        # upstream_objects_obj_ref = ray.put(upstream_objects)
        recovered_obj_refs = ray.get(
            actor_ray_kv_store.recover_objects.remote(upstream_objects))

        # To-do: if one is empty, fail task
        # Set recovered objects as current Task's XComArgs
        for task_id, obj_ref in recovered_obj_refs.items():
            RayBackend.set(
                key='return_value',
                value=str(obj_ref),
                execution_date=context['ti'].execution_date,
                task_id=task_id,
                dag_id=context['ti'].dag_id,
                session=session
            )

        # To-do: set new attributes in TaskInstance

    @provide_session
    def on_retry_callback(context, session=None):
        pass

    @provide_session
    def on_success_callback(context, session=None):
        pass

    @provide_session
    def on_failure_callback(context, session=None):
        pass

    @functools.wraps(python_callable)
    def wrapper(f):

        return task(
            ray_wrapped(f, ray_conn_id, eager=eager),
            pool=ray_worker_pool,
            on_execute_callback=on_execute_callback,
            on_retry_callback=on_retry_callback,
            on_success_callback=on_success_callback,
            on_failure_callback=on_failure_callback,
        )

    return wrapper
