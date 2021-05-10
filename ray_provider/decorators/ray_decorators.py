import logging
import functools
from typing import Callable, Optional
import ray
from airflow.operators.python import task
from ray_provider.hooks.ray_client import RayClientHook
from ray_provider.xcom.ray_backend import RayBackend, get_or_create_kv_store

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


def ray_task(
    python_callable: Optional[Callable] = None,
    ray_conn_id: str = "ray_default",
    ray_worker_pool: str = "ray_worker_pool",
    eager: bool = False,
):
    """Wraps a function to be executed on the Ray cluster.

    The return values of the function will be cached on the Ray object store.
    Downstream tasks must be ray tasks too, as the dependencies will be
    fetched from the object store.

    Args:
        python_callable (Callable): Function to be invoked on the Ray cluster.
        ray_conn_id (str): connection string provided by user.
        ray_worker_pool (str): The pool that controls the
            amount of parallel clients created to access the Ray cluster.
        eager (bool): Whether to run the the function on the
            coordinator process (on the Ray cluster) or to
            send the function to a remote task. You should
            set this to False normally.
    """

    @functools.wraps(python_callable)
    def wrapper(f):

        return task(
            ray_wrapped(f, ray_conn_id, eager=eager),
            pool=ray_worker_pool,
        )

    return wrapper
