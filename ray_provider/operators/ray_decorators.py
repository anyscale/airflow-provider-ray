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
    ray_conn_id="ray_default",
    multiple_outputs: bool = False,
    ray_worker_pool="ray_worker_pool",
    **kwargs,
):
    @functools.wraps(python_callable)
    def wrapper(f):

        return task(
            ray_wrapped(f, ray_conn_id, **kwargs),
            multiple_outputs=multiple_outputs,
            pool=ray_worker_pool,
        )

    return wrapper
