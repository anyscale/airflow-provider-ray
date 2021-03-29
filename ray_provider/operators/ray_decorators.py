import logging
import functools
from typing import Callable, Optional
import ray
from airflow.operators.python import task
from ray_provider.hooks.ray_client import RayClientHook

log = logging.getLogger(__name__)


def ray_wrapped(f, ray_conn_id='ray_default'):

    @functools.wraps(f)
    def wrapper(*args, **kwargs):

        hook = RayClientHook(ray_conn_id=ray_conn_id)
        log.debug("[wrapper] connecting in before calling the remote task")
        hook.connect()
        log.debug("[wrapper] Executing the remote task")
        ret = ray.remote(f).remote(*args, **kwargs)

        return ret
    return wrapper


def ray_task(
    python_callable: Optional[Callable] = None,
    ray_conn_id='ray_default',
    multiple_outputs: bool = False,
    ray_worker_pool='ray_worker_pool',
    **kwargs
):
    @functools.wraps(python_callable)
    def wrapper(f):

        return task(
            ray_wrapped(f, ray_conn_id, **kwargs),
            multiple_outputs=multiple_outputs,
            pool=ray_worker_pool

        )

    return wrapper
