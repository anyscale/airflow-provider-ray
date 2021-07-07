import logging
import functools
from typing import Callable, Optional
import ray
from airflow.decorators.python import python_task
from airflow.operators.python import PythonOperator
from airflow.decorators.base import DecoratedOperator, task_decorator_factory
from ray_provider.hooks.ray_client import RayClientHook
from ray_provider.xcom.ray_backend import RayBackend, get_or_create_kv_store
from typing import Callable, Optional, TypeVar

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


class _RayDecoratedOperator(DecoratedOperator, PythonOperator):
    """
    Wraps a Python callable and captures args/kwargs when called for execution.

    :param python_callable: A reference to an object that is callable
    :type python_callable: python callable
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function (templated)
    :type op_kwargs: dict
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable (templated)
    :type op_args: list
    :param multiple_outputs: if set, function return value will be
        unrolled to multiple XCom values. Dict will unroll to xcom values with keys as keys.
        Defaults to False.
    :type multiple_outputs: bool
    """

    template_fields = ('op_args', 'op_kwargs')
    template_fields_renderers = {"op_args": "py", "op_kwargs": "py"}

    # since we won't mutate the arguments, we should just do the shallow copy
    # there are some cases we can't deepcopy the objects (e.g protobuf).
    shallow_copy_attrs = ('python_callable',)

    def __init__(
            self,
            **kwargs,
    ) -> None:
        kwargs_to_upstream = {
            "python_callable": kwargs["python_callable"],
            "op_args": kwargs["op_args"],
            "op_kwargs": kwargs["op_kwargs"],
        }

        super().__init__(kwargs_to_upstream=kwargs_to_upstream, **kwargs)


T = TypeVar("T", bound=Callable)  # pylint: disable=invalid-name


def _ray_task(
        python_callable: Optional[Callable] = None, multiple_outputs: Optional[bool] = None, **kwargs
):
    """
    Python operator decorator. Wraps a function into an Airflow operator.
    Accepts kwargs for operator kwarg. Can be reused in a single DAG.

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
        **kwargs,
    )


def ray_task(
        python_callable: Optional[Callable] = None,
        ray_conn_id: str = "ray_default",
        ray_worker_pool: str = "ray_worker_pool",
        eager: bool = False,
):
    """Wraps a function to be executed on the Ray cluster.

    The return values of the function will be cached on the Ray object store.
    Downstream tasks must be ray tasks too, as the dependencies will be
    fetched from the object store. The RayBackend will need to be setup in your
    Dockerfile to use this decorator.

    Use as a task decorator: ::

        from ray_provider.decorators import ray_task

        def ray_example_dag():

            @ray_task("ray_conn_id")
            def sum_cols(df: pd.DataFrame) -> pd.DataFrame:
                return pd.DataFrame(df.sum()).T

    :param python_callable: Function to be invoked on the Ray cluster.
    :type python_callable: Optional[Callable]
    :param http_conn_id: Http connection id for conenction to ray.
    :type http_conn_id: str
    :param ray_worker_pool: The pool that controls the
            amount of parallel clients created to access the Ray cluster.
    :type ray_worker_pool: Optional[str]
    :param eager: Whether to run the the function on the
            coordinator process (on the Ray cluster) or to
            send the function to a remote task. You should
            set this to False normally.
    :type eager: Optional[bool]
    """

    @functools.wraps(python_callable)
    def wrapper(f):
        def pre_execute():
            pass

        ray_task = _ray_task(
            ray_wrapped(f, ray_conn_id, eager=eager),
            pool=ray_worker_pool,
        )
        ray_task.pre_execute = pre_execute

        return ray_task

    return wrapper
