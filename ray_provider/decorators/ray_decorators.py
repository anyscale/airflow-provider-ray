import datetime
import functools
import logging
import os
import pickle
from typing import Callable, Optional

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
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

        def on_execute_callback(context):
            pass

        @provide_session
        def on_retry_callback(context, session=None):

            # Connect to 'airflow' namespace to access scoped actors
            if not ray.util.client.ray.is_connected():
                ray.util.connect('192.168.1.70:10001', namespace='airflow')

            # Executes GCS download and unpickling from within Ray
            def _load_gcs_within_ray(object_name):

                # GCS conn points creds to path on Ray server
                os.environ['AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT'] = "google-cloud-platform://?extra__google_cloud_platform__key_path=%2FUsers%2Fp%2Fcode%2Fgcs%2Fastronomer-ray-demo-87cd7cd7e58f.json&extra__google_cloud_platform__scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fcloud-platform&extra__google_cloud_platform__project=airflow&extra__google_cloud_platform__num_retries=5"

                # Instantiate GCSHook pointing to creds on Ray server
                gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")

                # Read target value from GoogleCloudStorage
                obj_from_gcs = gcs_hook.download(
                    bucket_name='astro-ray',
                    object_name=object_name,
                    timeout=10000
                )

                # Deserialize GCS object
                return pickle.loads(obj_from_gcs)

            # Structure bucket object id
            object_name = f"{context['dag'].dag_id}_{context['ti'].task_id}.txt"

            # Execute function within Ray and return object reference key
            obj_ref_key = ray_wrapped(lambda: _load_gcs_within_ray(object_name=object_name),
                                      ray_conn_id,
                                      eager=eager)()

            # Retrieve the KV Actor
            actor_ray_kv_store = ray.get_actor("ray_kv_store")

            # This should be the GCS Object
            gcs_object_within_ray = ray.get(
                ray.get(actor_ray_kv_store.get.remote(obj_ref_key)))
            assert gcs_object_within_ray == 1

            # Write to xcom
            RayBackend.set(
                'return_value',
                obj_ref_key,
                execution_date=context['dag'].start_date,
                task_id=context['ti'].task_id,
                dag_id=context['dag'].dag_id
            )

            # To-do: Pass object to task as argument

        @provide_session
        def on_success_callback(context, session=None):
            # To-do: if checkpoint=True
            # To-do: encapsulate in a helper function

            # Connect to 'airflow' namespace to access scoped actors
            if not ray.util.client.ray.is_connected():
                ray.util.connect('192.168.1.70:10001', namespace='airflow')

            # Retrieve the KV Actor
            actor_ray_kv_store = ray.get_actor("ray_kv_store")

            # Retrieve object ref from xcom query
            # To-do: filter by execution date instead of using latest
            obj_ref_key = session.query(RayBackend).filter(
                RayBackend.key == 'return_value',
                RayBackend.task_id == context['ti'].task_id,
                RayBackend.dag_id == context['dag'].dag_id)\
                .order_by(RayBackend.timestamp.desc()).first().value

            # Retrieve `object reference` from `KV object` i.e. the Key of the kv pair
            obj_ref = actor_ray_kv_store.get.remote(obj_ref_key)

            # Retrieve `target object reference` i.e. the Value of the kv pair
            obj_ref_to_value = ray.get(obj_ref)

            # Retrieve target value from `target object reference`
            obj_value = ray.get(obj_ref_to_value)

            # Write target value to GoogleCloudStorage
            GCSHook(gcp_conn_id="google_cloud_default").upload(
                bucket_name='astro-ray',
                # To-do: create name based on dag run id
                object_name=f"{context['dag'].dag_id}_{context['ti'].task_id}.txt",
                data=pickle.dumps(obj_value),
                gzip=False,
                encoding='utf-8',
                timeout=10000
            )

        def on_failure_callback(context):
            pass

        return task(
            ray_wrapped(f, ray_conn_id, eager=eager),
            pool=ray_worker_pool,
            on_execute_callback=on_execute_callback,
            on_retry_callback=on_retry_callback,
            on_success_callback=on_success_callback,
            on_failure_callback=on_success_callback,
        )

    return wrapper
