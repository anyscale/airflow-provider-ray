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
from airflow.exceptions import AirflowNotFoundException
import ray

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


class RayHelperFunctions:

    def __init__(self, bucket_name='astro-ray', google_creds_path='/Users/p/code/gcs/astronomer-ray-demo-87cd7cd7e58f.json'):

        self.bucket_name = bucket_name
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = google_creds_path

    # Executes GCS download and unpickling from within Ray
    @staticmethod
    def _load_gcs_within_ray(object_name, bucket_name='astro-ray'):

        from google.cloud import storage
        import os
        import pickle

        print('in _load_gcs_within_ray')
        try:
            # Read target value from GoogleCloudStorage
            bucket = storage.Client().bucket(bucket_name)
            # To-do: use more efficient download method
            obj_from_gcs = bucket.blob(
                blob_name=object_name).download_as_string()

            print('obj_from_gcs: ', obj_from_gcs)
            print('obj_from_gcs loads: ', pickle.loads(obj_from_gcs))

            # Deserialize GCS object
            return pickle.loads(obj_from_gcs)

        except Exception as e:
            print("File not found in GCS.")
            print(e)

    # Execute GCS write from within Ray

    @staticmethod
    @ray.remote
    def _write_object_to_gcs(object_id, object_name, bucket_name='astro-ray'):
        from google.cloud import storage
        import os
        import pickle

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/p/code/gcs/astronomer-ray-demo-87cd7cd7e58f.json'

        # Retrieve the KV Actor
        actor_ray_kv_store = ray.get_actor("ray_kv_store")

        # Object to write into GCS
        gcs_object_within_ray = ray.get(
            ray.get(actor_ray_kv_store.get.remote(object_id)))

        # Write to GCS
        # To-do: what's the quicker upload?
        bucket = storage.Client().bucket(bucket_name)
        res = bucket.blob(object_name).upload_from_string(
            pickle.dumps(gcs_object_within_ray))

        print('Data uploaded to %s in %s bucket',
              object_name, bucket_name)

        return res

    # Identify upstream tasks
    @staticmethod
    def _list_all_upstream_tasks(task_id, context, path=[]):
        path = path + [task_id]
        upstream_tasks = context['dag'].get_task(
            task_id)._upstream_task_ids

        if len(upstream_tasks) == 0:
            return [path]

        paths = []
        for upstream_task in upstream_tasks:
            paths = paths + RayHelperFunctions._list_all_upstream_tasks(
                upstream_task,
                context,
                path=path)

        return paths

    # Get Task's object id
    @staticmethod
    @provide_session
    def _retrieve_obj_id_from_xcom(task_id, dag_id, session=None):
        # To-do: incorporate execution date in filter

        obj_ref_key = session.query(RayBackend).filter(
            RayBackend.key == 'return_value',
            RayBackend.task_id == task_id,
            RayBackend.dag_id == dag_id) \
            .order_by(RayBackend.timestamp.desc()).first()

        return (task_id, obj_ref_key.value if bool(obj_ref_key) else None)


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

        @provide_session
        def on_execute_callback(context, session=None):

            log.info('in on_execute_callback')

            # Execute function within Ray and return object reference key
            def _load_gcs_within_ray(object_name, bucket_name='astro-ray'):
                from google.cloud import storage
                import os
                import pickle
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/p/code/gcs/astronomer-ray-demo-87cd7cd7e58f.json'
                print('in _load_gcs_within_ray')
                try:
                    # Read target value from GoogleCloudStorage
                    bucket = storage.Client().bucket(bucket_name)
                    # To-do: use more efficient download method
                    obj_from_gcs = bucket.blob(
                        blob_name=object_name).download_as_bytes()
                    print('obj_from_gcs: ', obj_from_gcs)
                    print('obj_from_gcs loads: ',
                          pickle.loads(obj_from_gcs))
                    # Deserialize GCS object
                    return pickle.loads(obj_from_gcs)
                except Exception as e:
                    # To-do: Ray indicate an error instead of just assigning an object ref
                    print("File not found in GCS.")
                    print(e)

            # Skip if it's the first try
            if context['ti']._try_number <= 1:
                return 0

            # Otherwise, load data
            upstream_tasks = RayHelperFunctions._list_all_upstream_tasks(
                context['ti'].task_id, context)[0][1:]

            # Connect to 'airflow' namespace to access scoped actors
            RayClientHook(ray_conn_id='ray_cluster_connection').connect()

            # Retrieve relevant object ids from xcom
            list_of_upstream_objects = [RayHelperFunctions._retrieve_obj_id_from_xcom(
                task_id, context['dag'].dag_id, session) for task_id in upstream_tasks]

            # Retrieve the KV Actor
            actor_ray_kv_store = ray.get_actor("ray_kv_store")

            # To-do: run in parallel
            for task_id, obj_ref_key in list_of_upstream_objects:

                # 1. Check Ray
                object_ref_within_ray = ray.get(
                    actor_ray_kv_store.get.remote(obj_ref_key))

                # If object not in Ray
                # To-do: this check should be conducted within Ray
                if not object_ref_within_ray:
                    # Structure GCS bucket object id
                    object_name = f"{context['dag'].dag_id}_{task_id}.txt"

                    # 2. Check GCS
                    # To-do: decide whether to timestamp GCS objects

                    # To-do: first Airflow should check GCS object exists
                    new_gcs_obj_ref_key = ray_wrapped(lambda: _load_gcs_within_ray(
                        object_name=object_name), ray_conn_id, eager=eager)()

                    log.info('new_gcs_obj_ref_key: %s' % new_gcs_obj_ref_key)

                    # If object fetched from GCS, add to xcom and set as arg
                    if new_gcs_obj_ref_key:
                        context['ti'].task.op_args = (new_gcs_obj_ref_key,)
                        RayBackend.set(key='return_value',
                                       value=new_gcs_obj_ref_key,
                                       task_id=task_id,
                                       dag_id=context['dag'].dag_id,
                                       execution_date=context['ti'].execution_date,
                                       session=session)

                        # To-do: handle assigning multiple ordered op_args
                        context['ti'].task.op_args = (new_gcs_obj_ref_key,)
                    else:
                        # 3. Fail DAG
                        log.info('Data not in Ray nor GCS. Re-run DAG.')
                        raise AirflowNotFoundException(object_name)

        @provide_session
        def on_failure_callback(context, session=None):

            log.info('in on_failure_callback')

            upstream_tasks = RayHelperFunctions._list_all_upstream_tasks(
                context['ti'].task_id, context)[0][1:]

            # Connect to 'airflow' namespace to access scoped actors
            RayClientHook(ray_conn_id='ray_cluster_connection').connect()

            # Retrieve relevant object ids from xcom
            list_of_objects = [RayHelperFunctions._retrieve_obj_id_from_xcom(
                task_id, context['dag'].dag_id, session) for task_id in upstream_tasks]

            # Filter out objects without object ref
            list_of_objects = [each for each in list_of_objects if each[1]]

            # Stateless write to GCS within Ray
            _ = ray.get(
                [RayHelperFunctions._write_object_to_gcs.remote(object_id=object_id, object_name=f"{context['dag'].dag_id}_{task_id}.txt")
                 for task_id, object_id in list_of_objects])
            log.info('out on_failure_callback')

        @provide_session
        def on_retry_callback(context, session=None):
            log.info('in on_retry_callback')
            on_failure_callback(context, session=None)

        def on_success_callback(context):
            log.info('in on_success_callback')

        return task(
            ray_wrapped(f, ray_conn_id, eager=eager),
            pool=ray_worker_pool,
            on_execute_callback=on_execute_callback,
            on_retry_callback=on_retry_callback,
            on_success_callback=on_success_callback,
            on_failure_callback=on_failure_callback,
        )

    return wrapper
