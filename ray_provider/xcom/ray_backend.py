from ray.util.client.common import ClientObjectRef
from typing import List
import logging
import os
import pickle
from filelock import FileLock
from time import sleep
from airflow.models.xcom import BaseXCom
from airflow.utils.session import provide_session
from airflow.utils.state import State
from sqlalchemy import func, DateTime
import ray
from ray import ObjectRef as RayObjectRef
from ray_provider.hooks.ray_client import RayClientHook


log = logging.getLogger(__name__)

_KVStore = None


ObjectRef = (ClientObjectRef, RayObjectRef)


def get_or_create_kv_store(identifier, allow_new=False):
    global _KVStore
    if _KVStore:
        return _KVStore
    else:
        _KVStore = KVStore(identifier, allow_new=allow_new)
    return _KVStore


class KVStore:

    @ray.remote
    class _KVStoreActor(object):
        """Ray Actor that stores task-output objects and checkpoints in GCS or
        AWS.

        Note: GCS and AWS pick up credentials from env variables, so there 
        is no need to specify them as variables.
        """

        import os
        import pickle

        def __init__(self, env_var_payload):
            self.store = {}

            # Set passed environment variables
            for k, v in env_var_payload.items():
                os.environ[k] = v or ''

        def show_store(self):
            return self.store

        def ping(self):
            return 1

        def get(self, key: str, signal=None) -> "ObjectRef":
            return self.store.get(key)

        def put(self, key, value: "ObjectRef", signal=None):
            assert isinstance(value, ObjectRef), value
            self.store[key] = value

        def drop(self, key):
            assert key in self.store
            self.store.pop(key)

        def execute(self, *, fn, args, kwargs, eager=False) -> str:
            """Execute a function.

            TODO: allow resource access.

            :param fn: function to be executed, either on this actor or
                in a separate remote task.
            :type fn: func
            :parm args: function args.
            :type args: any
            :param kwargs: function kwargs.
            :type kwargs: obj
            :param eager: This value gets past the Ray ownership model
                    by allowing users to selectively execute tasks
                    directly on the metadata store actor.
                    this means that created objects like Modin Dataframes
                    will not be GC'ed upon task completion.
            :type eager: bool
            """
            ray_args = [self.get(a) for a in args]
            ray_kwargs = {k: self.get(v) for k, v in kwargs.items()}

            def status_function(*args_, **kwargs_):
                return 0, fn(*args_, **kwargs_)

            if eager:
                if ray_args:
                    ray_args = ray.get(ray_args)
                if ray_kwargs:
                    kwargs_list = list(ray_kwargs.items())
                    keys, values = zip(*kwargs_list)
                    if values:
                        values = ray.get(values)
                    ray_kwargs = dict(zip(keys, values))
                result = fn(*ray_args, **ray_kwargs)
                result = ray.put(result)
            else:
                remote_fn = ray.remote(status_function)
                status, result = remote_fn.options(num_returns=2).remote(
                    *ray_args, **ray_kwargs
                )
                ray.get(status)  # raise error if needed

            self.put(str(result), result)
            # self.put(_task_instance_string(), result)
            return str(result)

        def obj_to_kv_store(self, obj):
            obj_ref = ray.put(obj)
            self.put(str(obj_ref), obj_ref)
            return str(obj_ref)

        def exists_in_ray(self, obj_ref):
            """Check if object ID exists in KV Actor store.

            Note: Object will not appear in KV store if Actor is reset/terminated.
            """
            try:
                return ray.get(self.get(obj_ref)) is not None
            except Exception as e:
                return None

        def _external_object_name(self, dag_id, task_id, run_id):
            """Structure name of external object."""
            return '_'.join([dag_id, task_id, run_id]) + '.txt'

        def gcs_blob(self, object_name):
            # Structure GCS object name
            from google.cloud import storage
            from google.auth.exceptions import DefaultCredentialsError

            try:
                # Create GCS blob
                return storage.Client().bucket(os.environ['GCS_BUCKET_NAME']).blob(object_name)
            except DefaultCredentialsError as e:
                raise DefaultCredentialsError(e)

        def checkpoint_object(self, dag_id, task_id, run_id, object_id, cloud_storage='GCS'):
            """Write pickled object to external data store.

            `cloud_storage` attribute specifies whether to write to GCS or AWS.
            """
            def dump_object_to_aws():
                import boto3
                # Retrieve target object
                obj = ray.get(self.get(object_id))

                # Write to AWS S3 Bucket
                boto3.client('s3').put_object(
                    Body=pickle.dumps(obj),
                    Bucket=os.environ['S3_BUCKET_NAME'],
                    Key=object_name)
                log.info('Data uploaded to %s.', object_name)

            def dump_object_to_gcs():

                # Retrieve target object
                obj = ray.get(self.get(object_id))

                # Write to GCS blob
                blob = self.gcs_blob(object_name)
                blob.upload_from_string(pickle.dumps(obj))
                log.info('Data uploaded to %s.', blob.name)

            object_name = self._external_object_name(dag_id, task_id, run_id)
            if cloud_storage == 'GCS':
                dump_object_to_gcs()
            elif cloud_storage == 'AWS':
                dump_object_to_aws()
            else:
                raise Exception(
                    "Please set the `CHECKPOINTING_CLOUD_STORAGE` environment variable to specify GCS or AWS as the cloud storage provider.")

        def recover_objects(self, upstream_objects, run_id, cloud_storage='GCS'):
            """Find objects first in Ray then in GCS/AWS; return object ref ids.
            """
            def recover_object_from_gcs(dag_id, task_id, obj_ref):
                """Recover object from Ray or GCS.

                Returns object ref of recovered object, or -404 if not found.
                """

                if self.exists_in_ray(obj_ref):
                    return obj_ref
                else:
                    blob = self.gcs_blob(
                        self._external_object_name(dag_id, task_id, run_id))
                    if blob.exists():
                        # Retrieve object from GCS if it exists
                        obj = pickle.loads(blob.download_as_bytes())
                        return self.obj_to_kv_store(obj)
                    else:
                        return -404

            def recover_object_from_aws(dag_id, task_id, obj_ref):
                """Recover object from Ray or AWS.

                Returns object ref of recovered object, or -404 if not found.
                """
                import boto3

                object_name = self._external_object_name(
                    dag_id, task_id, run_id)

                if self.exists_in_ray(obj_ref):
                    return obj_ref
                else:
                    # If object exists in S3 bucket
                    if 'Contents' in boto3.client('s3').list_objects(Bucket=os.environ['S3_BUCKET_NAME'], Prefix=object_name):
                        # Retrieve object from AWS S3 if it exists
                        pickled_obj = boto3.client('s3').get_object(
                            Bucket=os.environ['S3_BUCKET_NAME'],
                            Key=object_name)['Body'].read()
                        obj = pickle.loads(pickled_obj)
                        return self.obj_to_kv_store(obj)

                    else:
                        return -404

            if cloud_storage == 'GCS':
                return {task_id: recover_object_from_gcs(dag_id, task_id, obj_ref)
                        for dag_id, task_id, obj_ref in upstream_objects}
            elif cloud_storage == 'AWS':
                return {task_id: recover_object_from_aws(dag_id, task_id, obj_ref)
                        for dag_id, task_id, obj_ref in upstream_objects}
            else:
                raise Exception(
                    "Please set the `CHECKPOINTING_CLOUD_STORAGE` environment variable to specify GCS or AWS as the cloud storage provider.")

    def __init__(self, identifier, backend_cls=None, allow_new=False):

        self.ray_conn_id = os.getenv(
            "ray_cluster_conn_id", "ray_cluster_connection")
        hook = RayClientHook(ray_conn_id=self.ray_conn_id)
        hook.connect()
        self.identifier = identifier
        self.actor = self.get_actor(identifier, allow_new)

    def get_actor(self, identifier, allow_new=False):
        """Creates the executor actor.

        :param identifier: Uniquely identifies the DAG.
        :type identifier: str
        :param allow_new: whether to create a new actor if the actor
            doesn't exist.
        :type allow_new: bool
        """
        try:
            log.debug(f"Retrieve {identifier} actor.")
            ret = ray.get_actor(identifier)
            self.is_new = False
            return ret
        except ValueError as e:
            log.info(e)
            if allow_new:
                log.info(
                    f"Actor not found, create new Actor with identifier {identifier}.")
                return self._create_new_actor(identifier)
            else:
                raise

    def _create_new_actor(self, identifier):
        self.is_new = True

        # Pass env variables to Ray
        env_var_payload = {var_name: os.getenv(var_name, None) for var_name in [
            'GOOGLE_APPLICATION_CREDENTIALS',
            'GCS_BUCKET_NAME',
            'CHECKPOINTING_CLOUD_STORAGE',
            'AWS_ACCESS_KEY_ID',
            'AWS_SECRET_ACCESS_KEY',
            'S3_BUCKET_NAME'
        ]}

        return self._KVStoreActor.options(name=identifier, lifetime="detached")\
            .remote(env_var_payload)

    def execute(self, fn, *, args, kwargs, eager=False):
        """Invokes the underlying actor with given args."""
        log.debug("fetching ray_kv lock.")

        with FileLock("/tmp/ray_kv.lock"):
            log.debug(f"Executing.")
            res = self.actor.execute.remote(
                fn=fn, args=args, kwargs=kwargs, eager=eager
            )
            return ray.get(res)
