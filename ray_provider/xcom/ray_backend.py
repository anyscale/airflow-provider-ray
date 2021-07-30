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
from ray_provider.tests import wrap_all_methods_with_counter, call_counter


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


@ray.remote
# To-do: decorate only during test
@wrap_all_methods_with_counter(call_counter)
class _KvStoreActor(object):

    from google.cloud import storage

    import os
    import pickle

    def __init__(self, gcs_creds_path, bucket_name):
        self.store = {}
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcs_creds_path
        self.bucket_name = bucket_name

    def show_store(self):
        return self.store

    def call_count(self):
        return self.n_called

    def ping(self):
        return 1

    def set_taskinstance_context(self, ti_context):
        self.ti_context = ti_context

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

    def gcs_blob(self, dag_id, task_id):
        # Structure GCS object name
        # To-do: introduce execution_date
        # To-do: `object_name` as external arg
        from google.auth.exceptions import DefaultCredentialsError
        object_name = '_'.join([dag_id, task_id]) + '.txt'
        try:
            # Create GCS blob
            return self.storage.Client().bucket(self.bucket_name).blob(object_name)
        except DefaultCredentialsError as e:
            raise DefaultCredentialsError(e)

    def gcs_dump(self, object_id, blob):

        # Retrieve target object
        obj = ray.get(self.get(object_id))

        # Write to GCS blob
        blob.upload_from_string(pickle.dumps(obj))
        print('Data uploaded to %s.', blob.name)

    def gcs_load(self, blob):
        # Retrieve object from GCS if it exists
        if blob.exists():
            return pickle.loads(blob.download_as_bytes())
        else:
            return None

    def obj_to_kv_store(self, obj):
        obj_ref = ray.put(obj)
        self.put(str(obj_ref), obj_ref)
        return str(obj_ref)

    def exists_in_gcs(self, blob):
        return blob.exists()

    def exists_in_ray(self, obj_ref):
        """Check if object ID exists in KV Actor store.

        Note: Object will not appear in KV store if Actor is reset/terminated.
        """
        try:
            return ray.get(self.get(obj_ref)) is not None
        except Exception as e:
            return None

    def recover_object(self, dag_id, task_id, obj_ref):
        """Recover object from Ray or GCS.

        Returns object ref of recovered object, or -1 if not found.
        """

        if self.exists_in_ray(obj_ref):
            return obj_ref
        else:
            blob = self.gcs_blob(dag_id, task_id)
            if self.exists_in_gcs(blob):
                obj = self.gcs_load(blob)
                return self.obj_to_kv_store(obj)
            else:
                return -1

    def recover_objects(self, upstream_objects):
        """Find objects in Ray and GCS; return their ids.
        """
        # To-do: Ray should be aware of task_id as fn of obj_ref
        # To-do: Execute in parallel

        return {task_id: self.recover_object(dag_id, task_id, obj_ref)
                for dag_id, task_id, obj_ref in upstream_objects}


class KVStore:
    def __init__(self, identifier, backend_cls=None, allow_new=False):
        backend_cls = backend_cls or RayBackend
        hook = backend_cls.get_hook()
        hook.connect()
        self.identifier = identifier
        self.actor = self.get_actor(identifier, allow_new)

    def get_actor(self, identifier, allow_new):
        """Creates the executor actor.

        :param identifier: Uniquely identifies the DAG.
        :type identifier: str
        :param allow_new: whether to create a new actor if the actor
            doesn't exist.
        :type allow_new: bool
        """
        try:
            log.debug(f"trying to get this actor {identifier} here")
            ret = ray.get_actor(identifier)
            self.is_new = False
            return ret
        except ValueError as e:
            log.info(e)
            if allow_new:
                log.info(
                    f"No problem.  Creating new Actor with identifier {identifier}")
                return self._create_new_actor(identifier)
            else:
                raise

    def _create_new_actor(self, identifier):
        self.is_new = True
        # Retrieve GCS credentials and bucket name from env variables
        # To-do: retrieve GCS credentials from Airflow conn
        gcs_creds_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
        bucket_name = os.environ["GCS_BUCKET_NAME"]
        return _KvStoreActor.options(name=identifier, lifetime="detached")\
            .remote(gcs_creds_path, bucket_name)

    def execute(self, fn, *, args, kwargs, eager=False):
        """Invokes the underlying actor with given args."""
        log.debug("fetching ray_kv lock.")

        with FileLock("/tmp/ray_kv.lock"):
            log.debug(f"Executing.")
            res = self.actor.execute.remote(
                fn=fn, args=args, kwargs=kwargs, eager=eager
            )
            return ray.get(res)


class RayBackend(BaseXCom):
    """
    Custom Backend Serving to use Ray.

    Setup in your airflow Dockerfile with the following lines: ::

        FROM quay.io/astronomer/ap-airflow:2.0.2-1-buster-onbuild
        USER root
        RUN pip uninstall astronomer-airflow-version-check -y
        USER astro
        ENV AIRFLOW__CORE__XCOM_BACKEND=ray_provider.xcom.ray_backend.RayBackend
    """

    conn_id = os.getenv("ray_cluster_conn_id", "ray_cluster_connection")
    store_identifier = os.getenv("ray_store_identifier", "ray_kv_store")

    @staticmethod
    def get_hook():
        return RayClientHook(ray_conn_id=RayBackend.conn_id)

    @staticmethod
    def generate_object_key(key, task_id, dag_id):
        return f"{key}_{task_id}_{dag_id}"

    @staticmethod
    def serialize_value(value, key, task_id, dag_id, execution_date):
        return pickle.dumps(value)

    @staticmethod
    def deserialize_value(result):
        return pickle.loads(result.value)

    @staticmethod
    def on_failure_callback(context):
        """
        Basically when the DAG state changes then
        we want to delete the ray resources. That is, wait
        for other tasks to complete first then
        get the kvstore and cleanup
        """
        log.error("Cleaning up from Failure: %s" % context)
        task = context.get("task")
        dag_run = context.get("dag_run")
        ti = context.get("task_instance")
        max_ctr = 5
        ctr = 1
        if task._downstream_task_ids:
            while (RayBackend.are_dependents_failed(task, dag_run) == False) and (
                ti.get_num_running_task_instances() > 0
            ):
                sleep_time = 2 ** ctr
                sleep(sleep_time)
                log.error("Waiting for downstream to fail")
                ctr = ctr + 1
                if ctr == max_ctr:
                    log.error("Max Sleep Time Reached. Consider Increasing")
                    break

        log.error("Time to clean up")
        with FileLock("/tmp/ray_backend.lock"):
            with FileLock("/tmp/ray_kv.lock"):
                log.debug("lock entered")
                handles = []
                try:

                    kv_store = get_or_create_kv_store(
                        identifier=RayBackend.store_identifier
                    )

                    actor = kv_store.get_actor(
                        kv_store.identifier, allow_new=False)
                    if actor is not None:
                        handles = [actor]

                except Exception as e:
                    log.error("Error getting store on cleanup %s" % e)

                RayBackend.get_hook().cleanup(handles=handles)

    @staticmethod
    def on_success_callback(context):
        log.info("DAG marked success, cleaning up")
        task = context.get("task")
        ti = context.get("task_instance")

        # If we're on the last task
        if ti.are_dependents_done():
            with FileLock("/tmp/ray_backend.lock"):
                with FileLock("/tmp/ray_kv.lock"):
                    handles = []
                    try:
                        kv_store = get_or_create_kv_store(
                            identifier=RayBackend.store_identifier
                        )

                        actor = kv_store.get_actor(
                            kv_store.identifier, allow_new=False)
                        if actor is not None:
                            handles = [actor]

                    except Exception as e:
                        log.error("Error getting store on cleanup %s" % e)

                    RayBackend.get_hook().cleanup(handles=handles)

    @classmethod
    @provide_session
    def set(cls, key, value, execution_date, task_id, dag_id, session=None):
        """
        Store an RayBackend value.
        :return: None
        """
        session.expunge_all()

        value = RayBackend.serialize_value(
            value, key, task_id, dag_id, execution_date)

        # remove any duplicate XComs
        session.query(cls).filter(
            cls.key == key,
            cls.execution_date == execution_date,
            cls.task_id == task_id,
            cls.dag_id == dag_id,
        ).delete()

        session.commit()

        # insert new XCom
        session.add(
            RayBackend(
                key=key,
                value=value,
                execution_date=execution_date,
                task_id=task_id,
                dag_id=dag_id,
            )
        )

        session.commit()

    # This doesn't Work but it should be made to somehow so
    #  we don't have to send these in as default_args in the dags
    @provide_session
    def set_dag_callbacks(session=None):
        from airflow.models.dag import DAG, DagModel, DagRun
        from airflow.models.taskinstance import _CURRENT_CONTEXT

        log.debug("Setting Callbacks _CURRENT_CONTEXT looks like %s" %
                  _CURRENT_CONTEXT)

        context = _CURRENT_CONTEXT[-1]
        dag = context.get("dag")
        dag_run = context.get("dag_run")
        exec_date = func.cast(dag_run.execution_date, DateTime)

        DR = DagRun

        dag_obj = session.query(DagModel).filter(
            DagModel.dag_id == dag._dag_id).one()

        dr = (
            session.query(DR)
            .filter(
                DR.dag_id == dag_run.dag_id,
                func.cast(DR.execution_date, DateTime) == exec_date,
                DR.run_id == dag_run.run_id,
            )
            .one()
        )

        dag_obj.on_success_callback = RayBackend.on_success_callback
        dag_obj.on_failure_callback = RayBackend.on_failure_callback
        dag_obj.has_on_success_callback = True
        dag_obj.has_on_failure_callback = True
        dr.dag = dag_obj
        session.commit()
        dr.refresh_from_db(session)
        log.info("RayDag Callbacks Configured")

        log.debug("dr %s" % dr.__dict__)
        log.debug("dag_obj %s" % dag_obj.__dict__)

    @provide_session
    def are_dependents_failed(task, dag_run, session=None):
        """
        Checks whether the immediate dependents of this task instance have failed.
        This is meant to be used to wait for cleanup.
        This is useful when you want to delete references but wait until
        other tasks finish.

        :param session: SQLAlchemy ORM Session
        :type session: Session
        """
        from airflow.models.taskinstance import TaskInstance

        log.info("Checking for failed dependents")

        if not task._downstream_task_ids:
            log.debug("not task._downstream_task_ids")
            return True

        exec_date = func.cast(dag_run.execution_date, DateTime)

        ti = session.query(func.count(TaskInstance.task_id)).filter(
            TaskInstance.dag_id == dag_run.dag_id,
            TaskInstance.task_id.in_(task._downstream_task_ids),
            func.cast(TaskInstance.execution_date, DateTime) == exec_date,
            TaskInstance.state.in_(State.failed_states),
        )
        count = ti[0][0]
        log.debug("count is %d" % count)
        return count == len(task._downstream_task_ids)
