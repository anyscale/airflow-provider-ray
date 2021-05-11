import logging
import ray
from airflow.hooks.http_hook import HttpHook

log = logging.getLogger(__name__)


class RayClientHook(HttpHook):
    """A Connection Hook for accessing Ray via the Ray Client.

    Extending the HttpHook for now to demonstrate the pattern using
    an http connection.

    :param http_conn_id: The http connection id used to connect to Ray.
    :type http_conn_id: str
    """

    def __init__(self, ray_conn_id="ray_default"):

        self.ray_conn_id = ray_conn_id
        self.base_url = None
        self.num_cpus = None
        self.num_gpus = None
        self.resources = {}

    def get_conn(self) -> str:
        """Returns a connection string."""
        if self.ray_conn_id:
            conn = self.get_connection(self.ray_conn_id)

        if conn.host and "://" in conn.host and self.schema:
            # schema defaults to HTTP
            schema = conn.schema if conn.schema else "http"
            host = conn.host if conn.host else ""
            self.base_url = schema + "://" + host
        else:
            self.base_url = conn.host

        if conn.port:
            self.base_url = self.base_url + ":" + str(conn.port)

        return conn

    def connect(self):
        if self.base_url is None:
            conn = self.get_conn()

        log.info("Connection base_url is %s" % self.base_url)
        # currently there isn't much useful info
        # returned from ray.util.connect(),
        # but if there could be, here would be where to use it,
        # so we should work to understand that as well.
        if not ray.util.client.ray.is_connected():
            ray.util.connect(self.base_url)
            log.info("New Ray Connection Established")
        else:
            log.info("Reusing Existing Ray Connections")

    def disconnect(self):
        if self.base_url is None:
            conn = self.get_conn()
        ray.util.disconnect()

    # TODO: Create LocationTypes and persist data to S3 or GCS
    def cleanup(self, handles=None):
        """Kills any handles to any actors forcibly and disconnects Ray."""
        handles = handles or []
        for handle in handles:
            log.info("Cleaning ray actors")
            log.debug("Killing handle %s" % handle)
            ray.kill(handle)

        log.info("Cleaning connections")
        self.disconnect()
