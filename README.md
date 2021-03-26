<p align="center">
  <a href="https://www.airflow.apache.org">
    <img alt="Airflow" src="https://cwiki.apache.org/confluence/download/attachments/145723561/airflow_transparent.png?api=v2" width="60" />
    <img alt="Ray" src="https://avatars.githubusercontent.com/u/22125274?s=400&v=4" width="60" />
  </a>
</p>
<h1 align="center">
  Apache Airflow Provider for Ray
</h1>
  <h3 align="center">
  A provider you can install into your Airflow environment to access custom Ray XCom backends, Ray Hooks, and Ray Operators.
</h3>
<br/>

## 🧪 Experimental Version

This provider is an experimental alpha containing necessary components to
orchestrate and schedule Ray tasks using Airflow. It is actively maintained
and being developed to bring production-ready workflows to Ray using Airflow.
Thie release contains everything needed to begin building these workflows using
the Airlfow taskflow API.
```
Release: 0.1.0.alpha0
```

## Requirements

> ⚠️ The server version and client version (build) of Ray MUST be
the same

```yaml
- Python Version == 3.7
- Airflow Version >= 2.0.0
- Ray Version == 2.0.0.dev0 (cp37-cp37m-manylinux2014_x86_64 @ 2157021fd31f6192a14165c880e5fa4c01c7d5ca)
- Filelock >= 3.0.0
```

## Modules

- [Ray XCom Backend](./ray_provider/xcom/ray_backend.py): Custom XCom backend
Serving to move data between tasks using the Ray API with its internal Plasma
store, thereby allowing for in-memory distributed processing.
- [Ray Hook](./ray_provider/hooks/ray_client.py): Extension of `Http` hooks
that uses the Ray client to provide connections to the Ray Server.
- [Ray Decorator](./ray_provider/operators/ray_decorators.py): Task decorator
to be used with the task flow API, combining wrapping the existing airflow
`@task` decorate with `ray.remote` functionality, thereby executing each
task on the ray cluster.

## Configuration and Usage

1. In your Airflow `Dockerfile`, you will need to add an environment variable to
specify you custom backend

    ```Dockerfile
    ENV AIRFLOW__CORE__XCOM_BACKEND=ray_provider.xcom.ray_backend.RayBackend
    ```

2. In the Airflow UI, add an Airflow Pool with the Following

    ```bash
    Pool (name): ray_worker_pool
    Slots: 25
    ```

3. In the Airflow UI, add an Airflow Connection with the following:

    ```bash
    Conn Id: ray_cluster_connection
    Conn Type: HTTP
    Host: Cluster IP Address, with basic Auth params if needed
    Port: 10001
    ```

4. In your Airflow DAG file, you must include the following in your
`default_args` dictionary:

    ```python
    from ray_provider.xcom.ray_backend import RayBackend
    .
    .
    .
    default_args = {
        'on_success_callback': RayBackend.on_success_callback,
        'on_failure_callback': RayBackend.on_failure_callback,
        .
        .
        .
    }
    @dag(
        default_args=default_args,
        .
        .
    )
    def ray_example_dag():
        # do stuff
    ```

5. Using the taskflow API, your airflow task should now use the
`@ray_task` decorator for any ray task, like:

    ```python
    from ray_provider.operators.ray_decorators import ray_task
    .
    .
    def ray_example_dag():

        @ray_task(ray_conn_id='ray_cluster_connection')
        def sum_cols(df: pd.DataFrame) -> pd.DataFrame:
            return pd.DataFrame(df.sum()).T
    ```

## Connections

[Info on building a connection to Ray]
