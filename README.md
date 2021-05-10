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

```yaml
Release: 0.1.1.alpha0
```

## Requirements

Visit the [Ray Project page](https://ray.io/)
for more info on Ray.

> ⚠️ The server version and client version (build) of Ray MUST be
the same.

```yaml
- Python Version == 3.7
- Airflow Version >= 2.0.0
- Ray Version == 1.3.0
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

1. Add the provider package wheel file to the root directory of your Airflow project.

2. In your Airflow `Dockerfile`, you will need to add an environment variable to
specify your custom backend, along with the provider wheel install. Add the following:

    ```Dockerfile
    FROM quay.io/astronomer/ap-airflow:2.0.0-3-buster-onbuild
    USER root
    RUN pip install airflow_provider_ray-0.1.0a0-py3-none-any.whl
    RUN pip uninstall astronomer-airflow-version-check -y
    USER astro
    ENV AIRFLOW__CORE__XCOM_BACKEND=ray_provider.xcom.ray_backend.RayBackend
    ```

    > Check ap-airflow version, if unsure, change to `ap-airflow:latest-onbuild`

3. We are using a Ray `1.3.0` and python version `3.7`. To get a bleeding edge
version of Ray, you can to follow this format to build the wheel url in your
`requirements.txt` file:

    ```http
    https://s3-us-west-2.amazonaws.com/ray-wheels/master/{COMMIT_HASH}/ray-{RAY_VERSION}-{PYTHON_VERSION}-{PYTHON_VERSION}m-{OS_VERSION}_intel.whl
    ```

    For example, for linux based systems and linux containers @ commit
    `0f9d1bb223bb1ba5edbdd557f2f2f3551a51061f` it would be:

    ```http
    https://s3-us-west-2.amazonaws.com/ray-wheels/master/0f9d1bb223bb1ba5edbdd557f2f2f3551a51061f/ray-2.0.0.dev0-cp37-cp37m-manylinux2014_x86_64.whl
    ```

    For MacOS system, the wheel version naming goes as `macosx_10_13`, so that would
    be:

    ```http
    https://s3-us-west-2.amazonaws.com/ray-wheels/master/0f9d1bb223bb1ba5edbdd557f2f2f3551a51061f/ray-2.0.0.dev0-cp37-cp37m-macosx_10_13_intel.whl
    ```

4. Start your Airflow environment and open the UI.

5. In the Airflow UI, add an `Airflow Pool` with the following:

    ```bash
    Pool (name): ray_worker_pool
    Slots: 25
    ```

6. In the Airflow UI, add an `Airflow Connection` with the following:

    ```bash
    Conn Id: ray_cluster_connection
    Conn Type: HTTP
    Host: Cluster IP Address, with basic Auth params if needed
    Port: 10001
    ```

7. In your Airflow DAG python file, you must include the following in your
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

8. Using the taskflow API, your airflow task should now use the
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

## Project Contributors and Maintainers

This project is built in collaboration between
[Astronomer](https://www.astronomer.io/) and
[Anyscale](https://www.anyscale.com/),
with active contributions from:

- [Pete DeJoy](https://github.com/petedejoy)
- [Daniel Imberman](https://github.com/dimberman)
- [Rob Deeb](https://github.com/mrrobby)
- [Richard Liaw](https://github.com/richardliaw)

This project is formatted via `black`:

```
pip install black
black .
```

## Connections

TBD - [Info on building a connection to Ray]

