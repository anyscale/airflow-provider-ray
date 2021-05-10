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
Release: 0.2.0-rc.1
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
to assist operators in moving data between tasks using the Ray API with its
internal Plasma store, thereby allowing for in-memory distributed processing
and handling of large data objects.
- [Ray Hook](./ray_provider/hooks/ray_client.py): Extension of `Http` hook
that uses the Ray client to provide connections to the Ray Server.
- [Ray Decorator](./ray_provider/decorators/ray_decorators.py): Task decorator
to be used with the task flow API, combining wrapping the existing airflow
`@task` decorate with `ray.remote` functionality, thereby executing each
task on the ray cluster.

## Configuration and Usage

1. Add the provider package wheel file to the root directory of your Airflow project.

2. In your Airflow `Dockerfile`, you will need to add an environment variable to
specify your custom backend, along with the provider wheel install. Add the following:

    ```Dockerfile
    FROM quay.io/astronomer/ap-airflow:2.0.2-1-buster-onbuild
    USER root
    RUN pip uninstall astronomer-airflow-version-check -y
    USER astro
    ENV AIRFLOW__CORE__XCOM_BACKEND=ray_provider.xcom.ray_backend.RayBackend
    ```

    > Check ap-airflow version, if unsure, change to `ap-airflow:latest-onbuild`

3. We are using a Ray `1.3.0` and python version `3.7`. To get a bleeding edge
version of Ray, you can to follow this format to build the wheel url in your
`requirements.txt` file:

    ```bash
    pip install airflow-provider-ray==0.2.0-rc.1
    ```

4. Configure Ray Locally. To run ray locally, you'll need a minimum 6GB of free
memory.To start, in your environment with ray installed, run:

    ```bash
    (venv)$ ray start --num-cpus=8 --object-store-memory=7000000000 --head
    ```

    If you have extra resources, you can bump the memory up.

    You should now be able to open the ray dashboard at [http://127.0.0.1:8265/](http://127.0.0.1:8265/).

6. Start your Airflow environment and open the UI.

7. In the Airflow UI, add an `Airflow Pool` with the following:

    ```bash
    Pool (name): ray_worker_pool
    Slots: 25
    ```

8. In the Airflow UI, add an `Airflow Connection` with the following:

    ```bash
    Conn Id: ray_cluster_connection
    Conn Type: HTTP
    Host: Cluster IP Address, with basic Auth params if needed
    Port: 10001
    ```

9. In your Airflow DAG python file, you must include the following in your
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

10. Using the taskflow API, your airflow task should now use the
`@ray_task` decorator for any ray task and add the `ray_conn_id`,
parameter as `task_args`, like:

    ```python
    from ray_provider.decorators.ray_decorators import ray_task

    default_args = {
        'on_success_callback': RayBackend.on_success_callback,
        'on_failure_callback': RayBackend.on_failure_callback,
        .
        .
        .
    }
    task_args = {"ray_conn_id": "ray_cluster_connection"}
    .
    .
    .
    @dag(
        default_args=default_args,
        .
        .
    )
    def ray_example_dag():

        @ray_task(**task_args)
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
- [Charles Greer](https://github.com/grechaw)
- [Will Drevo](https://github.com/worldveil)

This project is formatted via `black`:

```bash
pip install black
black .
```

## Connections

TBD - [Info on building a connection to Ray]

