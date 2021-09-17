from airflow.decorators import dag, task
from ray_provider.decorators.ray_decorators import ray_task
from ray_provider.xcom.ray_backend import RayBackend


from datetime import datetime


default_args = {
    "owner": "airflow",
    "on_success_callback": RayBackend.on_success_callback,
    "on_failure_callback": RayBackend.on_failure_callback,
    "retries": 1,
    "retry_delay": 0,
}

task_args = {
    "ray_conn_id": "ray_cluster_connection",
}


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2020, 1, 1, 0, 0, 0),
    tags=['demo']
)
def demo():

    @ray_task(**task_args)
    def load_data1():
        return 1

    @ray_task(**task_args, checkpoint=True)
    def load_data2():
        return 2

    @ray_task(**task_args)
    def transform_data(data1, data2):
        return data1 * data2 * 100

    # Upstream outputs save to GCS when this task retries
    @ray_task(**task_args)
    def divide_by_zero(data):
        return data/0

    the_data1 = load_data1()
    the_data2 = load_data2()
    the_transformed_data = transform_data(the_data1, the_data2)
    divide_by_zero_output = divide_by_zero(the_transformed_data)


demo_dag = demo()
