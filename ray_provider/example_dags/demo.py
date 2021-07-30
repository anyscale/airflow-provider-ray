from airflow.decorators import dag, task
from ray_provider.decorators import ray_task
from ray_provider.xcom.ray_backend import RayBackend
from airflow.operators.python import get_current_context


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
    def load_data():
        return 1

    # @ray_task(**task_args)
    # def retry_this_task(data):

    #     context = get_current_context()
    #     for each in get_current_context.items():
    #         print(each)
    #     ti = context["ti"]
    #     return 1

    @ray_task(**task_args)
    def transform_data(data):
        return data * 100

    @ray_task(**task_args)
    def divide_by_zero(data):
        return data/0

    the_data = load_data()
    # the_retried_task = retry_this_task(the_data)
    the_transformed_data = transform_data(the_data)

    divide_by_zero_output = divide_by_zero(the_transformed_data)


demo_dag = demo()
