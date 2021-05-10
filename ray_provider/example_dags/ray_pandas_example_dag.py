import json
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
import ray
from ray_provider.decorators.ray_decorators import ray_task
import numpy as np
import pandas as pd
from ray_provider.xcom.ray_backend import RayBackend
from datetime import datetime

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
    "on_success_callback": RayBackend.on_success_callback,
    "on_failure_callback": RayBackend.on_failure_callback,
}


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2021, 1, 1, 0, 0, 0),
    tags=["pandas-example"],
)
def task_flow_ray_pandas_example():
    @ray_task(ray_conn_id="ray_cluster_connection")
    def build_dataframe() -> pd.DataFrame:
        """
        #### build random dataframe task

        """
        df = pd.DataFrame(
            np.random.randint(0, 1000, size=(1000, 6)), columns=list("ABCDEF")
        )

        return df

    @ray_task(ray_conn_id="ray_cluster_connection")
    def sum_cols(df: pd.DataFrame) -> pd.DataFrame:
        """
        #### Transform
        """
        return pd.DataFrame(df.sum()).T

    @ray_task(ray_conn_id="ray_cluster_connection")
    def pick_least(df: pd.DataFrame) -> int:
        min_val = df.T.min()
        print("Min Val is %d" % min_val)
        return min_val

    @ray_task(ray_conn_id="ray_cluster_connection")
    def pick_greatest(df: pd.DataFrame) -> int:
        max_val = df.T.max()
        print("Max Val is %d" % max_val)
        return max_val

    @ray_task(ray_conn_id="ray_cluster_connection")
    def calc_mean(df: pd.DataFrame) -> int:
        mean = df.T.mean()
        print("Mean Val is %.2f" % mean)
        return mean

    @ray_task(ray_conn_id="ray_cluster_connection")
    def calc_std_dev(df: pd.DataFrame) -> int:
        std = df.T.std()
        print("Std Deviation is %.2f" % std)
        return std

    @ray_task(ray_conn_id="ray_cluster_connection")
    def calc_variance(df: pd.DataFrame) -> int:
        var = df.T.var()
        print("Variance Val is %.2f" % var)
        return var

    @ray_task(ray_conn_id="ray_cluster_connection")
    def calc_median(df: pd.DataFrame) -> int:
        median = df.T.median()
        print("Median Val is %.2f" % median)
        return median

    @ray_task(ray_conn_id="ray_cluster_connection")
    def load_results(
        min_val: int, max_val: int, mean: float, std: float, var: float, median: float
    ) -> None:
        """
        #### Load task
        This will print max and min
        """

        print("The final min is: %d" % min_val)
        print("The final max is: %d" % max_val)
        print("The final mean is: %.2f" % mean)
        print("The final std dev is: %.2f" % std)
        print("The final var is: %.2f" % var)
        print("The final median is: %.2f" % median)

    build_raw_df = build_dataframe()
    sum_cols = sum_cols(build_raw_df)
    pick_least = pick_least(sum_cols)
    pick_greatest = pick_greatest(sum_cols)
    calc_mean = calc_mean(sum_cols)
    calc_std_dev = calc_std_dev(sum_cols)
    calc_variance = calc_variance(sum_cols)
    calc_median = calc_median(sum_cols)

    load_results = load_results(
        pick_least, pick_greatest, calc_mean, calc_std_dev, calc_variance, calc_median
    )

    kickoff_dag = DummyOperator(task_id="kickoff_dag")
    complete_dag = DummyOperator(task_id="complete_dag")

    kickoff_dag >> build_raw_df
    load_results >> complete_dag


task_flow_ray_pandas_example = task_flow_ray_pandas_example()
