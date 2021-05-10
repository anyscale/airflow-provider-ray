from typing import Any

import json
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
import ray
from ray_provider.decorators.ray_decorators import ray_task
import numpy as np
import xgboost_ray as xgb
from ray_provider.xcom.ray_backend import RayBackend
from datetime import datetime

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
    "on_success_callback": RayBackend.on_success_callback,
    "on_failure_callback": RayBackend.on_failure_callback,
}

task_args = {"ray_conn_id": "ray_cluster_connection"}


SIMPLE = False

DataFrame = Any


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2021, 1, 1, 0, 0, 0),
    tags=["xgboost-modin"],
)
def xgboost_modin_breast_cancer():
    @ray_task(eager=True, **task_args)
    def load_dataframe() -> DataFrame:
        """Build a dataframe task."""
        print("Loading CSV.")
        if SIMPLE:
            print("Loading simple")
            from sklearn import datasets

            data = datasets.load_breast_cancer(return_X_y=True)
        else:
            url = (
                "https://archive.ics.uci.edu/ml/machine-learning-databases/"
                "00280/HIGGS.csv.gz"
            )
            import modin.pandas as mpd

            colnames = ["label"] + ["feature-%02d" % i for i in range(1, 29)]
            data = mpd.read_csv(url, names=colnames)

            print("loaded higgs")
        print("Loaded CSV.")
        return data

    @ray_task(**task_args)
    def create_data(data):
        if SIMPLE:
            from sklearn.model_selection import train_test_split

            data, labels = data
            train_x, test_x, train_y, test_y = train_test_split(
                data, labels, test_size=0.25
            )

            train_set = xgb.RayDMatrix(train_x, train_y)
            test_set = xgb.RayDMatrix(test_x, test_y)
        else:
            df_train = data[(data["feature-01"] < 0.4)]
            colnames = ["label"] + ["feature-%02d" % i for i in range(1, 29)]
            train_set = xgb.RayDMatrix(df_train, label="label", columns=colnames)
            df_validation = data[
                (data["feature-01"] >= 0.4) & (data["feature-01"] < 0.8)
            ]
            test_set = xgb.RayDMatrix(df_validation, label="label")
        return train_set, test_set

    @ray_task(**task_args)
    def train_model(data) -> None:
        dtrain, dvalidation = data
        evallist = [(dvalidation, "eval")]
        evals_result = {}
        config = {
            "tree_method": "hist",
            "eval_metric": ["logloss", "error"],
        }
        bst = xgb.train(
            params=config,
            dtrain=dtrain,
            evals_result=evals_result,
            ray_params=xgb.RayParams(
                max_actor_restarts=1, num_actors=2, cpus_per_actor=2
            ),
            num_boost_round=100,
            evals=evallist,
        )
        return bst

    build_raw_df = load_dataframe()
    data = create_data(build_raw_df)
    trained_model = train_model(data)

    kickoff_dag = DummyOperator(task_id="kickoff_dag")
    complete_dag = DummyOperator(task_id="complete_dag")

    kickoff_dag >> build_raw_df
    trained_model >> complete_dag


xgboost_modin_breast_cancer = xgboost_modin_breast_cancer()
