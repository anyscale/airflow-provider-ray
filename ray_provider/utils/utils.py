import json

from copy import deepcopy
from airflow.utils.context import Context


def push_to_xcom(result: dict, context: Context, ignore_keys: list = None):

    if ignore_keys is None:
        ignore_keys = []

    ti = context["ti"]
    result_copy = deepcopy(result)

    for key, value in result_copy.items():

        if key in ignore_keys:
            continue

        if type(value) is dict:
            value = json.dumps(value, default=str)

        value = str(value)
        ti.xcom_push(key=key, value=value)
