"""
Unittest module for Airflow Ray Provider Hook

1. Reference an active Ray endpoint in the `AIRFLOW_CONN_RAY_CLUSTER_CONNECTION` environment variable.
2. Run:

    AIRFLOW_CONN_RAY_CLUSTER_CONNECTION=http://@192.168.1.69:10001 python3 -m unittest tests.hooks.test_hooks.TestRayHook

"""
import logging
import os
import pytest
import unittest
from unittest import mock

import ray

from ray_provider.hooks.ray_client import RayClientHook


log = logging.getLogger(__name__)


class TestRayHook(unittest.TestCase):
    """
    Test connection to Ray Cluster.
    """

    def test_conn(self):

        hook = RayClientHook(ray_conn_id="ray_cluster_connection")
        hook.connect()
        assert ray.util.client.ray.is_connected() == True
        hook.disconnect()
