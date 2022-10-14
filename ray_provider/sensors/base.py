from anyscale import AnyscaleSDK
from airflow.utils.context import Context

from airflow.sensors.base import BaseSensorOperator
from airflow.compat.functools import cached_property


class AnyscaleBaseSensor(BaseSensorOperator):
    """
    Anyscale Base Sensor.
    :param auth_token: Anyscale CLI token.
    """

    def __init__(
        self,
        *,
        auth_token: str,
        **kwargs
    ):

        self.auth_token = auth_token
        super().__init__(**kwargs)

    @cached_property
    def sdk(self) -> AnyscaleSDK:
        return AnyscaleSDK(auth_token=self.auth_token)

    def poke(self, context: Context) -> bool:
        raise NotImplementedError("Please implement poke() in subclass")
