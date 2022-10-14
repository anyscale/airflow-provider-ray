from anyscale import AnyscaleSDK
from airflow.utils.context import Context

from airflow.models.baseoperator import BaseOperator
from airflow.compat.functools import cached_property


class AnyscaleBaseOperator(BaseOperator):
    """
    Anyscale Base Operator.
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

    def execute(self, context: Context):
        raise NotImplementedError('Please implement execute() in subclass')
