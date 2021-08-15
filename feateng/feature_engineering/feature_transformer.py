import abc
from typing import List

from feateng.data_input.table import HiveTable


class FeatureGenerator(abc.ABC):

    def __call__(self, table: HiveTable):
        assert isinstance(table, HiveTable)
        res = self._inner_call(table)
        assert isinstance(res, HiveTable)
        return res

    @abc.abstractmethod
    def _inner_call(self, table: HiveTable):
        """
        your feature generation logic should be implemented here

        :param table:
        :return:
        """
        pass


class GroupByFeatures(abc.ABC):

    def __init__(self):
        self.features = None
        self.group_keys = None
        self._set_params()
        assert isinstance(self.features, list)
        assert isinstance(self.group_keys, list)
        assert len(self.features) > 0
        assert len(self.group_keys) > 0

    @abc.abstractmethod
    def _set_params(self):
        """
        set the params for group by action:
            - features
            - group_keys

        ```
        self.group_keys = ["customer_id"]
        self.features = [F.avg("amount").alias("avg_amount"), F.sum("amount").alias("sum_amount")]
        ```
        :return: None
        """
        pass


class WindowFeatures(abc.ABC):

    def __init__(self):
        self.partition_keys = None
        self._set_params()
        assert isinstance(self.partition_keys, list)
        assert len(self.partition_keys) > 0

    @abc.abstractmethod
    def _set_params(self):
        """
        set the params for window creation:
            - partition_keys

        ```
        self.partition_keys = ["customer_id"]
        ```
        :return: None
        """
        pass

    def __call__(self, table: HiveTable):
        assert isinstance(table, HiveTable)
        res = self._inner_call(table)
        assert isinstance(res, HiveTable)
        return res

    @abc.abstractmethod
    def _inner_call(self, table: HiveTable, window):
        """
        your feature generation logic should be implemented here

        :param table:
        :param window: initialised window over the specified partition_keys
        :return:
        """
        pass
