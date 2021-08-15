import abc
from pyspark.sql import DataFrame, SparkSession


class HiveTable(abc.ABC, DataFrame):

    def __init__(self, spark_session: SparkSession):
        df = self._get_table_from_spark_session(spark_session=spark_session)
        assert isinstance(df, DataFrame), f"_get_table_from_spark_session doesn´t return a Spark DataFrame but an " \
                                          f"object of instance {type(df)}! Please fix this!"
        super(DataFrame).__init__(df._jdf, df.sql_ctx)

    @abc.abstractmethod
    def _get_table_from_spark_session(self, spark_session: SparkSession) -> DataFrame:
        return spark_session.sql("Select * from table")


class Sales(HiveTable):

    used_features = ["id", "customer_id", "time", "Amount"]

    def _get_table_from_spark_session(self, spark_session: SparkSession):
        return spark_session.sql("Select {} from sales".format(", ".join(self.used_features)))


class Customers(HiveTable):

    used_features = ["id", "first_name", "last_name", "age"]

    def _get_table_from_spark_session(self, spark_session: SparkSession):
        return spark_session.sql("Select {} from customers".format(", ".join(self.used_features)))
