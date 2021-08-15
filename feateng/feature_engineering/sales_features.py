from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from feateng.data_input.table import Sales, Customers, HiveTable
from datetime import datetime
from feateng.feature_engineering.feature_transformer import FeatureGenerator


class AgeTransformer(FeatureGenerator):

    def _inner_call(self, table: HiveTable):
        return table.withColumn("age", datetime.now().year - F.col("birthyear"))
