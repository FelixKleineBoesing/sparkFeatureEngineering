from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from feateng.data_input.table import Sales, Customers
from datetime import datetime


def get_age(customers: Customers):
    customers = customers.withColumn("age", datetime.now().year - F.col("birthyear"))
    return customers

