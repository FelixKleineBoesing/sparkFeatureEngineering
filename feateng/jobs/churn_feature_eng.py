from feateng.data_input.table import Sales, Customers
from feateng.helpers import start_spark
from feateng.feature_engineering import SALES_TRANSFORMATIONS, CUSTOMERS_TRANSFORMATIONS
from feateng.jobs.shared import merge_datasets, run_feature_engineering_pipeline


def main():
    spark_session = start_spark()[0]

    customers = Customers(spark_session)
    customers = run_feature_engineering_pipeline(customers, )

    sales = Sales(spark_session)
    sales = run_feature_engineering_pipeline(sales, SALES_TRANSFORMATIONS)

    data_frames = [customers, sales]
    joined_dataframe = merge_datasets(customers, data_frames, keys=["id"])


if __name__ == "__main__":
    pass
