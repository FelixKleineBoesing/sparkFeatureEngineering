from typing import List
from pyspark.sql import DataFrame


def merge_datasets(left_df:  DataFrame, right_dfs: List[DataFrame], keys: list):
    left_df = left_df.repartition(*keys)
    for df in right_dfs:
        df = df.repartition(*keys)
        left_df = left_df.join(df, keys)
    return left_df


def run_feature_engineering_pipeline(table, funcs):
    table_cls = type(table)
    for func in funcs:
        table = func(table)
        assert isinstance(table, table_cls), f"The function {func.__name__} doesn´t return the desired DataFrame! " \
                                         f"Please fix this function"

    return table

