# Databricks notebook source
from pyspark.sql import DataFrame


def test_write_e2e(file_path: DataFrame, spark, display_f, **kwargs):
    result = spark.createDataFrame(file_path)
    display_f(result)
    
    result_count = result.filter(result.name.endswith(".snappy.parquet")).count()

    expected_count = 1
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"

    print("All tests pass! :)")

# COMMAND ----------


