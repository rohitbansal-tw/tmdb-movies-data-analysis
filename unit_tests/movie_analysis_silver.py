# Databricks notebook source
import json
from datetime import datetime
from typing import Callable
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, TimestampType, DoubleType


def test_flatten_genres(spark, f: Callable):
    input_pandas = pd.DataFrame([
        {
            "id": 1,
            "genres": "[{\"id\": 28, \"name\": \"Action\"}, {\"id\": 12, \"name\": \"Adventure\"}, {\"id\": 14, \"name\": \"Fantasy\"}, {\"id\": 878, \"name\": \"Science Fiction\"}]"
        },
        {
            "id": 2,
            "genres": "[{\"id\": 29, \"name\": \"Action\"}]"
        },
    ])

    input_df = spark.createDataFrame(
        input_pandas,
        StructType([
            StructField("id", IntegerType()),
            StructField("genres", StringType()),
        ])
    )
    result = input_df.transform(f)
    print("Transformed DF")
    result.show()

    result_count = result.count()
    expected_count = 5
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"

    result_schema = result.schema
    expected_schema = StructType(
        [
            StructField('id', IntegerType(), True),
            StructField('genres', StringType(), True),
            StructField('genre_id', IntegerType(), True),
            StructField('genre_name', StringType(), True)
        ])
    assert result_schema == expected_schema, f"Expected {expected_schema}, but got {result_schema}"

    print("All tests pass! :)")

# COMMAND ----------


