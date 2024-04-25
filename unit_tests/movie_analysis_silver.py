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
            StructField('genre_id', IntegerType(), True),
            StructField('genre_name', StringType(), True)
        ])
    assert result_schema == expected_schema, f"Expected {expected_schema}, but got {result_schema}"

    print("All tests pass! :)")

# COMMAND ----------

def test_flatten_keywords(spark, f: Callable):
    input_pandas = pd.DataFrame([
        {
            "id": 1,
            "keywords": "[{\"id\": 1463, \"name\": \"culture clash\"}, {\"id\": 2964, \"name\": \"future\"}]"
        },
        {
            "id": 2,
            "keywords": "[{\"id\": 123, \"name\": \"horror\"}]"
        },
    ])

    input_df = spark.createDataFrame(
        input_pandas,
        StructType([
            StructField("id", IntegerType()),
            StructField("keywords", StringType()),
        ])
    )
    result = input_df.transform(f)
    print("Transformed DF")
    result.show()

    result_count = result.count()
    expected_count = 3
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"

    result_schema = result.schema
    expected_schema = StructType(
        [
            StructField('id', IntegerType(), True),
            StructField('keyword_id', IntegerType(), True),
            StructField('keyword_name', StringType(), True)
        ])
    assert result_schema == expected_schema, f"Expected {expected_schema}, but got {result_schema}"

    print("All tests pass! :)")

# COMMAND ----------

def test_flatten_production_companies(spark, f: Callable):
    input_pandas = pd.DataFrame([
        {
            "id": 1,
            "production_companies": "[{\"name\":\"WaltDisneyPictures\",\"id\":2},{\"name\":\"JerryBruckheimerFilms\",\"id\":130},{\"name\":\"InfinitumNihil\",\"id\":2691},{\"name\":\"SilverBulletProductions(II)\",\"id\":37380},{\"name\":\"BlindWinkProductions\",\"id\":37381},{\"name\":\"ClassicMedia\",\"id\":37382}]"
        },
        {
            "id": 2,
            "production_companies": "[{\"name\":\"AmblinEntertainment\",\"id\":150}]"
        },
    ])

    input_df = spark.createDataFrame(
        input_pandas,
        StructType([
            StructField("id", IntegerType()),
            StructField("production_companies", StringType()),
        ])
    )
    result = input_df.transform(f)
    print("Transformed DF")
    result.show()

    result_count = result.count()
    expected_count = 7
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"

    result_schema = result.schema
    expected_schema = StructType(
        [
            StructField('id', IntegerType(), True),
            StructField('production_company_id', IntegerType(), True),
            StructField('production_company_name', StringType(), True)
        ])
    assert result_schema == expected_schema, f"Expected {expected_schema}, but got {result_schema}"

    print("All tests pass! :)")
