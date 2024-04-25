# Databricks notebook source
from pyspark.sql import DataFrame
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
import json

def __check_result(result, display_f):
    is_passed = result["success"]
    if not is_passed:
        display_f(result)
        assert result["success"]

def validate_id(input_df: SparkDFDataset, display_f):
    print("Validating 'id' column ...")
    result = input_df.expect_column_values_to_not_be_null("id")
    __check_result(result, display_f)

    result = input_df.expect_column_values_to_be_unique("id")
    __check_result(result, display_f)

def validate_status(input_df: SparkDFDataset, display_f):
    print("Validating 'status' column ...")
    result = input_df.expect_column_values_to_match_regex_list("status", ["Post Production", "Released", "Rumored"])
    __check_result(result, display_f)

def validate_vote_average(input_df: SparkDFDataset, display_f):
    print("Validating 'vote_average' column ...")
    result = input_df.expect_column_values_to_be_between("vote_average", 0.0, 10.0)
    __check_result(result, display_f)
    
def run_data_validation(file_path: DataFrame, spark, display_f, **kwargs):
    result = spark.read.parquet(file_path)
    df_test = SparkDFDataset(result)

    validate_id(df_test, display_f)
    validate_status(df_test, display_f)
    validate_vote_average(df_test, display_f)

    print("All tests pass! :)")
