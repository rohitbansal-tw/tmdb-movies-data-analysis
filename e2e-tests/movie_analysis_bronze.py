from pyspark.sql import DataFrame


def test_write_e2e(input_df: DataFrame, spark, display_f, **kwargs):
    result = spark.createDataFrame(input_df)
    display_f(result)
    result_count = result.filter(result.name.endswith(".snappy.parquet")).count()

    expected_count = 1
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"

    print("All tests pass! :)")