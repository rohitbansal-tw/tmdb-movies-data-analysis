# Databricks notebook source
# MAGIC %pip uninstall -y databricks_helpers exercise_ev_databricks_unit_tests
# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers git+https://github.com/data-derp/exercise_ev_databricks_unit_tests#egg=exercise_ev_databricks_unit_tests

# COMMAND ----------

from databricks_helpers.databricks_helpers import DataDerpDatabricksHelpers

exercise_name = "tmdb-movies-silver"
helpers = DataDerpDatabricksHelpers(dbutils, exercise_name)

current_user = helpers.current_user()
working_directory = helpers.working_directory()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Data from Bronze Layer
# MAGIC Let's read the parquet files that we created in the Bronze layer!

# COMMAND ----------

bronze_input_location = working_directory.replace("silver", "bronze")
bronze_output_folder = f"{bronze_input_location}/output"

dbutils.fs.ls(bronze_output_folder)

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def read_parquet(filepath: str) -> DataFrame:
    df = spark.read.parquet(filepath)
    return df
    
df = read_parquet(bronze_output_folder)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fix data types on movie schema columns

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *

df_cleaned = df \
    .withColumn("id", df.id.cast("integer")) \
    .withColumn("budget", df.budget.cast("integer")) \
    .withColumn("runtime", df.runtime.cast("double")) \
    .withColumn("revenue", df.revenue.cast("integer")) \
    .withColumn("release_date", to_timestamp("release_date")) \
    .withColumn("vote_average", df.vote_average.cast("double")) \
    .withColumn("vote_count", df.vote_count.cast("integer"))

display(df_cleaned)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flatten movie genres

# COMMAND ----------

from pyspark.sql import DataFrame

def flatten_genres(input_df: DataFrame) -> DataFrame:
    genre_schema = ArrayType(
        StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ]
    ))
    df_parsed = input_df \
        .withColumn("genres_array", from_json(input_df["genres"], genre_schema)
    )
    df_exploded = df_parsed \
        .withColumn("genre", explode(df_parsed.genres_array)
    )
    return df_exploded \
        .withColumn("genre_id", df_exploded.genre.id) \
        .withColumn("genre_name", df_exploded["genre"]["name"]) \
        .drop("genres_array", "genre")

df_cleaned = df_cleaned.transform(flatten_genres)
display(df_cleaned)

# COMMAND ----------

# MAGIC %run ./unit_tests/movie_analysis_silver

# COMMAND ----------

test_flatten_genres(spark, flatten_genres)
