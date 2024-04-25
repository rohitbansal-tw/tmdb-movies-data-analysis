# Databricks notebook source
# MAGIC %md
# MAGIC ## Notebook Setup - Silver

# COMMAND ----------

# MAGIC %pip uninstall -y databricks_helpers exercise_ev_databricks_unit_tests
# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers git+https://github.com/data-derp/exercise_ev_databricks_unit_tests#egg=exercise_ev_databricks_unit_tests

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import unit and e2e tests

# COMMAND ----------

# MAGIC %pip install great-expectations

# COMMAND ----------

# MAGIC %run ./unit_tests/movie_analysis_silver

# COMMAND ----------

# MAGIC %run ./e2e_tests/movie_analysis_silver

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
# MAGIC ## Clean-up data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fix data types on movie schema columns

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *

def fix_data_types_on_movie_schema(input_df: DataFrame) -> DataFrame:
    return input_df \
        .withColumn("id", input_df.id.cast("integer")) \
        .withColumn("budget", input_df.budget.cast("integer")) \
        .withColumn("runtime", input_df.runtime.cast("double")) \
        .withColumn("revenue", input_df.revenue.cast("integer")) \
        .withColumn("release_date", to_timestamp("release_date")) \
        .withColumn("vote_average", input_df.vote_average.cast("double")) \
        .withColumn("vote_count", input_df.vote_count.cast("integer"))

output_df = df\
    .transform(fix_data_types_on_movie_schema)
display(output_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Flatten movie genres

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

output_df = df\
    .transform(fix_data_types_on_movie_schema)\
    .transform(flatten_genres)

display(output_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run unit tests

# COMMAND ----------

test_flatten_genres(spark, flatten_genres)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write result to parquet in output dir

# COMMAND ----------

out_dir = f"{working_directory}/output"
print(out_dir)

# COMMAND ----------

def write_to_parquet(input_df: DataFrame):
    output_directory = f"{out_dir}"
    input_df.\
        write.\
        mode("overwrite").\
        parquet(output_directory)

write_to_parquet(output_df)

display(spark.createDataFrame(dbutils.fs.ls(f"{out_dir}")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Validation

# COMMAND ----------

# MAGIC %md 
# MAGIC Run data validation checks e2e for following:
# MAGIC * "id" is unique

# COMMAND ----------

run_data_validation(f"{working_directory}/output", spark, display)
