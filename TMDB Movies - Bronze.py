# Databricks notebook source
# MAGIC %md
# MAGIC ## Movie Processing - Bronze

# COMMAND ----------

# MAGIC %pip uninstall -y databricks_helpers exercise_ev_databricks_unit_tests
# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers
# MAGIC %pip install pandas

# COMMAND ----------

from databricks_helpers.databricks_helpers import DataDerpDatabricksHelpers

exercise_name = "tmdb-movies-bronze"
helpers = DataDerpDatabricksHelpers(dbutils, exercise_name)

current_user = helpers.current_user()
working_directory = helpers.working_directory()

movies_url = "https://raw.githubusercontent.com/rohitbansal-tw/tmdb-movies-data-analysis/main/datasets/tmdb_5000_movies.csv"
movies_file_path = helpers.download_to_local_dir(movies_url)

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
      
def create_dataframe(filepath: str) -> DataFrame:
    movies_schema = StructType([
        StructField("budget", StringType(), True),
        StructField("genres", StringType(), True),
        StructField("homepage", StringType(), True),
        StructField("id", StringType(), True),
        StructField("keywords", StringType(), True),
        StructField("original_language", StringType(), True),
        StructField("original_title", StringType(), True),
        StructField("overview", StringType(), True),
        StructField("popularity", StringType(), True),
        StructField("production_companies", StringType(), True),
        StructField("production_countries", StringType(), True),
        StructField("release_date", StringType(), True),
        StructField("revenue", StringType(), True),
        StructField("runtime", StringType(), True),
        StructField("spoken_languages", StringType(), True),
        StructField("status", StringType(), True),
        StructField("tagline", StringType(), True),
        StructField("title", StringType(), True),
        StructField("vote_average", StringType(), True),
        StructField("vote_count", StringType(), True),
    ])

    return spark.read.format("csv") \
    .option("header", True) \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("multiline", True) \
    .option("delimiter", ",") \
    .schema(movies_schema) \
    .load(filepath)
    
df = create_dataframe(movies_file_path)
display(df)

# COMMAND ----------

def write(input_df: DataFrame):
    out_dir = f"{working_directory}/output/"
    mode_name = "overwrite"
    input_df. \
        write. \
        mode(mode_name). \
        parquet(out_dir)
    
write(df)

dbutils.fs.ls(f"{working_directory}/output/")

# COMMAND ----------

from e2e_test.movie_analysis_bronze import test_write_e2e

test_write_e2e(dbutils.fs.ls(f"{working_directory}/output"), spark, display)


# COMMAND ----------


