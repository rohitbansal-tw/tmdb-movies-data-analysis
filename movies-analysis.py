# Databricks notebook source
# MAGIC %pip uninstall -y databricks_helpers exercise_ev_databricks_unit_tests
# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers
# MAGIC %pip install pandas

# COMMAND ----------

from databricks_helpers.databricks_helpers import DataDerpDatabricksHelpers

exercise_name = "tmdb-movies"
helpers = DataDerpDatabricksHelpers(dbutils, exercise_name)

current_user = helpers.current_user()
working_directory = helpers.working_directory()

print(f"Your current working directory is: {working_directory}")

# COMMAND ----------

movies_url = "https://raw.githubusercontent.com/rohitbansal-tw/tmdb-movies-data-analysis/main/datasets/tmdb_5000_movies.csv"
movies_file_path = helpers.download_to_local_dir(movies_url)
dbutils.fs.ls(working_directory)

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
      
def create_dataframe(filepath: str) -> DataFrame:
    return spark.read.format("csv") \
    .option("header", True) \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("multiline", True) \
    .option("delimiter", ",") \
    .load(filepath)
    
df = create_dataframe(movies_file_path)
df \
.withColumn("release_date", to_timestamp(col("release_date"))) \
.printSchema()

# COMMAND ----------


