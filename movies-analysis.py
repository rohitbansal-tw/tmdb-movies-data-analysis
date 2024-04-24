# Databricks notebook source
# MAGIC %run ./init

# COMMAND ----------

from databricks_helpers.databricks_helpers import DataDerpDatabricksHelpers

working_directory = helpers.working_directory()
print(f"Your current working directory is: {working_directory}")

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


