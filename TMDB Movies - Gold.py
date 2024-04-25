# Databricks notebook source
# MAGIC %md
# MAGIC ## Notebook Setup - Gold
# MAGIC

# COMMAND ----------

# MAGIC %pip uninstall -y databricks_helpers exercise_ev_databricks_unit_tests
# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers git+https://github.com/data-derp/exercise_ev_databricks_unit_tests#egg=exercise_ev_databricks_unit_tests

# COMMAND ----------

# MAGIC %pip install great-expectations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import unit and e2e tests

# COMMAND ----------

# MAGIC %run ./unit_tests/movie_analysis_gold

# COMMAND ----------

# MAGIC %run ./e2e_tests/movie_analysis_gold

# COMMAND ----------

from databricks_helpers.databricks_helpers import DataDerpDatabricksHelpers

exercise_name = "tmdb-movies-gold"
helpers = DataDerpDatabricksHelpers(dbutils, exercise_name)

current_user = helpers.current_user()
working_directory = helpers.working_directory()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Data from Silver Layer
# MAGIC Let's read the parquet files that we created in the Silver layer!

# COMMAND ----------

silver_input_location = working_directory.replace("gold", "silver")
silver_output_folder = f"{silver_input_location}/output"

dbutils.fs.ls(silver_output_folder)

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def read_parquet(filepath: str) -> DataFrame:
    df = spark.read.parquet(filepath)
    return df
    
df = read_parquet(silver_output_folder)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Most popular genres in last 10 years

# COMMAND ----------

# MAGIC %md
# MAGIC A genre is more popular if "all" movies belonging to this genre on chart have "average" `popularity` across movies more than other genres

# COMMAND ----------

from datetime import datetime, timezone, timedelta
from dateutil import parser
from pyspark.sql.functions import year, month, dayofmonth, hour, minute, col, avg, row_number
from pyspark.sql import DataFrame

def apply_date_filter(input_df: DataFrame, from_date: datetime) -> DataFrame:
    return input_df.where(col("release_date") > from_date)

def most_popular_genre(input_df: DataFrame, now: datetime, years: int) -> DataFrame:
    last_n_years: datetime = now - timedelta(days=years*365)
    filtered_df = input_df.orderBy(col("release_date").desc())\
        .transform(apply_date_filter, last_n_years)
    
    return filtered_df\
            .select("genre_name", "popularity", "release_date")\
            .groupBy("genre_name")\
            .agg(avg("popularity").alias("average_popularity"))\
            .sort(col("average_popularity").desc())
    
output_df = df\
    .transform(most_popular_genre, datetime.now(tz=timezone.utc), 10)

display(output_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Rate of change of movie budget per year

# COMMAND ----------

# MAGIC %md
# MAGIC We will be calculating the rate of change of budget of the movies year wise. We will assume that movies created with budget less than or equal to 0 are not correct entries and thus we will exclude those for this exercise.
# MAGIC
# MAGIC We need to clarify, if partially correct data should be excluded completely in the silver/gold dataset.

# COMMAND ----------

def find_rate_of_change_of_budget(df: DataFrame) -> DataFrame:
    return df.select("id","budget", year("release_date").alias("year"))\
        .filter(col("budget") > 0)\
        .distinct()\
        .groupBy("year")\
        .agg(avg("budget").alias("avg_budget"))\
        .orderBy(col("year").desc())

display(df.transform(find_rate_of_change_of_budget))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Most profitable genre in last 'N' years

# COMMAND ----------

# MAGIC %md
# MAGIC For last n years, calculate "average" on total profit across all movies, per genre, per year. Most profitable genres in profit terms is going to be displayed in descending order. 
# MAGIC
# MAGIC Assumption: Exclude movies with budget=0, review is NULL

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *

def find_distint_movies_per_genre(input_df: DataFrame) -> DataFrame:
    return input_df.select("id","budget", "genre_name", "revenue", year("release_date").alias("year")) \
        .filter(col("budget") > 0) \
        .filter(col("revenue").isNotNull()) \
        .distinct()

def find_most_profitable_genre(df: DataFrame, now: datetime, years: int) -> DataFrame:
    window_partition = Window.partitionBy("year", "genre_name").orderBy("year")

    last_n_years: datetime = now - timedelta(days=years*365)
    return df \
        .transform(apply_date_filter, last_n_years) \
        .transform(find_distint_movies_per_genre) \
        .withColumn("total_revenue", sum("revenue").over(window_partition)) \
        .withColumn("total_budget", sum("budget").over(window_partition)) \
        .withColumn("rank", row_number().over(window_partition)) \
        .filter(col("rank") == 1) \
        .withColumn("total_profit", col("total_revenue") - col("total_budget")) \
        .select("year", "genre_name", "total_profit") \
        .orderBy(col("year").desc())
        

output_df = df.transform(find_most_profitable_genre, datetime.now(tz=timezone.utc), 20)
display(output_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Movies by production company

# COMMAND ----------

def movies_by_production_company_df(input_df: DataFrame) -> DataFrame:
    return input_df.select("id", "production_company_name") \
        .distinct() \
        .groupBy("production_company_name") \
        .count().withColumnRenamed("count", "count_movies") \
        .orderBy(col("count_movies").desc())

output_df = df\
    .transform(movies_by_production_company_df)

display(output_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Most popular movie in the country

# COMMAND ----------

from pyspark.sql.functions import first

def popular_movie_by_country(input_df: DataFrame) -> DataFrame:
    window_country = Window.partitionBy("production_country_name").orderBy(col("popularity").desc())

    popular_movie_by_country_df = (
        input_df.select("production_country_name", "original_title", "popularity") \
            .withColumn("rank", row_number().over(window_country))
            .filter(col("rank") == 1)
            .drop("rank")
    )    
    return popular_movie_by_country_df

display(popular_movie_by_country(df))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Movies by release year

# COMMAND ----------

from pyspark.sql.functions import year

def moviesByYear(input_df: DataFrame) -> DataFrame:
    return input_df.select("id","release_year").distinct().groupBy("release_year").count()

display(moviesByYear(df))
