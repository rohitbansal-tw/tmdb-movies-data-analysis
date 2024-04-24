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

movies_url = "https://raw.githubusercontent.com/rohitbansal-tw/tmdb-movies-data-analysis/main/datasets/tmdb_5000_movies.csv"
movies_file_path = helpers.download_to_local_dir(movies_url)

# COMMAND ----------


