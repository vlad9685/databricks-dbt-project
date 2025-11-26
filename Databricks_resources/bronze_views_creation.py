# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.default.bronze_weather AS
# MAGIC SELECT * FROM json.`/Volumes/workspace/default/bronze/nyc_weather_2024_04.json`;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW workspace.default.bronze_citibike_trips AS
# MAGIC SELECT * FROM csv.`/Volumes/workspace/default/bronze/citibike_tripdata_2024_04.csv`
# MAGIC WITH (
# MAGIC   header = 'true',
# MAGIC   inferSchema = 'true'
# MAGIC );
# MAGIC
# MAGIC CREATE OR REPLACE VIEW workspace.default.bronze_taxi_trips AS
# MAGIC SELECT * FROM parquet.`/Volumes/workspace/default/bronze/yellow_tripdata_2024_04.parquet`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES