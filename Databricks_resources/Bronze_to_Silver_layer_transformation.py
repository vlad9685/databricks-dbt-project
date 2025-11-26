# Databricks notebook source
# CONFIGURATION
CATALOG = "workspace"
SCHEMA = "default"

# Paths to raw files
weather_path = f"/Volumes/{CATALOG}/{SCHEMA}/bronze/nyc_weather_2024_04.json"
citibike_path = f"/Volumes/{CATALOG}/{SCHEMA}/bronze/citibike_tripdata_2024_04.csv"
taxi_path = f"/Volumes/{CATALOG}/{SCHEMA}/bronze/yellow_tripdata_2024_04.parquet"

print("Paths configured.")


# READING THE DATA
# JSON weather data
df_weather_raw = spark.read.option("multiline", True).json(weather_path)

# CSV citibike data
df_citibike_raw = spark.read.csv(citibike_path, header=True, inferSchema=True)

# Parquet taxi data
df_taxi_raw = spark.read.parquet(taxi_path)

print("Raw DataFrames created. Displaying small samples:")
display(df_weather_raw.limit(5))
display(df_citibike_raw.limit(5))
display(df_taxi_raw.limit(5))


from pyspark.sql.functions import col, to_date, explode, arrays_zip

# CLEANING THE DATA
# The weather data is nested, therefore it needs to be exploded
df_weather_silver = df_weather_raw.select(
    explode(
        arrays_zip(
            col("daily.time"),
            col("daily.temperature_2m_max"),
            col("daily.temperature_2m_min"),
            col("daily.precipitation_sum"),
            col("daily.snowfall_sum")
        )
    ).alias("daily_data")
) \
.select(
    # After zipping, access the fields by their original names within the struct.
    to_date(col("daily_data.time")).alias("weather_date"),
    col("daily_data.temperature_2m_max").alias("temp_max_c"),
    col("daily_data.temperature_2m_min").alias("temp_min_c"),
    col("daily_data.precipitation_sum").alias("precipitation_mm"),
    col("daily_data.snowfall_sum").alias("snowfall_cm")
    )

# Cleaning the citibike data
df_citibike_silver = df_citibike_raw.select(
    col("started_at").cast("timestamp").alias("trip_start_timestamp"),
    col("ended_at").cast("timestamp").alias("trip_end_timestamp"),
    col("start_station_name"),
    col("end_station_name"),
    col("rideable_type")
)

# Cleaning the taxi data
df_taxi_silver = df_taxi_raw.select(
    col("tpep_pickup_datetime").cast("timestamp").alias("pickup_timestamp"),
    col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_timestamp"),
    col("passenger_count").cast("integer"),
    col("trip_distance").cast("double"),
    col("PULocationID").alias("pickup_location_id"),
    col("DOLocationID").alias("dropoff_location_id"),
    col("fare_amount").cast("double"),
    col("tip_amount").cast("double"),
    col("total_amount").cast("double")
)


print("Silver DataFrames created with cleaned data and correct schemas.")
df_weather_silver.printSchema()
df_citibike_silver.printSchema()
df_taxi_silver.printSchema()

# Define the names of the persisted silver tables
silver_weather_table_name = f"{CATALOG}.{SCHEMA}.silver_weather"
silver_citibike_table_name = f"{CATALOG}.{SCHEMA}.silver_citibike_trips"
silver_taxi_table_name = f"{CATALOG}.{SCHEMA}.silver_taxi_trips"

# Write the tables
df_weather_silver.write.mode("overwrite").saveAsTable(silver_weather_table_name)
df_citibike_silver.write.mode("overwrite").saveAsTable(silver_citibike_table_name)
df_taxi_silver.write.mode("overwrite").saveAsTable(silver_taxi_table_name)

print("Successfully created/updated Silver tables:")
print(f"- {silver_weather_table_name}")
print(f"- {silver_citibike_table_name}")
print(f"- {silver_taxi_table_name}")