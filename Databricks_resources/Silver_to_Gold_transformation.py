# Databricks notebook source
# CONFIGURATION
CATALOG = "workspace"
SCHEMA = "default"

silver_weather_table = f"{CATALOG}.{SCHEMA}.silver_weather"
silver_taxi_table = f"{CATALOG}.{SCHEMA}.silver_taxi_trips"
silver_citibike_table = f"{CATALOG}.{SCHEMA}.silver_citibike_trips"

# Reading the silver tables into DataFrames
df_weather = spark.table(silver_weather_table)
df_taxi = spark.table(silver_taxi_table)
df_citibike = spark.table(silver_citibike_table)

print("Successfully loaded Silver tables into DataFrames.")

print("Weather data:")
display(df_weather)
print("Taxi data:")
display(df_taxi)
print("CitiBike data:")
display(df_citibike)


from pyspark.sql.functions import to_date, count, sum, col, year, month

# Aggregating citibike data by day
# Count the total number of trips per day
df_citibike_daily = df_citibike \
    .withColumn("trip_date", to_date(col("trip_start_timestamp"))) \
    .groupby("trip_date") \
    .agg(
        count("*").alias("total_citibike_trips")
    )

# Aggregating taxi data by day
# Count the total number of trips, passangers and revenue per day
df_taxi_daily = df_taxi \
    .filter(year(col("pickup_timestamp")) == 2024) \
    .filter(month(col("pickup_timestamp")) == 4) \
    .withColumn("trip_date", to_date(col("pickup_timestamp"))) \
    .groupby("trip_date") \
    .agg(
        count("*").alias("total_taxi_trips"),
        sum("passenger_count").alias("total_passengers"),
        sum("total_amount").alias("total_revenue")
    )

print("Daily aggregations for Citi Bike and Taxi trips are complete.")
print("Citi Bike Daily Summary:")
display(df_citibike_daily)
print("Taxi Daily Summary:")
display(df_taxi_daily)


# Joining aggregated data with the weather data

#Avoiding column name ambiguity
citibike_daily_renamed = df_citibike_daily.withColumnRenamed("trip_date", "weather_date")
taxi_daily_renamed = df_taxi_daily.withColumnRenamed("trip_date", "weather_date")

# Starting with the weather data as our base and then joining the citibike and taxi data
df_gold_daily_metrics = df_weather \
    .join(citibike_daily_renamed, on="weather_date", how="left") \
    .join(taxi_daily_renamed, on="weather_date", how="left") \
    .select(
        "weather_date",
        "temp_max_c",
        "temp_min_c",
        "precipitation_mm",
        "snowfall_cm",
        "total_citibike_trips",
        "total_taxi_trips",
        "total_passengers",
        "total_revenue"
    ) \
    .fillna(0, subset=["total_citibike_trips", "total_taxi_trips", "total_passengers", "total_revenue"]) \
    .orderBy("weather_date")

print("Gold DataFrame created by joining daily weather and trip data.")
print("Final Gold Table Schema:")
df_gold_daily_metrics.printSchema()

print("Displaying the final Gold table:")
display(df_gold_daily_metrics)


# Saving the data frame as a persistent table
gold_table_name = f"{CATALOG}.{SCHEMA}.gold_daily_transportation_metrics"
df_gold_daily_metrics.write.mode("overwrite").saveAsTable(gold_table_name)

print(f"Successfully created/updated Gold table: {gold_table_name}")