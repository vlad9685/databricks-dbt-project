# Databricks notebook source
#Load Gold data
gold_table = "workspace.dbt_models.gold_daily_metrics"

#Load the data into a dataframe
df = spark.read.table(gold_table)

#Select specific columns to use in the model
ml_df = df.select(
    "temp_max_c",
    "precipitation_mm",
    "snowfall_cm",
    "total_taxi_trips" #The label to predict
)

display(ml_df)

# COMMAND ----------

#Prepare data for ML
from pyspark.ml.feature import VectorAssembler

#Define the feature columns
feature_cols = ["temp_max_c", "precipitation_mm", "snowfall_cm"]

#Use VectorAssembler to combine the feature columns into a single vector column
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
df_assembled = assembler.transform(ml_df)

#Split the data: 80% for training, 20% for testing
(train_df, test_df) = df_assembled.randomSplit([0.8, 0.2], seed=42)

print("Data has been split into training and testing sets")

# COMMAND ----------

#Train and Evaluate
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# Create a Linear Regression model instance
lr = LinearRegression(featuresCol="features", labelCol="total_taxi_trips")

# Train the model using the training data
lr_model = lr.fit(train_df)
print("Model training complete.")

# Use the trained model to make predictions on the unseen test data
predictions_df = lr_model.transform(test_df)

print("Predictions made on test data:")
display(predictions_df.select("total_taxi_trips", "prediction", "temp_max_c", "precipitation_mm"))

# Use an evaluator to measure the model's performance (lower is better)
evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="total_taxi_trips", metricName="rmse")
rmse = evaluator.evaluate(predictions_df)

print(f"Model Performance on Test Data: Root Mean Squared Error (RMSE) = {rmse}")