# Databricks notebook source
# MAGIC %md
# MAGIC **Load CSV into a PySpark DataFrame
# MAGIC  **

# COMMAND ----------

# Step 1: Load the CSV file from FileStore into a dataframe

file_path="/FileStore/tables/nyc_taxi_sample.csv"
df = spark.read.option("header", True).option("inferschema",True).csv(file_path)

#show sample
df.printSchema()
df.show(15)


# COMMAND ----------

# MAGIC %md
# MAGIC **Clean & Transform the Data
# MAGIC Let’s:
# MAGIC
# MAGIC Convert string columns to timestamp
# MAGIC
# MAGIC Drop rows with nulls in important fields**

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col

# Step 2: Clean data
df_clean = df.withColumn("pickup_datetime", to_timestamp(col("tpep_pickup_datetime"))) \
             .withColumn("dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime"))) \
             .dropna(subset=["pickup_datetime", "dropoff_datetime", "passenger_count", "trip_distance"])

# Optional: drop original string columns
df_clean = df_clean.drop("tpep_pickup_datetime", "tpep_dropoff_datetime")

df_clean.show(15)


# COMMAND ----------

# MAGIC %md
# MAGIC STEP 3: Save as Delta Table

# COMMAND ----------

 
df_clean.write.format("delta").mode("overwrite").save("/tmp/nyc_taxi_clean_delta")

# Register the table for SQL use
spark.sql("DROP TABLE IF EXISTS nyc_taxi_clean")
spark.sql("""
    CREATE TABLE nyc_taxi_clean
    USING DELTA
    LOCATION '/tmp/nyc_taxi_clean_delta'
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ** STEP 4: Query the Data Using SQL**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- How many records?
# MAGIC SELECT COUNT(*) FROM nyc_taxi_clean;
# MAGIC
# MAGIC -- Average trip distance by passenger count
# MAGIC SELECT 
# MAGIC   passenger_count, 
# MAGIC   ROUND(AVG(trip_distance), 2) AS avg_distance 
# MAGIC FROM nyc_taxi_clean
# MAGIC GROUP BY passenger_count
# MAGIC ORDER BY passenger_count;
# MAGIC