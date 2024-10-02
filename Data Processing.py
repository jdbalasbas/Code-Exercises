# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 1: Process Ingested Data

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, FloatType, TimestampType
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# Define schema
schema = StructType([
    StructField("stationId", StringType(), True),
    StructField("issueTime", StringType(), True),
    StructField("forecastValidFrom", StringType(), True),
    StructField("forecastValidTo", StringType(), True),
    StructField("windDirection", IntegerType(), True),
    StructField("windSpeed", FloatType(), True),
    StructField("cloudCoverage", StringType(), True),  # Cloud coverage as a JSON string
    StructField("type", StringType(), True)
])

# I used the sample data and created a tab separated TXT file, uploaded the file under /FileStore/shared_uploads/jdbalasbas@gmail.com
file_path = "/FileStore/shared_uploads/jdbalasbas@gmail.com/weather_data.txt"
df = spark.read.option("delimiter", "\t").option("header", "true").schema(schema).csv(file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Data Transformation

# COMMAND ----------

# Convert timestamp fields and parse JSON for cloud coverage data
df = df.withColumn("forecastValidFrom", col("forecastValidFrom").cast(TimestampType())) \
       .withColumn("forecastValidTo", col("forecastValidTo").cast(TimestampType())) \
       .withColumn("cloudCoverage", F.from_json(col("cloudCoverage"), ArrayType(StructType([
           StructField("cover", IntegerType(), True),
           StructField("baseHeight", IntegerType(), True),
           StructField("type", StringType(), True)
       ]))))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Data Validation and Enhancing Data Quality

# COMMAND ----------

# MAGIC %run "./Include/Validation"

# COMMAND ----------

# Apply the UDFs in data transformation pipeline for validation

# Add validation for wind direction
time_df = time_df.withColumn("is_valid_wind_direction", validate_wind_direction_udf(col("windDirection")))

# Add validation for wind speed
time_df = time_df.withColumn("is_valid_wind_speed", validate_wind_speed_udf(col("windSpeed")))

# Add validation for cloud coverage
time_df = time_df.withColumn("is_valid_cloud_coverage", validate_cloud_coverage_udf(col("cloud")))

# Filter out invalid rows based on the validation UDFs
time_df = time_df.filter("is_valid_wind_direction and is_valid_wind_speed and is_valid_cloud_coverage")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Data Aggregation

# COMMAND ----------

# Generate rows for every hour between forecastValidFrom and forecastValidTo
time_df = df.withColumn("hours", F.expr("sequence(forecastValidFrom, forecastValidTo, interval 1 hour)")) \
            .withColumn("hour", F.explode("hours")) \
            .drop("hours")

# Explode cloudCoverage so that each row corresponds to one cloud layer
time_df = time_df.withColumn("cloud", F.explode("cloudCoverage"))

# Repartition the DataFrame based on 'hour' to optimize groupBy operation
time_df = time_df.repartition("hour")

# Filter TEMPO types
time_df = time_df.filter(col("type") != "TEMPO")

# Calculate the required aggregates for each hour
result_df = time_df.groupBy("hour") \
    .agg(
        F.avg("windSpeed").alias("average_wind_speed"),  # Average wind speed per hour
        F.expr("mode(windDirection)").alias("most_common_wind_direction"),  # Most frequent wind direction per hour
        F.max("cloud.cover").alias("max_cloud_coverage"),  # Maximum cloud coverage per hour
        F.avg("cloud.baseHeight").alias("average_cloud_base_height")  # Average cloud base height per hour
    )

# COMMAND ----------

# For testing initial load
# spark.sql("drop table weather_forecast_summary")
# dbutils.fs.rm("/FileStore/tables/weather_forecast_summary", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Incremental Inserts into Delta Table (Upsert)

# COMMAND ----------

delta_path = "/FileStore/tables/weather_forecast_summary"

# Check if the Delta table or path exists
try:
    if check_delta_path_exists(delta_path) and DeltaTable.isDeltaTable(spark, delta_path):
        
        # Load the Delta table
        delta_table = DeltaTable.forPath(spark, delta_path)
    
        # Merge new data into the Delta table (Upsert)
        delta_table.alias("existing") \
            .merge(
                result_df.alias("new"), 
                "existing.hour = new.hour"  # Matching condition based on 'hour'hive_metastore.default.weather_forecast_summary
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

        print("Successfully merged data")
    else:
        print("Delta path does not exist. Writing data to delta path")

        # Save the DataFrame to the Delta path if it doesn't exist
        result_df.write.format("delta").mode("overwrite").save(delta_path)
        result_df.write.format("delta").mode("overwrite").saveAsTable("weather_forecast_summary")
        print("Delta table saved at path:", delta_path)

except Exception as e:
    print(f"An error occurred: {e}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6: Optimization of Summarized Table

# COMMAND ----------

# Perform Z-order optimization to enhance future queries against the table
try:
    spark.sql(f"OPTIMIZE weather_forecast_summary ZORDER BY (hour)")
except Exception as e:
    print(f"An error occurred: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7: Formatting and Displaying Results

# COMMAND ----------

# Format results
raw_df = spark.read.table("weather_forecast_summary")

# Round the floating point numbers to avoid precision issues
formatted_result_df = raw_df \
    .withColumn("Hour", date_format(col("hour"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("Average wind Speed (knots)", F.round(col("average_wind_speed"), 2)) \
    .withColumn("Most common wind direction (degree)", col("most_common_wind_direction")) \
    .withColumn("Maximum cloud coverage (%)", F.round(col("max_cloud_coverage"), 0)) \
    .withColumn("Average cloud base height (feet)", F.round(col("average_cloud_base_height"), 0))

# Rename columns for final display
formatted_result_df = formatted_result_df \
    .drop( \
        "average_wind_speed", \
        "most_common_wind_direction", \
        "max_cloud_coverage", \
        "average_cloud_base_height")

# Output results
formatted_result_df \
    .sort("Hour") \
    .display()

# COMMAND ----------


