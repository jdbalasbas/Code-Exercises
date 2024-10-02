# Databricks notebook source
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

# UDF to check if the Delta path exists
def check_delta_path_exists(path):
    try:
        dbutils.fs.ls(path)  # Try listing the files at the given path
        return True  # Path exists if no exception is raised
    except Exception as e:
        # print(f"Path not found: {path}. Exception: {e}")
        return False  # Path does not exist

# UDF to validate wind direction (0-360 degrees)
def validate_wind_direction(wind_dir):
    if wind_dir is not None and 0 <= wind_dir <= 360:
        return True
    return False

# UDF to validate wind speed (non-negative)
def validate_wind_speed(wind_speed):
    if wind_speed is not None and wind_speed >= 0:
        return True
    return False

# UDF to validate cloud coverage (0-100%)
def validate_cloud_coverage(cloud):
    if cloud is not None and 0 <= cloud['cover'] <= 100 and cloud['baseHeight'] > 0:
        return True
    return False

# Registering UDFs
validate_wind_direction_udf = udf(validate_wind_direction, BooleanType())
validate_wind_speed_udf = udf(validate_wind_speed, BooleanType())
validate_cloud_coverage_udf = udf(validate_cloud_coverage, BooleanType())

