from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from math import radians, cos, sin, asin, sqrt
from builtins import round,abs
from pyspark.sql.types import DoubleType

METERS_PER_FOOT = 0.3048
FEET_PER_MILE = 5280
EARTH_RADIUS_IN_METERS = 6371e3
METERS_PER_MILE = METERS_PER_FOOT * FEET_PER_MILE

def calculate_distance(start_latitude,start_longitude,end_latitude,end_longitude):
    start_latitude, start_longitude, end_latitude, end_longitude = map(radians,[start_latitude,start_longitude,end_latitude,end_longitude])
    diff_latitude = end_latitude - start_latitude
    diff_longitude = end_longitude - start_longitude

    # Calculate area
    area = sin(diff_latitude / 2) ** 2 + cos(start_latitude) * cos(end_latitude) * sin(diff_longitude / 2) ** 2

    # Calculate the central angle
    central_angle = 2 * asin(sqrt(area))

    # Calculate Distance
    distance = (central_angle * EARTH_RADIUS_IN_METERS) / METERS_PER_MILE
    return abs(round(distance, 2))

udf_get_distance = udf(calculate_distance)

def compute_distance(_spark: SparkSession, dataframe: DataFrame) -> DataFrame:
    output_df = dataframe.select('*').withColumn(
        'distance',
        udf_get_distance(col("start_station_latitude"),col("start_station_longitude"),col("end_station_latitude"),col("end_station_longitude")).cast(DoubleType())
    )
    return output_df


def run(spark: SparkSession, input_dataset_path: str, transformed_dataset_path: str) -> None:
    input_dataset = spark.read.parquet(input_dataset_path)
    input_dataset.show()

    dataset_with_distances = compute_distance(spark, input_dataset)
    dataset_with_distances.show()

    dataset_with_distances.write.parquet(transformed_dataset_path, mode='append')
