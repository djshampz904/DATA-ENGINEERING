from pyspark.sql import SparkSession
import time


# create spark session
spark = SparkSession.builder \
    .appName("Spark Web server") \
    .config("spark.ui.port", "4042") \
    .getOrCreate()

print(spark.sparkContext.uiWebUrl)

while True:
    time.sleep(10)