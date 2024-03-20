#!/usr/bin/env python3
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MySparkApp").getOrCreate()

data = spark.read.csv("student_scores.csv", header=True, inferSchema=True)

result = data.groupBy("Name").agg({"Score": "sum"})

result.show()


spark.stop()
