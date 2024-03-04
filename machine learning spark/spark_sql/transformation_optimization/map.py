#!/usr/bin/env python3
from pyspark import SparkContext

sc = SparkContext("local", "MapExample")
data = [1, 2, 3, 4, 5]

rdd = sc.parallelize(data)

mapped_rdd = rdd.map(lambda x : x * 2)
mapped_rdd.collect()

