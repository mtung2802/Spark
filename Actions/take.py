
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("FileDataSource").setMaster("local[*]").set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

numbers = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2)
# 1, 2, 3, 4, 5
# 6, 7, 8, 9, 10
print(numbers.take(7))