
from pyspark import SparkContext, SparkConf
from pyspark.shell import spark

conf = SparkConf().setAppName("FileDataSource").setMaster("local[*]").set("spark.executor.memory", "2g")

sc = spark.SparkContext(conf=conf)

number = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
rdd = sc.parallelize(number)
print(rdd.getNumPartitions())

# using transformation for create rdd
square = rdd.map(lambda x: x * x)
filtered = square.filter(lambda x: x > 4)
flat = filtered.flatMap(lambda x: [x, x * 2])
print(flat.collect())