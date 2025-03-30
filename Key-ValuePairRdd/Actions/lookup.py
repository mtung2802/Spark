from pyspark import SparkConf, SparkContext
# tra cuu gia tri
conf = SparkConf().setAppName("FileDataSource").setMaster("local[*]").set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

data = sc.parallelize([("a", 1), ("a", 2), ("a", 3), ("b", 1), ("b", 2), ("c", 4)]
print(data.lookup("a"))