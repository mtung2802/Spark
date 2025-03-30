from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("FileDataSource").setMaster("local[*]").set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)
rdd1 = sc.parallelize([1,2,3,4,5])
rdd2 = sc.parallelize([6,7,8,9,10])

# hàm để hợp nhất hai rdd
rrd3 = rdd1.union(rdd2)
print(rrd3.collect())
