from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("FileDataSource").setMaster("local[*]").set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

# lấy những thành phần giống nhau trong 2 hoặc nhiều rdd
# ví dụ [1,2,3,4] [2.3.5.6] =>> 2,3
rdd1 = sc.parallelize([1,2,3,4, 5])
rdd2 = sc.parallelize([2,3,5,6])

rdd3 = rdd1.intersection(rdd2)
print(rdd3.collect())