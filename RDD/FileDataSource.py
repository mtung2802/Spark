# cách 2 làm việc với rdd là DataSource

from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("FileDataSource").setMaster("local[*]").set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)
fildeRdd = sc.textFile("../Data/DataDE.txt")
print(fildeRdd.collect())
print(f"number of partition: {fildeRdd.count()}")
print(f"frist valude of data: {fildeRdd.first()}")
