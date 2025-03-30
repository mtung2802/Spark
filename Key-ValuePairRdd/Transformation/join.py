from pyspark import SparkConf, SparkContext
#  nhóm cột trung của hai bảng với nhau và các bản ghi
conf = SparkConf().setAppName("FileDataSource").setMaster("local[*]").set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

data1 = sc.parallelize([(110, 1), (165, 2), (300, 3), (300, 1), (200, 2)])

data2 = sc.parallelize([(110, "dat"), (165, "goleden"), (300, "heu kkk"), (300, "phuc"), (200, "quanh")])

dataNew = data1.join(data2).sortByKey()
#print(dataNew.collect())
for result in dataNew.collect():
    print(result)

print(dataNew.lookup(110))