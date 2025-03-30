from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("FileDataSource").setMaster("local[*]").set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

fildeRdd = sc.textFile("../Data/DataDE.txt")
# flatmap đi đến từng phần tử trong mỗi hàng, còn map chỉ đi đến từng hàng
flatmapRdd = fildeRdd.flatMap(lambda line: line.split(" "))

for line in flatmapRdd.collect():
    print(line)
print(flatmapRdd.count())
