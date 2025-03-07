from pyspark import SparkContext, SparkConf
from pyspark.shell import spark

conf = SparkConf().setAppName("FileDataSource").setMaster("local[*]").set("spark.executor.memory", "2g")

sc = spark.SparkContext(conf=conf)

fildeRdd = sc.textFile("../Data/DataDE.txt")
flatmapRdd = fildeRdd.flatMap(lambda line: line.split(" "))

for line in flatmapRdd.collect():
    print(line)
print(flatmapRdd.count())
