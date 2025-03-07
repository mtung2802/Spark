from pyspark import SparkContext, SparkConf
from pyspark.shell import spark

conf = SparkConf().setAppName("FileDataSource").setMaster("local[*]").set("spark.executor.memory", "2g")

sc = spark.SparkContext(conf=conf)

fildeRdd = sc.textFile("../Data/DataDE.txt")

allCapRdd = fildeRdd.map(lambda x: x.upper())

for line in allCapRdd.collect():
    print(line)