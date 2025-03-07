from random import Random

from pyspark import SparkContext, SparkConf
from pyspark.shell import spark

conf = SparkConf().setAppName("FileDataSource").setMaster("local[*]").set("spark.executor.memory", "2g")

sc = spark.SparkContext(conf=conf)

data = ["Dat", "goden", "heu", "Sami"]
rdd = sc.parallelize(data)
def numsPartition():
    # create 1 nums for map Partition data
    rand = Random(int(time.time()*1000) + Random().randint(0, 1000))
    return [f"{item}:{rand.randint(0, 1000)}" for item in iterator]

result = rdd.mapPartitions(numsPartition)
print(result.collect())

result = rdd.mapPartitions(
    lambda item: map(
        lambda l: f"{l}:{Random(int(time.time()*1000) + Random().randint(0, 1000)).randint(0, 1000)}", item")
)
print(result.collect())