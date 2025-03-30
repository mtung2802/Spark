from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("FileDataSource").setMaster("local[*]").set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

rdd = sc.parallelize(["co gang len roi se on thoi nha"])
rdd2 = rdd.flatMap(lambda x: x.split(" "))
#in ra do dai tung phan tu trong list
#pairRDD = rdd2.map(lambda x: (x, len(x)))
#print(pairRDD.collect())
pairRDD = rdd2.map(lambda x: (len(x), x))
for pard in pairRDD.collect():
    print(pard)