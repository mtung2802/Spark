from pyspark import SparkConf, SparkContext
# groupbykey chi nhom cac value co cung key chứ khó thực hiện tính toán giữa các value cùng key
conf = SparkConf().setAppName("FileDataSource").setMaster("local[*]").set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

rdd = sc.parallelize(["co gang len roi se on thoi nha"])
rdd2 = rdd.flatMap(lambda x: x.split(" "))
#in ra do dai tung phan tu trong list
#pairRDD = rdd2.map(lambda x: (x, len(x)))
#print(pairRDD.collect())
pairRDD = rdd2.map(lambda x: (len(x), x))
print(pairRDD.collect())
groupRDD = pairRDD.groupByKey()
for keys in groupRDD.collect():
    print(keys)

for key, value in groupRDD.collect():
    print(key, list(value))
