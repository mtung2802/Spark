from pyspark import SparkConf, SparkContext
# nhom value cung key va cong voi nhau
conf = SparkConf().setAppName("FileDataSource").setMaster("local[*]").set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

data = sc.parallelize([("a", 1), ("a", 2), ("a", 3), ("b", 1), ("b", 2)])
bill = data.reduceByKey(lambda x, y: x + y) # nhom value cung key va cong voi nhau
print(bill.collect())