from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("FileDataSource").setMaster("local[*]").set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

data = sc.parallelize([("a", 1), ("a", 2), ("a", 3), ("b", 1), ("b", 2), ("c", 4)])
bill = data.reduceByKey(lambda x, y: x + y) # nhom value cung key va cong voi nhau
sortBill = bill.map(lambda x: (x[1], x[0])).sortByKey(ascending=False) # sắp xếp value theo thứ tự giam dan
print(sortBill.collect())