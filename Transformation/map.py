from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("FileDataSource").setMaster("local[*]").set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

fildeRdd = sc.textFile("../Data/DataDE.txt")
# map Không làm thay đổi số lượng phần tử của RDD, chỉ thay đổi giá trị của từng phần tử.
# map lặp qua lập lại trong mỗi hàng file text
allCapRdd = fildeRdd.map(lambda x: x.upper())

for line in allCapRdd.collect():
    print(line)