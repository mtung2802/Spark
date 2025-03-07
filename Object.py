# 3 cách làm việc với rdd đầu tiên là object
# import libary
from pyspark import SparkContext

#create sparkContext: 1
sc = SparkContext("local","Simple App") #chạy ở local tên Simple App

# create object
data = [
    {"id": 1, "name": "Dat"},
    {"id": 2, "name": "Tung"},
    {"id": 3, "name": "Tuan"}
]
print(data)

# create rdd from date
rdd = sc.parallelize(data)
print(rdd.collect()) # print list format
print(f"number of partition: {rdd.count()}")
print(f"frist valude of data: {rdd.first()}")
