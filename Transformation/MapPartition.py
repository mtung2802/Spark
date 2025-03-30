from random import Random
import time  # Đảm bảo rằng module time đã được import
from pyspark import SparkConf
from pyspark.shell import spark

# Cấu hình Spark
conf = SparkConf().setAppName("FileDataSource").setMaster("local[*]").set("spark.executor.memory", "2g")
sc = spark.SparkContext(conf=conf)

# Dữ liệu mẫu
data = ["Dat", "goden", "heu", "Sami"]

# Tạo một RDD với 2 phân vùng
rdd = sc.parallelize(data, 2)

def numsPartition(iterator):
    # Tạo số ngẫu nhiên cho mỗi phần tử trong phân vùng
    rand = Random(int(time.time() * 1000) + Random().randint(0, 1000))
    return [f"{item}:{rand.randint(0, 1000)}" for item in iterator]

# Áp dụng mapPartitions để xử lý mỗi phân vùng với hàm numsPartition
result = rdd.mapPartitions(numsPartition)

# In kết quả sau khi áp dụng mapPartitions
print(result.collect())

# Một cách khác sử dụng lambda với mapPartitions
'''
result = rdd.mapPartitions(
    lambda iterator: map(
        lambda l: f"{l}:{Random(int(time.time() * 1000) + Random().randint(0, 1000)).randint(0, 1000)}", iterator)
)
'''
# In kết quả sau khi áp dụng mapPartitions
print(result.collect())
