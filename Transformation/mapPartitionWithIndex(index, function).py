# index là id của số phân vùng
# [65, 123 , 2] index 0, 1, 2
# function là một logic, lặp qua các phần tử trong index

from random import Random
import time  # Đảm bảo rằng module time đã được import
from pyspark import SparkConf
from pyspark.shell import spark
conf = SparkConf().setAppName("FileDataSource").setMaster("local[*]").set("spark.executor.memory", "2g")
sc = spark.SparkContext(conf=conf)

numbersRdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2)
# 1, 2 , 3, 4, 5
# 6, 7, 8, 9, 10
# idex: id ( ex: 0, 1, 2,...)
#  iterator: vong lap qua tat ca phan tu trong phan vung
# lambda: idex chi so cua phan vung, itr vong lap qua tat ca phan tu trong phan vung
# =>> (1,0),(2,0),(3,0),(4,0),(5,0),(6,1),(7,1),(8,1),(9,1),(10,1) (idex, n)
# n là mỗi phần tử trong phân vùng thì sẽ tạo r( n ,idx) ghép nối lại với nhau
# =>> mapPartitionWithIndex để tạo ra các logic khác nhau trong mỗi phân vùng
result = (numbersRdd.mapPartitionsWithIndex(
        lambda idex, itr: [(n, idex) for n in itr]
    )
.collect())
print(result)
print(numbersRdd.glom().collect())

# bài tập về nhà phân vùng 1 cộng mỗi phần tử với một đơn vị, phân vùng 2 nhân với 2