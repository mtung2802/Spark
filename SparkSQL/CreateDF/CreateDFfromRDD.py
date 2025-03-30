# Làm viêc với dữ liệu có cau trúc, còn Rdd là làm việc dữ liệu phi cấu trúc
# lấy ý tưởng từ python pandas
import random
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SparkSQL").master("local[*]").config("spark.executor.memory", "2g").getOrCreate()

rdd = spark.sparkContext.parallelize(range(1, 11))\
    .map(lambda x: (x, random.randint(0, 99) * x))
# Để tạo một DataFrame cần một data: rdd, iterable, datafram, ndarray và 1 schema
# schema là tên định dạng các cột, ở đây là number và random
"""
key     value
1       random
2       random
3       random
4       random
4       random
5       random
"""
df = spark.createDataFrame(rdd, ["number", "random"]).show()
