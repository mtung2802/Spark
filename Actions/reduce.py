from pyspark import SparkConf, SparkContext
# reduce để chia và trị
conf = SparkConf().setAppName("FileDataSource").setMaster("local[*]").set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

numbers = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2)
print(numbers.glom().collect())
# 1, 2, 3, 4, 5
# 6, 7, 8, 9, 10
def sum(v1: int, v2: int) -> int: # vao int ra int
    print(f"v1: {v1}, v2: {v2} => ({v1} + {v2})")
    return v1 + v2
print(numbers.reduce(sum))