from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("FileDataSource").setMaster("local[*]").set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

text = sc.parallelize(["cO gAng roi Se thaNh conG nhA nhE"]) \
    .flatMap(lambda x: x.split(" ")) \
    .map(lambda x: x.lower())
print(text.collect())

removeText = sc.parallelize(["nha nhe"]) \
    .flatMap(lambda x: x.split(" "))
print(removeText.collect())
niceText = text.subtract(removeText).collect()
print(niceText)



