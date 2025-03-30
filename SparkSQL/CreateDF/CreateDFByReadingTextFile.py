from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from datetime import datetime
spark = SparkSession.builder.appName("SparkSQL").master("local[*]").config("spark.executor.memory", "2g").getOrCreate()

textFile = spark.read.text("C:\\Users\\Admin\\PycharmProjects\\DE-ETL-Spark\\Data\\DataDE.txt")
textFile.show(truncate=False)