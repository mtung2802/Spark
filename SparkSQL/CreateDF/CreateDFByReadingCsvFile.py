from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from datetime import datetime
spark = SparkSession.builder.appName("SparkSQL").master("local[*]").config("spark.executor.memory", "4g").getOrCreate()

csvFile = spark.read.option("header", "value").option("inferSchema", "true").csv("C:\\Users\\Admin\\PycharmProjects\\DE-ETL-Spark\\Data\\DataDE.csv")
csvFile.show(truncate=False)