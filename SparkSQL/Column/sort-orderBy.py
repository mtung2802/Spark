
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import upper
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql.functions import length
from datetime import datetime
spark = SparkSession.builder.appName("SparkSQL").master("local[*]").config("spark.executor.memory", "4g").getOrCreate()

jsonSchema = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("actor", StructType([
        StructField("id", LongType(), True),
        StructField("login", StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("avatar_url", StringType(), True),
    ]), True),
    StructField("repo", StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("url", StringType(), True),
    ]), True),
    StructField("payload", StructType([
        StructField("action", StringType(), True),
        StructField("issue", StructType([
            StructField("url", StringType(), True),
            StructField("labels_url", StringType(), True),
            StructField("comments_url", StringType(), True),
            StructField("events_url", StringType(), True),
            StructField("html_url", StringType(), True),
            StructField("id", LongType(), True),
            StructField("number", IntegerType(), True),
            StructField("title", StringType(), True),
            StructField("user", StructType([
                StructField("login", StringType(), True),
                StructField("id", LongType(), True),
                StructField("avatar_url", StringType(), True),
                StructField("gravatar_id", StringType(), True),
                StructField("url", StringType(), True),
                StructField("html_url", StringType(), True),
                StructField("followers_url", StringType(), True),
                StructField("following_url", StringType(), True),
                StructField("gists_url", StringType(), True),
                StructField("starred_url", StringType(), True),
                StructField("subscriptions_url", StringType(), True),
                StructField("organizations_url", StringType(), True),
                StructField("repos_url", StringType(), True),
                StructField("events_url", StringType(), True),
                StructField("received_events_url", StringType(), True),
                StructField("type", StringType(), True),
                StructField("site_admin", BooleanType(), True),
            ]), True),
            StructField("labels", ArrayType(
                StructType([
                    StructField("url", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("color", StringType(), True),
                ])
            ), True),
            StructField("state", StringType(), True),
            StructField("locked", BooleanType(), True),
            StructField("assignee", StringType(), True),
            StructField("milestone", StringType(), True),
            StructField("comments", IntegerType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("closed_at", TimestampType(), True),
            StructField("body", StringType(), True),
        ]), True),
    ]), True),
    StructField("public", BooleanType(), True),
    StructField("created_at", TimestampType(), True),
])
dataFile = spark.read.schema(jsonSchema).json("D:/HocSpark/Data/2015-03-01-17.json")
# Sort
# dataFile.sort(col("id")).show()
# dataFile.select(col("id")).sort(col("id").desc()).show()

# order-by
# dataFile.select(col("id")).orderBy(col("id").desc()).show()
# (col("id"), col("actor.id") la tuple, còn ascending= [True, False] là list nên muốn chạy được thì phải cùng là tuple hoặc list
#dataFile.select(col("id"), col("actor.id"))c.show()
# dataFile.select(col("id"), col("actor.id")).orderBy([col("id"), col("actor.id")], ascending= [True, False]).show()