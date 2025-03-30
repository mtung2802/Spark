from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import upper
from pyspark.sql.functions import col
from pyspark.sql.types import *
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


jsonFile = spark.read.json("D:/HocSpark/Data/2015-03-01-17.json")
#jsonFile.select(col("id")).show()
#jsonFile.select(col("id"), col("id") > 2615567690).show()

# select with alias = SQL (as)
#jsonFile.select(col("id").alias("user_id"), col("type").alias("user_type"), col("actor.id")).show()

#select with expression

# jsonFile.select(
#     col("id") * 2,
#     upper(col("type").alias("upper_type"))
# ).show()

# jsonFile.select("*").show() # hiển thị đầy đủ các cột

# jsonFile.select(jsonFile.columns).show(truncate=False) # hiển thị đầy đủ các cột và nội dung của từng cột

# jsonFile.select(
#     col("id"),
#     (col("id") < "261556768").alias("id_smaller_than_2615567688")
# ).show()

jsonFile.select(
    col("id"),
    (col("actor.id") - (col("actor.id") % 2)).alias("actor_id_new")
).show()

'''
jsonFile.select(
    "id",
    "type",
    "actor.id",
    "actor.gravatar_id",
    "actor.url",
    "actor.avatar_url"
).show(truncate=False)
'''


