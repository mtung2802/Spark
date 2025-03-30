
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import upper, lit, struct, udf
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
"""
withCoulumn
colName: name of column
column: value of column
- them 1 cột mới nếu column name không tồn tại
- ghi đè lên cột hiện tái nếu column ( tồn tại )
- col(), lit(): để xác định giá trị của cột
"""

# dataFile.withColumn("id2", lit("tundepzai")).select(col("actor.id"), col("id2")).show()

# Cách thêm một trường vào toàn bộ cột actor khi bi rang buoc boi schema ( khong nen tao 1 schema moi )
# dataFile.withColumn("actor.id2", lit("tundepzai")).select(col("actor.id"), col("actor.id2")).show()

dataFileStruct = dataFile.withColumn(
    "actor",
    struct(
        col("actor.id").alias("id"),
        col("actor.login").alias("lo+gin"),
        col("actor.gravatar_id").alias("gravatar_id"),
        col("actor.url").alias("url"),
        col("actor.avatar_url").alias("avatar_url"),
        lit("tundepzai").alias("id2")

    )
)
# dataFileStruct.select(col("actor.id"), col("actor.id2")) \
#     .orderBy([col("id"), col("actor.id")], ascending= [True, False])
#bay gio quay lai xu ly bai toan sort id
# ham udf la ham do nguoi dung dinh nghia
def tungdepzai(id):
    count = 2

    return count + int(id)

tungdepzaiUDF = udf(tungdepzai, IntegerType())

df = dataFile.withColumn("tung", tungdepzaiUDF(col("id"))).select(col("id"), col("tung"))
df.show()
df.orderBy([col("id"), col("actor.id")], ascending= [True, False]).show()






