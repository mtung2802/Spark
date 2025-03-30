from pydantic.v1.schema import schema
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf,col,split

spark = SparkSession.builder \
    .appName("Quynh dai gia") \
    .master("local[*]") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

data = [["11//02/2025"],
        ["27/11-2021"],
        ["28.12-2005"],
        ["14~9*2002"],
        ["-31:03{}1995"]]

df = (spark.createDataFrame(data, ["date"]))
df.show()

# Định nghĩa hàm làm sạch
def clean_date(date_str):
    if date_str.startswith("-"):
        date_str = date_str[1:]
    for char in ["~", "-", ":", "*", "{", "}", ".","//"]:
        date_str = date_str.replace(char, "/")
    return date_str


clean_date_udf = udf(clean_date, StringType())

df_clean = df.withColumn("clean_date", clean_date_udf(col("date")))

df_clean.show()

# Tách chuỗi thành các cột ngày, tháng, năm
df_final = df_clean.withColumn("date_parts", split(col("cleaned_date"), "/")) \
    .withColumn("day", col("date_parts")[0]) \
    .withColumn("month", col("date_parts")[1]) \
    .withColumn("year", col("date_parts")[2]) \
    .select("day", "month", "year")

df_final.show()
