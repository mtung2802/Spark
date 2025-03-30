from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from datetime import datetime
spark = SparkSession.builder.appName("SparkSQL").master("local[*]").config("spark.executor.memory", "2g").getOrCreate()

"""
data = spark.sparkContext.parallelize([
    Row(1, "dat", 18),
    Row(2, "tung", 19),
    Row(3, "tuan", 20),
    Row(None, None, None),
    Row(4," ",16)
])
# schema là tên định dạng các cột
schema = StructType([
    StructField("id", LongType(), True), # True là cho phép name và LongType có giá trị Null
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])
df = spark.createDataFrame(data, schema).show()
"""
"""
PYSPARK SQL TYPE
StringType: chuoi ky tu 32
IntegerType: so nguyen 32 bit
LongType: so nguyen 64 bit
FloatType: so thap phan  32 bit
DoubleTypeL: so thap phan 64 bit
DecimalType(precision: tong chu so lam tron, scale: so chu so sau dau phay): do chinh xac cua so thap phan
BooleanType: True or False
DateType: nam/thang/ngay
TimestampTypes: ngay va gio
ByteType: so nguyen 8 bit
ShortType: so nguyen 16 bit
-------- Nang Cao -----------
StructType: Bieu dien mot cau truc
StructField: (name: tên, dataType: kiểu dữ liệu cửa trường đó, nullable: true or false): bieu dien mot trường trong StructType
ArrayType: (elementType: kieu du lieu cua cac phan tu trong mang): bieu dien cac mang duoc chi dinh
MapType(keyType, valueType): bieu dien cap khoa key - value

"""
"""
data1 = [
    Row(
        name="Quang Anh Tran",
        age=15,
        id=1,
        salary=10000.0,
        bonus=5000.0,
        is_active=True,
        scores=[1, 8, 9],  # ArrayType
        attributes={"dept": "Engineering", "role": "Data Engineer"},  # MapType
        hire_date=datetime.strptime("2020-01-01", "%Y-%m-%d").date(),
        last_login=datetime.strptime("2020-01-01 12:00:00", "%Y-%m-%d %H:%M:%S")  # Use datetime (not date)
    ),
    Row(
        name="Le Boang Hoang",
        age=22,
        id=2,
        salary=20000.0,
        bonus=1000.0,
        is_active=False,
        scores=[8, 9],  # ArrayType
        attributes={"dept": "Engineering", "role": "Data Engineer"},  # MapType
        hire_date=datetime.strptime("2020-01-01", "%Y-%m-%d").date(),
        last_login=datetime.strptime("2020-01-01 12:00:00", "%Y-%m-%d %H:%M:%S")  # Use datetime (not date)
    )
]

# Define schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("id", LongType(), False),
    StructField("salary", DoubleType(), True),
    StructField("bonus", DoubleType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("scores", ArrayType(IntegerType()), True),
    StructField("attributes", MapType(StringType(), StringType()), True),
    StructField("hire_date", DateType(), True),
    StructField("last_login", TimestampType(), True)  # last_login requires datetime
])

# Create DataFrame
df1 = spark.createDataFrame(data1, schema)
df1.show(truncate=False) # show het tat ca cac dong du lieu
df1.printSchema()
"""

df = spark.range(1, 10).toDF("number").show() # spark implict: chay ngam