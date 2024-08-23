import duckdb
from duckdb.experimental.spark.sql import SparkSession
from duckdb.experimental.spark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)
from duckdb.experimental.spark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

input_schema = StructType(
    [
        StructField("department", StringType(), True),
        StructField("employees", IntegerType(), True),
    ]
)

dept = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40)]
df = spark.createDataFrame(data=dept, schema=input_schema)

df.show()


df_flights = spark.read.parquet("data/flights.parquet")
df_flights.limit(10).show()
df_flights = (
    df_flights.filter("year=2013")
    .select("carrier", "dep_delay")
    .groupBy(F.col("carrier"))
    .agg(F.avg(F.col("dep_delay")).alias("avg_delay"))
)
df_flights.show()
