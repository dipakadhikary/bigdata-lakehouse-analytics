from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    avg,
    max,
    min
)

# --------------------------------------------------
# 1. Create SparkSession
# --------------------------------------------------

spark = (
    SparkSession.builder
        .appName("AirlinePartitionedParquet")
        .getOrCreate()
)

# --------------------------------------------------
# 2. Read raw data
# --------------------------------------------------

df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("hdfs:///data/airline/airline.csv")
)

# --------------------------------------------------
# 3. Clean data
# --------------------------------------------------

clean_df = df.filter(
    df.DepDelay.isNotNull() &
    df.ArrDelay.isNotNull()
)

# --------------------------------------------------
# 4. Write partitioned Parquet
# --------------------------------------------------

output_path = (
    "hdfs:///data/airline/output/"
    "airline_partitioned"
)

(
    clean_df.write
        .mode("overwrite")
        .partitionBy("Airline")
        .parquet(output_path)
)

print("Partitioned Parquet written to:")
print(output_path)

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
        .appName("ReadPartitionedAirline")
        .getOrCreate()
)

df = spark.read.parquet(
    "hdfs:///data/airline/output/airline_partitioned"
)

df.printSchema()

df.show(10)

# --------------------------------------------------
# 5. Stop Spark
# --------------------------------------------------

spark.stop()
