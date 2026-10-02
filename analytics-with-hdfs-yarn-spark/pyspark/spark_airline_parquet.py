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
        .appName("AirlineBatchParquet")
        .getOrCreate()
)

# --------------------------------------------------
# 2. Read raw airline data
# --------------------------------------------------

df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("hdfs:///data/airline/airline.csv")
)

print("========== SOURCE ==========")

df.show(5)

# --------------------------------------------------
# 3. Clean required columns
# --------------------------------------------------

clean_df = df.filter(
    df.DepDelay.isNotNull() &
    df.ArrDelay.isNotNull()
)

# --------------------------------------------------
# 4. Business aggregation
# --------------------------------------------------

result = (
    clean_df
        .groupBy("Airline")
        .agg(
            count("*").alias("total_flights"),
            avg("DepDelay").alias("avg_departure_delay"),
            avg("ArrDelay").alias("avg_arrival_delay"),
            max("ArrDelay").alias("max_arrival_delay"),
            min("ArrDelay").alias("min_arrival_delay")
        )
)

print("========== RESULT ==========")

result.show(truncate=False)

# --------------------------------------------------
# 5. Write Parquet
# --------------------------------------------------

output_path = "hdfs:///data/airline/output/airline_summary"

result.write \
    .mode("overwrite") \
    .parquet(output_path)

print("========== PARQUET WRITTEN ==========")

print(output_path)

parquet_df = spark.read.parquet(
    "hdfs:///data/airline/output/airline_summary"
)

print("========== READ PARQUET ==========")

parquet_df.show(truncate=False)

print("========== PARQUET SCHEMA ==========")

parquet_df.printSchema()
# --------------------------------------------------
# 6. Stop Spark
# --------------------------------------------------

spark.stop()
