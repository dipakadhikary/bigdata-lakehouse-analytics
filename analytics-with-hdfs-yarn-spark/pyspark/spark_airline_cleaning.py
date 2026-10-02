from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    when,
    trim
)

spark = (
    SparkSession.builder
        .appName("AirlineBatchCleaning")
        .getOrCreate()
)

# --------------------------------------------------
# 1. Read source
# --------------------------------------------------

df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("hdfs:///data/airline/airline.csv")
)

print("========== ORIGINAL DATA ==========")

df.show(10, truncate=False)

print("Original count:", df.count())

# --------------------------------------------------
# 2. NULL analysis
# --------------------------------------------------

print("========== NULL COUNTS ==========")

null_counts = df.select(
    [
        count(
            when(col(c).isNull(), c)
        ).alias(c)
        for c in df.columns
    ]
)

null_counts.show(truncate=False)

# --------------------------------------------------
# 3. Trim string column
# --------------------------------------------------

clean_df = df.withColumn(
    "Airline",
    trim(col("Airline"))
)

# --------------------------------------------------
# 4. Remove records missing delay information
# --------------------------------------------------

clean_df = clean_df.filter(
    col("DepDelay").isNotNull() &
    col("ArrDelay").isNotNull()
)

# --------------------------------------------------
# 5. Remove duplicates
# --------------------------------------------------

clean_df = clean_df.dropDuplicates()

print("Clean count:", clean_df.count())

# --------------------------------------------------
# 6. Verify
# --------------------------------------------------

print("========== CLEAN DATA ==========")

clean_df.show(10, truncate=False)

print("========== CLEAN SCHEMA ==========")

clean_df.printSchema()

spark.stop()
