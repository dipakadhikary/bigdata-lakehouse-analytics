from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    row_number,
    rank,
    dense_rank
)

# --------------------------------------------------
# 1. Create SparkSession
# --------------------------------------------------

spark = (
    SparkSession.builder
        .appName("AirlineBatchWindow")
        .getOrCreate()
)

# --------------------------------------------------
# 2. Read airline data
# --------------------------------------------------

df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("hdfs:///data/airline/airline.csv")
)

# --------------------------------------------------
# 3. Define Window
# --------------------------------------------------

window_spec = (
    Window
        .partitionBy("Airline")
        .orderBy(df.ArrDelay.desc())
)

# --------------------------------------------------
# 4. Apply window functions
# --------------------------------------------------

result = (
    df
    .withColumn(
        "row_number",
        row_number().over(window_spec)
    )
    .withColumn(
        "rank",
        rank().over(window_spec)
    )
    .withColumn(
        "dense_rank",
        dense_rank().over(window_spec)
    )
)

# --------------------------------------------------
# 5. Select useful columns
# --------------------------------------------------

result = result.select(
    "Airline",
    "FlightNum",
    "ArrDelay",
    "row_number",
    "rank",
    "dense_rank"
)

# --------------------------------------------------
# 6. Physical plan
# --------------------------------------------------

print("========== PHYSICAL PLAN ==========")

result.explain("formatted")

# --------------------------------------------------
# 7. Show result
# --------------------------------------------------

print("========== RESULT ==========")

result.show(30, truncate=False)

# --------------------------------------------------
# 8. Stop Spark
# --------------------------------------------------

spark.stop()
