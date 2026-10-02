from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    count,
    avg,
    max,
    row_number
)

# ==================================================
# 1. Spark Session
# ==================================================

spark = (
    SparkSession.builder
        .appName("AirlineCompleteBatchPipeline")
        .getOrCreate()
)

print("========================================")
print("STARTING AIRLINE BATCH PIPELINE")
print("========================================")


# ==================================================
# 2. Read Flight Data
# ==================================================

flights = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(
            "hdfs:///data/airline/airline.csv"
        )
)

print("\n========== FLIGHT DATA ==========")

flights.show(5)

print("Flight partitions:",
      flights.rdd.getNumPartitions())


# ==================================================
# 3. Data Cleaning
# ==================================================

clean_flights = (
    flights
        .filter(
            col("DepDelay").isNotNull()
            &
            col("ArrDelay").isNotNull()
        )
        .dropDuplicates()
)

print("\n========== CLEAN DATA ==========")

print(
    "Original records:",
    flights.count()
)

print(
    "Clean records:",
    clean_flights.count()
)


# ==================================================
# 4. Read Airport Reference Data
# ==================================================

airports = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(
            "hdfs:///data/airline/reference/airports.csv"
        )
)

print("\n========== AIRPORT DATA ==========")

airports.show()


# ==================================================
# 5. Broadcast Join
# ==================================================

enriched = (
    clean_flights.alias("f")
        .join(
            airports.alias("a"),
            col("f.Origin") ==
            col("a.AirportCode"),
            "left"
        )
)

print("\n========== JOINED DATA ==========")

enriched.select(
    col("f.Airline"),
    col("f.FlightNum"),
    col("f.Origin"),
    col("a.City"),
    col("f.ArrDelay")
).show(10, truncate=False)


# ==================================================
# 6. Airline Aggregation
# ==================================================

airline_summary = (
    enriched
        .groupBy("Airline")
        .agg(
            count("*").alias(
                "total_flights"
            ),
            avg("DepDelay").alias(
                "avg_departure_delay"
            ),
            avg("ArrDelay").alias(
                "avg_arrival_delay"
            ),
            max("ArrDelay").alias(
                "max_arrival_delay"
            )
        )
)

print("\n========== AIRLINE SUMMARY ==========")

airline_summary.show(
    truncate=False
)


# ==================================================
# 7. Window Function
# ==================================================

window_spec = (
    Window
        .partitionBy("Airline")
        .orderBy(
            col("ArrDelay").desc()
        )
)

ranked_flights = (
    enriched
        .withColumn(
            "delay_rank",
            row_number().over(
                window_spec
            )
        )
)

print("\n========== TOP DELAYED FLIGHTS ==========")

ranked_flights.select(
    "Airline",
    "FlightNum",
    "Origin",
    "City",
    "ArrDelay",
    "delay_rank"
).filter(
    col("delay_rank") <= 3
).show(
    100,
    truncate=False
)


# ==================================================
# 8. Create Final Dataset
# ==================================================

final_result = (
    ranked_flights.select(
        "Airline",
        "FlightNum",
        "Origin",
        "City",
        "ArrDelay",
        "DepDelay",
        "delay_rank"
    )
)

print("\n========== FINAL DATASET ==========")

final_result.show(
    20,
    truncate=False
)


# ==================================================
# 9. Physical Plan
# ==================================================

print("\n========== PHYSICAL PLAN ==========")

final_result.explain(
    "formatted"
)


# ==================================================
# 10. Repartition Before Output
# ==================================================

output_df = final_result.repartition(
    "Airline"
)

print(
    "\nOutput partitions:",
    output_df.rdd.getNumPartitions()
)


# ==================================================
# 11. Write Partitioned Parquet
# ==================================================

output_path = (
    "hdfs:///data/airline/output/"
    "final_airline_batch"
)

(
    output_df.write
        .mode("overwrite")
        .partitionBy("Airline")
        .parquet(output_path)
)

print("\n========================================")
print("BATCH PIPELINE COMPLETED")
print("Output:")
print(output_path)
print("========================================")


# ==================================================
# 12. Stop Spark
# ==================================================

spark.stop()
