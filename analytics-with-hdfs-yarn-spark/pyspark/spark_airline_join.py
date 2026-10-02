from pyspark.sql import SparkSession

# --------------------------------------------------
# 1. Create SparkSession
# --------------------------------------------------

spark = (
    SparkSession.builder
        .appName("AirlineBatchJoin")
        .getOrCreate()
)

# --------------------------------------------------
# 2. Read flights
# --------------------------------------------------

flights = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("hdfs:///data/airline/airline.csv")
)

# --------------------------------------------------
# 3. Read airport reference data
# --------------------------------------------------

airports = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("hdfs:///data/airline/reference/airports.csv")
)

print("========== FLIGHTS ==========")

flights.show(5)

print("========== AIRPORTS ==========")

airports.show()

# --------------------------------------------------
# 4. Join
# --------------------------------------------------

result = (
    flights.alias("f")
        .join(
            airports.alias("a"),
            flights.Origin == airports.AirportCode,
            "inner"
        )
)

# --------------------------------------------------
# 5. Select required columns
# --------------------------------------------------

result = result.select(
    "f.Origin",
    "a.AirportName",
    "a.City",
    "f.ArrDelay"
)

# --------------------------------------------------
# 6. Physical plan
# --------------------------------------------------

print("========== PHYSICAL PLAN ==========")

result.explain("formatted")

# --------------------------------------------------
# 7. Execute
# --------------------------------------------------

print("========== RESULT ==========")

result.show(20, truncate=False)

# --------------------------------------------------
# 8. Stop Spark
# --------------------------------------------------

spark.stop()
