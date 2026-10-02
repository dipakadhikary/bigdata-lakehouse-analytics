from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# --------------------------------------------------
# 1. SparkSession
# --------------------------------------------------

spark = (
    SparkSession.builder
        .appName("AirlineBroadcastJoin")
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
# 3. Read small reference dataset
# --------------------------------------------------

airports = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("hdfs:///data/airline/reference/airports.csv")
)

# --------------------------------------------------
# 4. Broadcast Join
# --------------------------------------------------

result = (
    flights.alias("f")
        .join(
            broadcast(airports).alias("a"),
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
# 6. Physical Plan
# --------------------------------------------------

print("========== BROADCAST JOIN PLAN ==========")

result.explain("formatted")

# --------------------------------------------------
# 7. Execute
# --------------------------------------------------

print("========== RESULT ==========")

result.show(20, truncate=False)

spark.stop()
