from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
        .appName("airline-groupby")
        .getOrCreate()
)

input_file = "hdfs:///data/airline/airline.csv"

df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_file)
)

print("Input partitions:", df.rdd.getNumPartitions())

df.printSchema()

# Deliberately introduce a shuffle
result = (
    df.groupBy("Airline")
      .count()
)

print("===== EXECUTION PLAN =====")
result.explain("formatted")

print("===== RESULT =====")
result.show(truncate=False)

spark.stop()
