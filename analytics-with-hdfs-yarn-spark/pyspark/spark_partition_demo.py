from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
        .appName("PartitionDemo")
        .getOrCreate()
)

# --------------------------------------------------
# 1. Read airline data
# --------------------------------------------------

df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("hdfs:///data/airline/airline.csv")
)

print("========== ORIGINAL ==========")

print("Partitions:", df.rdd.getNumPartitions())

# --------------------------------------------------
# 2. Repartition
# --------------------------------------------------

repartitioned_df = df.repartition(10)

print("========== REPARTITION ==========")

print(
    "Partitions:",
    repartitioned_df.rdd.getNumPartitions()
)

# --------------------------------------------------
# 3. Coalesce
# --------------------------------------------------

coalesced_df = df.coalesce(10)

print("========== COALESCE ==========")

print(
    "Partitions:",
    coalesced_df.rdd.getNumPartitions()
)

# --------------------------------------------------
# 4. Explain plans
# --------------------------------------------------

print("========== REPARTITION PLAN ==========")

repartitioned_df.explain("formatted")

print("========== COALESCE PLAN ==========")

coalesced_df.explain("formatted")

spark.stop()
