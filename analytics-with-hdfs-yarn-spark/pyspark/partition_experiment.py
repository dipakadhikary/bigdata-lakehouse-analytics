from pyspark.sql import SparkSession

spark = (
        SparkSession.builder.appName("airline").getOrCreate()
     )

input_file = "hdfs:///data/airline/airline.csv"

df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_file)
        )

df.printSchema()

print("Number of partitions", df.rdd.getNumPartitions())
df.show(5, truncate=False)
count = df.count()
print("Total records:::::::::::::::::::::", count)
spark.stop()
