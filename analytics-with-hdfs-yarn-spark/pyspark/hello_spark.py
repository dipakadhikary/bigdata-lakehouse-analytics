from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("HelloSpark")
    .getOrCreate()
)

print("Spark Version:", spark.version)

data = [
    (1, "Amit", 25),
    (2, "Rahul", 30),
    (3, "Priya", 28),
    (4, "Sneha", 35)
]

columns = ["id", "name", "age"]

df = spark.createDataFrame(data, columns)

df.show()

df.printSchema()

spark.stop()
