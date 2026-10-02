from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("KafkaS11")
    .getOrCreate()
)

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "10.0.53.103:9092")
    .option("subscribe", "payment-events")
    .option("startingOffsets", "earliest")
    .load()
)

result = df.selectExpr(
    "CAST(value AS STRING) AS message"
)

query = (
    result.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()
