from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

spark = (
    SparkSession.builder
    .appName("KafkaJSONS12")
    .getOrCreate()
)

payment_schema = StructType([
    StructField("payment_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("status", StringType())
])

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "10.0.53.103:9092")
    .option("subscribe", "payment-events")
    .option("startingOffsets", "latest")
    .load()
)

json_df = df.select(
    from_json(
        col("value").cast("string"),
        payment_schema
    ).alias("payment")
)

payment_df = json_df.select(
    "payment.payment_id",
    "payment.customer_id",
    "payment.amount",
    "payment.currency",
    "payment.status"
)

query = (
    payment_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()
