from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

spark = (
    SparkSession.builder
        .appName("PaymentStreamingS8")
        .getOrCreate()
)

payment_schema = StructType([
    StructField("payment_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True)
])

payments = (
    spark.readStream
        .schema(payment_schema)
        .option("header", "true")
        .csv("hdfs:///data/stream/payment/s8/input")
)

successful = payments.filter(
    col("status") == "SUCCESS"
)

query = (
    successful.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option(
            "checkpointLocation",
            "hdfs:///data/stream/checkpoints/payment_s8"
        )
        .start()
)

query.awaitTermination()
