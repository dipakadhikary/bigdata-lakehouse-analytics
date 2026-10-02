from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    window,
    count,
    sum
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType
)

spark = (
    SparkSession.builder
        .appName("PaymentStreamingS10")
        .getOrCreate()
)

payment_schema = StructType([
    StructField("payment_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("event_time", TimestampType(), True)
])

payments = (
    spark.readStream
        .schema(payment_schema)
        .option("header", "true")
        .csv("hdfs:///data/stream/payment/s10/input")
)

successful = payments.filter(
    col("status") == "SUCCESS"
)

windowed = (
    successful
        .withWatermark(
            "event_time",
            "10 minutes"
        )
        .groupBy(
            window(
                col("event_time"),
                "5 minutes"
            )
        )
        .agg(
            count("*").alias("payment_count"),
            sum("amount").alias("total_amount")
        )
)

query = (
    windowed.writeStream
        .format("console")
        .outputMode("update")
        .option("truncate", "false")
        .option(
            "checkpointLocation",
            "hdfs:///data/stream/checkpoints/payment_s10"
        )
        .start()
)

query.awaitTermination()
