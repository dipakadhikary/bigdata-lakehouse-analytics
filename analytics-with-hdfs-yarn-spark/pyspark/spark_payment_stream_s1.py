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
        .appName("PaymentStreamingS1")
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
        .csv("hdfs:///data/stream/payment/input")
)

successful_payments = payments.filter(
    col("status") == "SUCCESS"
)

query = (
    successful_payments.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .start()
)

query.awaitTermination()
