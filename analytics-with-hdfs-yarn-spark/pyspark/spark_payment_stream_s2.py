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
        .appName("PaymentStreamingS2")
        .getOrCreate()
)

payment_schema = StructType([
    StructField("payment_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True)
])

# Streaming DataFrame
payments = (
    spark.readStream
        .schema(payment_schema)
        .option("header", "true")
        .csv("hdfs:///data/stream/payment/input")
)

# Transformation 1
successful_payments = payments.filter(
    col("status") == "SUCCESS"
)

# Transformation 2
high_value_payments = successful_payments.filter(
    col("amount") > 1000
)

# Start streaming query
query = (
    high_value_payments.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .start()
)

query.awaitTermination()
