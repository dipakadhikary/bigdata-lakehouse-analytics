from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count, sum
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

spark = (
    SparkSession.builder
    .appName("KafkaCustomerStateS14")
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

payment_df = (
    df.select(
        from_json(
            col("value").cast("string"),
            payment_schema
        ).alias("payment")
    )
    .select("payment.*")
)

successful_payments = payment_df.filter(
    col("status") == "SUCCESS"
)

customer_metrics = (
    successful_payments
    .groupBy("customer_id")
    .agg(
        count("*").alias("payment_count"),
        sum("amount").alias("total_amount")
    )
)

query = (
    customer_metrics.writeStream
    .format("console")
    .outputMode("complete")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()
