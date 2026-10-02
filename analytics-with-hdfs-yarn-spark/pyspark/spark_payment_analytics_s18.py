from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    window,
    count,
    sum,
    to_json,
    struct
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
    .appName("PaymentAnalyticsS18")
    .getOrCreate()
)

# --------------------------------------------------
# Payment schema
# --------------------------------------------------

payment_schema = StructType([
    StructField("payment_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("status", StringType()),
    StructField("event_time", TimestampType())
])

# --------------------------------------------------
# Kafka source
# --------------------------------------------------

kafka_df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "10.0.53.103:9092"
    )
    .option("subscribe", "payment-events")
    .option("startingOffsets", "latest")
    .load()
)

# --------------------------------------------------
# JSON parsing
# --------------------------------------------------

payment_df = (
    kafka_df
    .select(
        from_json(
            col("value").cast("string"),
            payment_schema
        ).alias("payment")
    )
    .select("payment.*")
)

# --------------------------------------------------
# Successful payments
# --------------------------------------------------

successful_payments = payment_df.filter(
    col("status") == "SUCCESS"
)

# --------------------------------------------------
# Event-time window aggregation
# --------------------------------------------------

windowed_metrics = (
    successful_payments
    .groupBy(
        window(
            col("event_time"),
            "1 minute"
        )
    )
    .agg(
        count("*").alias("payment_count"),
        sum("amount").alias("total_amount")
    )
)

# --------------------------------------------------
# Output to console
# --------------------------------------------------

query = (
    windowed_metrics
    .writeStream
    .format("console")
    .outputMode("complete")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()
