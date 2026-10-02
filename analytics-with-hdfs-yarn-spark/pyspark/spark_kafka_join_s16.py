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
    .appName("KafkaJoinS16")
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
    StructField("status", StringType())
])

# --------------------------------------------------
# Streaming source: Kafka
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
# Parse JSON
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
# Static reference data
# --------------------------------------------------

customer_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/data/stream/reference/customers.csv")
)

# --------------------------------------------------
# Streaming + Static Join
# --------------------------------------------------

enriched_df = (
    payment_df
    .join(
        customer_df,
        payment_df.customer_id == customer_df.customer_id,
        "left"
    )
    .select(
        payment_df.payment_id,
        payment_df.customer_id,
        customer_df.customer_name,
        customer_df.segment,
        payment_df.amount,
        payment_df.currency,
        payment_df.status
    )
)

# --------------------------------------------------
# Output
# --------------------------------------------------

query = (
    enriched_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()
