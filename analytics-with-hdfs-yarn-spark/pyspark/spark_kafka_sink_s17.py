from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    struct,
    to_json
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

spark = (
    SparkSession.builder
    .appName("KafkaSinkS17")
    .getOrCreate()
)

payment_schema = StructType([
    StructField("payment_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("status", StringType())
])

# Kafka source
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

# Parse JSON
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

# Process only successful payments
successful_payments = payment_df.filter(
    col("status") == "SUCCESS"
)

# Convert structured data back to JSON
output_df = successful_payments.select(
    to_json(
        struct(
            col("payment_id"),
            col("customer_id"),
            col("amount"),
            col("currency"),
            col("status")
        )
    ).alias("value")
)

# Kafka sink
query = (
    output_df.writeStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "10.0.53.103:9092"
    )
    .option(
        "topic",
        "processed-payments"
    )
    .option(
        "checkpointLocation",
        "hdfs:///data/stream/checkpoints/kafka_sink_s17"
    )
    .outputMode("append")
    .start()
)

query.awaitTermination()
