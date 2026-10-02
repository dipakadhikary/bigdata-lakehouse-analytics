from pyspark.sql import SparkSession

# Initialize SparkSession
spark = (
    SparkSession.builder
    .appName("PySpark Session")
    .enableHiveSupport()
    .getOrCreate()
)

# Create Hive table
spark.sql("""
    CREATE TABLE t1 (
        id INT,
        name STRING
    )
""")

# Insert data
spark.sql("""
    INSERT INTO TABLE t1
    VALUES
        (1, 'nameOne'),
        (2, 'name Two ')
""")

# Read data
spark.sql("""
    SELECT *
    FROM t1
""").show()

spark.stop()
