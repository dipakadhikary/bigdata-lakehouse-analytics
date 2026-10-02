from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType
)
from pyspark.sql.functions import col


spark = (
    SparkSession.builder
    .appName("SparkCSVHDFS")
    .getOrCreate()
)


# --------------------------------------------------
# 1. Define Schema
# --------------------------------------------------

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", IntegerType(), True)
])


# --------------------------------------------------
# 2. Read CSV from HDFS
# --------------------------------------------------

input_path = "hdfs:///data/input/employees.csv"

df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .schema(schema)
    .load(input_path)
)


# --------------------------------------------------
# 3. Display Data
# --------------------------------------------------

print("Original DataFrame")

df.show()


# --------------------------------------------------
# 4. Display Schema
# --------------------------------------------------

print("Schema")

df.printSchema()


# --------------------------------------------------
# 5. Filter IT Employees
# --------------------------------------------------

it_employees = df.filter(
    col("department") == "IT"
)

print("IT Employees")

it_employees.show()


# --------------------------------------------------
# 6. Employees with Salary > 65000
# --------------------------------------------------

high_salary = df.filter(
    col("salary") > 65000
)

print("Employees with salary > 65000")

high_salary.show()


# --------------------------------------------------
# 7. Select Required Columns
# --------------------------------------------------

result = (
    df
    .filter(col("salary") > 65000)
    .select(
        "name",
        "department",
        "salary"
    )
)

print("Final Result")

result.show()


# --------------------------------------------------
# 8. Write Result to HDFS
# --------------------------------------------------

output_path = "hdfs:///data/output/high_salary"

result.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)


print("Data written to:", output_path)


spark.stop()
