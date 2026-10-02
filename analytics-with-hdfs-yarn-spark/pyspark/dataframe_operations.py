from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("DataFrameOperations")
    .getOrCreate()
)

data = [
    (1, "Amit", "IT", 25, 60000),
    (2, "Rahul", "HR", 30, 50000),
    (3, "Priya", "IT", 28, 75000),
    (4, "Sneha", "Finance", 35, 80000),
    (5, "Raj", "IT", 32, 70000),
    (6, "Anita", "HR", 27, 55000)
]

columns = ["id", "name", "department", "age", "salary"]

df = spark.createDataFrame(data, columns)

print("Original DataFrame")
df.show()

print("Select name and salary")
df.select("name", "salary").show()

print("Employees with salary greater than 60000")
df.filter(col("salary") > 60000).show()

print("Employees from IT department")
df.filter(col("department") == "IT").show()

print("Add annual bonus column")
df_with_bonus = df.withColumn(
    "bonus",
    col("salary") * 0.10
)

df_with_bonus.show()

print("Rename salary column")
df_renamed = df.withColumnRenamed(
    "salary",
    "monthly_salary"
)

df_renamed.show()

print("Drop age column")
df_without_age = df.drop("age")

df_without_age.show()

print("Multiple transformations")

result = (
    df
    .filter(col("salary") > 60000)
    .select("name", "department", "salary")
    .withColumn("bonus", col("salary") * 0.10)
)

result.show()

spark.stop()
