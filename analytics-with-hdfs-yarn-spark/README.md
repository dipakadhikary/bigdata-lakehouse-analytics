# Python setup virtual environment with specific version
python3.13 -m venv venv
.\venv\Scripts\activate

# Feed a text file directly into the script
python script.py < data.txt

# Pipe the output of another command into the script
cat data.txt | python script.py

# Transfer file from windows into AWS
scp -i /d/paynow/aws-keys/MerchantBoardingapplication.pem -r /d/projects/bigdata-analytics/hadoop-streaming-word-count/ ec2-user@13.202.134.109:/home/ec2-user/

docker build -t analytics-with-hadoop-spark ./

docker run --rm -t --name analytics-with-hadoop-spark --hostname localhost -P -p7070:9870 -p7171:8088 -p7373:3306 -p8082:4040 -it analytics-with-hadoop-spark /bin/bash -c "/bootstrap.sh >/tmp/boostrap.log"

# Start Spark-shell and Verify the Setup
./spark/bin/spark-shell

# Login to the container:
docker exec -it analytics-with-hadoop-spark "/bin/bash"

# Interactive command - Scala in spark-shell
val df = spark.read.json("/spark/examples/src/main/resources/people.json")
val df1 = df.unionAll(df)
df1.show()

# another 
spark-shell
val data = Seq(("Alice", 25), ("Bob", 30))
val df = spark.createDataFrame(data).toDF("Name", "Age")
df.show()
:quit

# Scala application
# To run this application, you would typically compile the code, package it into a JAR
# file, and submit it to a Spark cluster using the spark-submit command.
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
object SparkAppExample {
def main(args: Array[String]): Unit = {
  // Initialize Spark session
    val spark = SparkSession.builder.appName(«SparkAppExample»).getOrCreate()
// Define schema
    val schema = StructType(Seq(
        StructField(«Name», StringType, true),
        StructField(«Age», IntegerType, true)
    ))
// Create DataFrame
    val data = Seq(Row(«Alice», 25), Row("Bob", 30))
    val df =
        spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    // Perform operations on the DataFrame or execute Spark jobs
    // Stop Spark session
    spark.stop()
    }
}

# Exploratory Data Analysis
val df = spark.read.option("header", true).csv("/GHCND_sample_csv.csv")
df.printSchema
df.count()
df.show(5)
df.where("TMAX<-100 and TMIN>-222").show

# When spark not resolving host name:
spark-submit \
    --conf spark.hadoop.fs.defaultFS=hdfs://localhost:9000 \
    /tmp/spark_csv_hdfs.py
