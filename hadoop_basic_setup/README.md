# Python setup virtual environment with specific version
python3.13 -m venv venv
.\venv\Scripts\activate

# Feed a text file directly into the script
python script.py < data.txt

# Pipe the output of another command into the script
cat data.txt | python script.py

# Transfer file from windows into AWS
scp -i /d/paynow/aws-keys/MerchantBoardingapplication.pem -r /d/projects/bigdata-analytics/hadoop-streaming-word-count/ ec2-user@13.202.134.109:/home/ec2-user/

$HADOOP_HOME/bin/hdfs -mkdir /input/
$HADOOP_HOME/bin/hdfs dfs -copyFromLocal /root/hadoop-streaming-word-count/data/story.txt /input/

# Run this MR job
$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.5.0.jar \
-files mapper.py,reducer.py \
-mapper "python3 mapper.py" \
-reducer "python3 reducer.py" \
-input /input/story.txt -output /output2

# List Job
$HADOOP_HOME/bin/yarn application -list