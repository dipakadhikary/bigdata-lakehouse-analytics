# Python setup virtual environment with specific version
python3.13 -m venv venv
.\venv\Scripts\activate

# Feed a text file directly into the script
python script.py < data.txt

# Pipe the output of another command into the script
cat data.txt | python script.py

# Transfer file from windows into AWS
scp -i /d/paynow/aws-keys/MerchantBoardingapplication.pem -r /d/projects/bigdata-analytics/hadoop-streaming-word-count/ ec2-user@13.202.134.109:/home/ec2-user/

# Login to the container:
docker exec -it analytics-with-hadoop-hive "/bin/bash"

# Start Beeline and execute a simple query to verify the setup
/hive/bin/beeline -u 'jdbc:hive2://localhost:10001/'

# Create table
create table t1(id int);
insert into t1 values(1);
select * from t1;

# DML
CREATE TABLE employees ( emp_id INT, emp_name STRING, emp_salary DOUBLE );
INSERT INTO TABLE employees VALUES (1, 'Alice', 65000.0), (2, 'Bob', 75000.0), (3, 'Charlie', 60000.0);
SELECT emp_name, emp_salary FROM employees;
UPDATE employees SET emp_salary = 70000.0 WHERE emp_id = 3;
DELETE FROM employees WHERE emp_id = 2;
SELECT emp_name, emp_salary FROM employees ORDER BY emp_salary DESC;
SELECT AVG(emp_salary) AS avg_salary, MAX(emp_salary) AS max_salary FROM employees;

CREATE TABLE departments ( dept_id INT, dept_name STRING );
INSERT INTO TABLE departments VALUES (1, 'HR'), (2, 'IT');
SELECT e.emp_name, d.dept_name FROM employees e JOIN departments d ON e.emp_id = d.dept_id;

SELECT emp_name FROM employees WHERE emp_salary > (SELECT AVG(emp_salary) FROM employees);

CREATE VIEW high_earners AS SELECT emp_name, emp_salary FROM employees WHERE emp_salary > 60000.0;

# Inserting Data with SELECT:
INSERT INTO TABLE employees SELECT 4, 'David', 72000.0;

# Merging Data (Upsert):
MERGE INTO employees AS target USING temp_employees AS source
ON target.emp_id = source.emp_id WHEN MATCHED THEN UPDATE SET
target.emp_salary = source.emp_salary WHEN NOT MATCHED THEN
INSERT VALUES (source.emp_id, source.emp_name,
source.emp_salary);

ALTER TABLE employees RENAME TO staff;

ALTER TABLE employees ADD COLUMNS ( emp_department STRING );

DROP TABLE IF EXISTS employees;

TRUNCATE TABLE staff;

# ACID table
CREATE TABLE transactional_managed_table ( id INT, name STRING, age INT) CLUSTERED BY (id) INTO 4 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true');

Insert into transactional_managed_table values (1, 'name', 10), (2, 'two', 20);
Insert into transactional_managed_table values (1, 'name', 10), (2, 'two', 20);
Insert into transactional_managed_table values (1, 'name', 10), (2, 'two', 20);

select * from transactional_managed_table;transactional_managed_table;

describe formatted transactional_managed_table;

# Check files
/hadoop/bin/hdfs dfs -ls  hdfs://0.0.0.0:9000/user/hive/warehouse/transactional_managed_table

update transactional_managed_table set age=50 where id=2;
# Check hdfs and observe the new delete and delta files:
/hadoop/bin/hdfs dfs -ls  hdfs://0.0.0.0:9000/user/hive/warehouse/transactional_managed_table
# Compaction - Let us use major compaction to compact these small files:
alter table transactional_managed_table compact 'MAJOR';

