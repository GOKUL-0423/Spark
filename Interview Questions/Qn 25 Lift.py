# Databricks notebook source
"""
https://www.linkedin.com/posts/sneha-majumder_fitabrpassengers-withoutabroverloading-theabrchallange-activity-7309777082815852544-VLwM?utm_source=share&utm_medium=member_desktop&rcm=ACoAADC8k3MBr4BkJYHxs3S3v6NEzSWeAYOuvqc
Multiple passengers attempt to enter a lift, but the total weight cannot exceed the lift’s capacity. The challenge?
✅ Identify which passengers can fit in each lift without exceeding capacity
✅ Arrange them in increasing order of weight
✅ Generate a comma-separated list of accommodated passengers

📌 Given Tables
We have two tables:
1️⃣ LIFT – Stores the ID and maximum capacity of each lift.
2️⃣ LIFT_PASSENGERS – Stores passenger names, their weight, and the lift they are assigned to.

CREATE TABLE LIFT (
 ID INT PRIMARY KEY,
 CAPACITY_KG INT
);
INSERT INTO LIFT (ID, CAPACITY_KG) VALUES
(1, 300),
(2, 350);

CREATE TABLE LIFT_PASSENGERS (
 PASSENGER_NAME VARCHAR(50),
 WEIGHT_KG INT,
 LIFT_ID INT,
 FOREIGN KEY (LIFT_ID) REFERENCES LIFT(ID)
);
INSERT INTO LIFT_PASSENGERS (PASSENGER_NAME, WEIGHT_KG, LIFT_ID) VALUES
('Rahul', 85, 1),
('Adarsh', 73, 1),
('Riti', 95, 1),
('Dheeraj', 80, 1),
('Vimal', 83, 2),
('Neha', 77, 2),
('Priti', 73, 2),
('Himanshi', 85, 2);
"""

print()

# COMMAND ----------

try:
    dbutils.fs.rm('dbfs:/user/hive/warehouse/',True)
    # dbutils.fs.rm(,True)
except:
    pass

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE LIFT (
# MAGIC  ID INT,
# MAGIC  CAPACITY_KG INT
# MAGIC );
# MAGIC INSERT INTO LIFT (ID, CAPACITY_KG) VALUES
# MAGIC (1, 300),
# MAGIC (2, 350);
# MAGIC
# MAGIC CREATE TABLE LIFT_PASSENGERS (
# MAGIC  PASSENGER_NAME VARCHAR(50),
# MAGIC  WEIGHT_KG INT,
# MAGIC  LIFT_ID INT
# MAGIC );
# MAGIC INSERT INTO LIFT_PASSENGERS (PASSENGER_NAME, WEIGHT_KG, LIFT_ID) VALUES
# MAGIC ('Rahul', 85, 1),
# MAGIC ('Adarsh', 73, 1),
# MAGIC ('Riti', 95, 1),
# MAGIC ('Dheeraj', 80, 1),
# MAGIC ('Vimal', 83, 2),
# MAGIC ('Neha', 77, 2),
# MAGIC ('Priti', 73, 2),
# MAGIC ('Himanshi', 85, 2);

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.window as W

df_lift = spark.read.table('lift')
df_passengers = spark.read.table('LIFT_PASSENGERS')
# df_lift.display()
# df_passengers.display()

windowSpec = W.Window().partitionBy('LIFT_ID').orderBy('WEIGHT_KG')

df_lift.join(df_passengers,df_lift.ID==df_passengers.LIFT_ID,'inner')\
    .select(df_passengers.LIFT_ID,df_lift.CAPACITY_KG,df_passengers.PASSENGER_NAME,df_passengers.WEIGHT_KG)\
    .withColumn('sum_weight_kg',F.sum('WEIGHT_KG').over(windowSpec))\
    .filter('sum_weight_kg < capacity_kg')\
    .groupBy('LIFT_ID').agg(F.collect_list('passenger_name').alias('passengers_list'))\
    .display()

# COMMAND ----------



# COMMAND ----------

"""
Multiple passengers attempt to enter a lift, but the total weight cannot exceed the lift’s capacity. The challenge?
✅ Identify which passengers can fit in each lift without exceeding capacity
✅ Arrange them in increasing order of weight
✅ Generate a comma-separated list of accommodated passengers
"""

# COMMAND ----------

windowSpec = W.Window().partitionBy('LIFT_ID').orderBy('WEIGHT_KG')

df_lift.join(df_passengers,df_lift.ID==df_passengers.LIFT_ID,'inner')\
    .select(df_passengers.LIFT_ID,df_lift.CAPACITY_KG,df_passengers.PASSENGER_NAME,df_passengers.WEIGHT_KG)\
    .withColumn('sum_weight_kg',F.sum('WEIGHT_KG').over(windowSpec))\
    .filter('sum_weight_kg < capacity_kg')\
    .groupBy('LIFT_ID').agg(F.collect_list('passenger_name').alias('passengers_list'))\
    .display()