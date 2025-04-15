# Databricks notebook source
"""
Given a cinema table with seat_id and free (1 = available, 0 = occupied), find all free seats that have at least one neighboring free seat.

CREATE TABLE cinema (
 seat_id INT PRIMARY KEY,
 free int
);
INSERT INTO cinema (seat_id, free) VALUES 
(1, 1), (2, 0), (3, 1), (4, 1), (5, 1), 
(6, 0), (7, 1), (8, 1), (9, 0), (10, 1), 
(11, 0), (12, 1), (13, 0), (14, 1), (15, 1), 
(16, 0), (17, 1), (18, 1), (19, 1), (20, 1);
"""
print()

# COMMAND ----------

dbutils.fs.rm('dbfs:/user/hive/warehouse/',True)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE cinema (
# MAGIC  seat_id INT,
# MAGIC  free int
# MAGIC );
# MAGIC INSERT INTO cinema (seat_id, free) VALUES 
# MAGIC (1, 1), (2, 0), (3, 1), (4, 1), (5, 1), 
# MAGIC (6, 0), (7, 1), (8, 1), (9, 0), (10, 1), 
# MAGIC (11, 0), (12, 1), (13, 0), (14, 1), (15, 1), 
# MAGIC (16, 0), (17, 1), (18, 1), (19, 1), (20, 1);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cinema;

# COMMAND ----------

"""
Given a cinema table with seat_id and free (1 = available, 0 = occupied), find all free seats that have at least one neighboring free seat.
"""
import pyspark.sql.functions as F
import pyspark.sql.window as W

df = spark.read.table('cinema')
df_cte = df.withColumn('prev_seat_sts',F.lag('free').over(W.Window.orderBy('seat_id')))\
    .withColumn('next_seat_sts',F.lead('free').over(W.Window.orderBy('seat_id')))\
    .filter((F.col('free') == 1) & ((F.col('prev_seat_sts')==1) | (F.col('next_seat_sts')==1)))\
    .select('seat_id')
df_cte.display()

    