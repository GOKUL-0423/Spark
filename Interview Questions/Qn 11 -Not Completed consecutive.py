# Databricks notebook source
"""
A user login table records the dates when each user logs into the system. Identify users who logged in for 5 or more consecutive days.
Return the following details:
✅ User ID
✅ Start Date (First date of the streak)
✅ End Date (Last date of the streak)
✅ Number of Consecutive Days

💡 Important_Consideration:

--A user can log in multiple times on the same day, but only distinct consecutive days count toward the streak.
--Only users with login streaks of 5 days or more should be considered.

CREATE TABLE user_logins (
 user_id INT,
 login_date DATE
);
INSERT INTO user_logins (user_id, login_date) VALUES
(1, '2024-03-01'),(1, '2024-03-02'),(1, '2024-03-03'),
(1, '2024-03-04'),(1, '2024-03-06'),(1, '2024-03-10'),
(1, '2024-03-11'),(1, '2024-03-12'),(1, '2024-03-13'),
(1, '2024-03-14'),(1, '2024-03-20'),(1, '2024-03-25'),
(1, '2024-03-26'),(1, '2024-03-27'),(1, '2024-03-28'),
(1, '2024-03-29'),(1, '2024-03-30'),(2, '2024-03-01'),
(2, '2024-03-02'),(2, '2024-03-03'),(2, '2024-03-04'),
(3, '2024-03-01'),(3, '2024-03-02'),(3, '2024-03-03'),
(3, '2024-03-04'),(3, '2024-03-04'),(3, '2024-03-05'),
(4, '2024-03-03'),(4, '2024-03-04'),(4, '2024-03-04'),
(4, '2024-03-05'),(4, '2024-03-06');

"""
print()

# COMMAND ----------

try:
    dbutils.fs.rm('dbfs:/user/hive/warehouse/',True)
except Exception as e:
    print('Failed')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE user_logins (
# MAGIC  user_id INT,
# MAGIC  login_date DATE
# MAGIC );
# MAGIC INSERT INTO user_logins (user_id, login_date) VALUES
# MAGIC (1, '2024-03-01'),(1, '2024-03-02'),(1, '2024-03-03'),
# MAGIC (1, '2024-03-04'),(1, '2024-03-06'),(1, '2024-03-10'),
# MAGIC (1, '2024-03-11'),(1, '2024-03-12'),(1, '2024-03-13'),
# MAGIC (1, '2024-03-14'),(1, '2024-03-20'),(1, '2024-03-25'),
# MAGIC (1, '2024-03-26'),(1, '2024-03-27'),(1, '2024-03-28'),
# MAGIC (1, '2024-03-29'),(1, '2024-03-30'),(2, '2024-03-01'),
# MAGIC (2, '2024-03-02'),(2, '2024-03-03'),(2, '2024-03-04'),
# MAGIC (3, '2024-03-01'),(3, '2024-03-02'),(3, '2024-03-03'),
# MAGIC (3, '2024-03-04'),(3, '2024-03-04'),(3, '2024-03-05'),
# MAGIC (4, '2024-03-03'),(4, '2024-03-04'),(4, '2024-03-04'),
# MAGIC (4, '2024-03-05'),(4, '2024-03-06');

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.window as W

df = spark.read.table('user_logins')
df.display()

# COMMAND ----------

windowSpec = W.Window().partitionBy('user_id').orderBy(F.asc('login_date'))

df.withColumn('win',F.dense_rank().over(windowSpec))\
     .withColumn('prev_date',F.(F.lag('login_date').over(windowSpec),F.add_date('login_date',1)))\
    .withColumn('date_diff',F.datediff('login_date','prev_date')).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (
# MAGIC select user_id,login_date, 
# MAGIC       row_number() over(partition by user_id order by login_date) rnum,
# MAGIC       -- dense_rank() over(partition by user_id order by login_date) drank,
# MAGIC       NVL(lag(login_date) over(partition by user_id order by login_date),login_date) prev_login
# MAGIC        from user_logins
# MAGIC ),
# MAGIC  2_cte as(
# MAGIC select *,CASE WHEN date_diff(login_date,prev_login) = 0 THEN 1 ELSE date_diff(login_date,prev_login) END dt_diff  from cte)
# MAGIC
# MAGIC select *, dense_rank() over(partition by user_id,dt_diff order by login_date) drank from 2_cte
# MAGIC ;