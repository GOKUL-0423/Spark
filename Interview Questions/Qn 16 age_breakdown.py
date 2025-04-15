# Databricks notebook source
"""
💡𝐐𝐮𝐞𝐬𝐭𝐢𝐨𝐧:- 
/*
Write a query to obtain a breakdown of the time spent sending vs. opening snaps as a percentage of total time spent on these activities grouped by age group. Round the percentage to 2 decimal places in the output.

Notes:-
 Calculate the following percentages:
 time spent sending / (Time spent sending + Time spent opening)
 Time spent opening / (Time spent sending + Time spent opening)
 To avoid integer division in percentages, multiply by 100.0 and not 100.
*/

-- 𝐓𝐚𝐛𝐥𝐞 -𝟏 
CREATE TABLE age_breakdown (
 user_id INT PRIMARY KEY,
 age_bucket VARCHAR(10)
);

-- 𝐈𝐧𝐬𝐞𝐫𝐭 𝐭𝐡𝐞 𝐝𝐚𝐭𝐚 
INSERT INTO age_breakdown(user_id, age_bucket) VALUES
(123, '31-35'),
(456, '26-30'),
(789, '21-25');

-- 𝐓𝐚𝐛𝐥𝐞 - 𝟐
CREATE TABLE activities (
 activity_id INT PRIMARY KEY,
 user_id INT,
 activity_type VARCHAR(10),
 time_spent DECIMAL(5,2),
 activity_date DATETIME,
 FOREIGN KEY (user_id) REFERENCES age_breakdown(user_id) ON DELETE CASCADE
);

-- 𝐈𝐧𝐬𝐞𝐫𝐭 𝐭𝐡𝐞 𝐝𝐚𝐭𝐚 
INSERT INTO activities(activity_id, user_id, activity_type, time_spent, activity_date) VALUES
(7274, 123, 'open', 4.50, '2022-06-22 12:00:00'),
(2425, 123, 'send', 3.50, '2022-06-22 12:00:00'),
(1413, 456, 'send', 5.67, '2022-06-23 12:00:00'),
(2536, 456, 'open', 3.00, '2022-06-25 12:00:00'),
(8564, 456, 'send', 8.24, '2022-06-26 12:00:00'),
(5235, 789, 'send', 6.24, '2022-06-28 12:00:00'),
(4251, 123, 'open', 1.25, '2022-07-01 12:00:00'),
(1414, 789, 'chat', 11.00, '2022-06-25 12:00:00'),
(1314, 123, 'chat', 3.15, '2022-06-26 12:00:00'),
(1435, 789, 'open', 5.25, '2022-07-02 12:00:00');
"""
print()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE age_breakdown (
# MAGIC  user_id INT,
# MAGIC  age_bucket VARCHAR(10)
# MAGIC );
# MAGIC
# MAGIC -- 𝐈𝐧𝐬𝐞𝐫𝐭 𝐭𝐡𝐞 𝐝𝐚𝐭𝐚 
# MAGIC INSERT INTO age_breakdown(user_id, age_bucket) VALUES
# MAGIC (123, '31-35'),
# MAGIC (456, '26-30'),
# MAGIC (789, '21-25');
# MAGIC
# MAGIC -- 𝐓𝐚𝐛𝐥𝐞 - 𝟐
# MAGIC CREATE TABLE activities (
# MAGIC  activity_id INT ,
# MAGIC  user_id INT,
# MAGIC  activity_type VARCHAR(10),
# MAGIC  time_spent DECIMAL(5,2),
# MAGIC  activity_date DATE
# MAGIC );
# MAGIC
# MAGIC -- 𝐈𝐧𝐬𝐞𝐫𝐭 𝐭𝐡𝐞 𝐝𝐚𝐭𝐚 
# MAGIC INSERT INTO activities(activity_id, user_id, activity_type, time_spent, activity_date) VALUES
# MAGIC (7274, 123, 'open', 4.50, '2022-06-22 12:00:00'),
# MAGIC (2425, 123, 'send', 3.50, '2022-06-22 12:00:00'),
# MAGIC (1413, 456, 'send', 5.67, '2022-06-23 12:00:00'),
# MAGIC (2536, 456, 'open', 3.00, '2022-06-25 12:00:00'),
# MAGIC (8564, 456, 'send', 8.24, '2022-06-26 12:00:00'),
# MAGIC (5235, 789, 'send', 6.24, '2022-06-28 12:00:00'),
# MAGIC (4251, 123, 'open', 1.25, '2022-07-01 12:00:00'),
# MAGIC (1414, 789, 'chat', 11.00, '2022-06-25 12:00:00'),
# MAGIC (1314, 123, 'chat', 3.15, '2022-06-26 12:00:00'),
# MAGIC (1435, 789, 'open', 5.25, '2022-07-02 12:00:00');

# COMMAND ----------

# dbutils.widget.text("table","")

# COMMAND ----------

"""
Write a query to obtain a breakdown of the time spent sending vs. opening snaps as a percentage of total time spent on these activities grouped by age group. Round the percentage to 2 decimal places in the output.

Notes:-
 Calculate the following percentages:
 time spent sending / (Time spent sending + Time spent opening)
 Time spent opening / (Time spent sending + Time spent opening)
 To avoid integer division in percentages, multiply by 100.0 and not 100.
"""

# COMMAND ----------

import pyspark.sql.functions as F

df_age = spark.read.table('age_breakdown')
df_activities = spark.read.table('activities')

df_age.display()
df_activities.display


# COMMAND ----------

import pyspark.sql.functions as F

df_age = spark.read.table('age_breakdown')
df_activities = spark.read.table('activities')

df_age_time_spent = df_activities.alias('ac').filter("activity_type !='chat'")\
    .join(df_age.alias('age'),df_activities.user_id == df_age.user_id,'inner')\
    .selectExpr('age.age_bucket','ac.activity_type','ac.time_spent')\

df_age_time_spent\
    .groupBy('age_bucket')\
    .agg(\
        (F.sum(F.when(F.col('activity_type')=='open',F.col('time_spent')))/F.sum(F.col('time_spent'))*100).alias('open')\
       ,(F.sum(F.when(F.col('activity_type')=='send',F.col('time_spent')))/F.sum(F.col('time_spent'))*100).alias('send')\
        )\
        .select('age_bucket',F.round('open',2).alias('open_perc'),F.round('send',2).alias('send_perc'))\
        .show()

    