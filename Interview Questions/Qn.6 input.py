# Databricks notebook source
"""
Q. Write a SQL query to retrieve the names of students who have scored marks less than three other students from the following dataset?

Table Script:
create table input(name varchar(50),marks int);
insert into input values('John',60);
insert into input values('Tom',60);
insert into input values('Michael',70);
insert into input values('Lincoln',50);
insert into input values('Sara',40);
"""

# COMMAND ----------

# dbutils.fs.rm('dbfs:/hive/')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table input(name varchar(50),marks int);
# MAGIC insert into input values('John',60);
# MAGIC insert into input values('Tom',60);
# MAGIC insert into input values('Michael',70);
# MAGIC insert into input values('Lincoln',50);
# MAGIC insert into input values('Sara',40);

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.window as W
df = spark.read.table('input')

windowSpec = W.Window().orderBy('marks')

df_rank = df.withColumn('rank',F.dense_rank().over(windowSpec))

df_rank.filter('rank>3').select('name','marks').display()





# COMMAND ----------

# MAGIC %sql
# MAGIC with rank_table
# MAGIC as (select *, dense_rank() over(order by marks) as rnk from input)
# MAGIC select name,marks from rank_table
# MAGIC where rnk > 3;
# MAGIC