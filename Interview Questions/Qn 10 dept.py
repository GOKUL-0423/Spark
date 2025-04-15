# Databricks notebook source
"""
Q.Write a SQL query to fetch the details of employee whose salary is greater than the average salary deptwise.

Table Script:
create table dept (dept_id number,deptname varchar(10),empname varchar(10),salary number);
insert into dept values(1,'HR','A',100);
insert into dept values(1,'HR','B',200);
insert into dept values(1,'HR','C',300);
insert into dept values(2,'SALES','D',400);
insert into dept values(2,'SALES','E',500);
insert into dept values(2,'SALES','F',600);
insert into dept values(3,'TECH','G',700);
insert into dept values(3,'TECH','H',800);
insert into dept values(3,'TECH','I',900);
"""
print()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table dept (dept_id int,deptname varchar(10),empname varchar(10),salary int);
# MAGIC insert into dept values(1,'HR','A',100);
# MAGIC insert into dept values(1,'HR','B',200);
# MAGIC insert into dept values(1,'HR','C',300);
# MAGIC insert into dept values(2,'SALES','D',400);
# MAGIC insert into dept values(2,'SALES','E',500);
# MAGIC insert into dept values(2,'SALES','F',600);
# MAGIC insert into dept values(3,'TECH','G',700);
# MAGIC insert into dept values(3,'TECH','H',800);
# MAGIC insert into dept values(3,'TECH','I',900);

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.window as W

df = spark.read.table('dept')
windowSpec = W.Window().partitionBy('deptname')

df_trans = df.withColumn('avg_salary',F.avg('salary').over(windowSpec))\
    .filter('salary > avg_salary')\
    .select(df.columns)
df_trans.display()



# COMMAND ----------

df_group = df.groupBy('deptname').agg(F.avg('salary').alias('avg_sal'))
df_group.display()

df.join(df_group,df.deptname==df_group.deptname,'inner')\
    .filter(df.salary > df_group.avg_sal)\
    .select([f"df.{col}" for col in df.columns])\
        .display()

# COMMAND ----------

df.columns