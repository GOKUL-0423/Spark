# Databricks notebook source
"""
Q. You have an emp table with id, name, dept and salary. You need to write a query to:
 ✅ Find the highest salary from the HR department.
 ✅ Find the second highest salary from the SALES department.
 ✅ Find the third highest salary from the TECH department.

Table Script:
create table emp(id INT,name varchar(10),dept varchar(10),salary INT);
insert into emp values(1,'A','HR',100);
insert into emp values(2,'B','HR',600);
insert into emp values(5,'H','TECH',300);
insert into emp values(6,'E','TECH',200);
insert into emp values(7,'F','TECH',500);
insert into emp values(11,'J','SALES',700);
insert into emp values(12,'K','SALES',900);
insert into emp values(15,'R','SALES',1900);
insert into emp values(13,'L','HR',900);
insert into emp values(14,'M','HR',900);
insert into emp values(15,'N','HR',600);
"""
print()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table emp(id INT,name varchar(10),dept varchar(10),salary INT);
# MAGIC insert into emp values(1,'A','HR',100);
# MAGIC insert into emp values(2,'B','HR',600);
# MAGIC insert into emp values(5,'H','TECH',300);
# MAGIC insert into emp values(6,'E','TECH',200);
# MAGIC insert into emp values(7,'F','TECH',500);
# MAGIC insert into emp values(11,'J','SALES',700);
# MAGIC insert into emp values(12,'K','SALES',900);
# MAGIC insert into emp values(15,'R','SALES',1900);
# MAGIC insert into emp values(13,'L','HR',900);
# MAGIC insert into emp values(14,'M','HR',900);
# MAGIC insert into emp values(15,'N','HR',600);

# COMMAND ----------

"""
Q. You have an emp table with id, name, dept and salary. You need to write a query to:
 ✅ Find the highest salary from the HR department.
 ✅ Find the second highest salary from the SALES department.
 ✅ Find the third highest salary from the TECH department.
 """
import pyspark.sql.functions as F
import pyspark.sql.window as W

#creating datafram on emp table
df = spark.read.table('emp')
windowSpec = W.Window().partitionBy('dept').orderBy(F.desc('salary'))

df_hr = df.withColumn('dense_rank',F.dense_rank().over(windowSpec))
df_hr.filter("\
    (dept=='HR' and dense_rank==1) \
    or (dept='SALES' and dense_rank==2) \
    or (dept='TECH' and dense_rank==3)")\
    .select(df.columns)\
    .display()

# COMMAND ----------

# MAGIC %sql
# MAGIC with emp_rank as 
# MAGIC   (select *, dense_rank() over(partition by dept order by salary desc) as drnk from emp)
# MAGIC select id,name,dept,salary from emp_rank
# MAGIC where
# MAGIC   (dept=='HR' and drnk==1)
# MAGIC   or (dept='SALES' and drnk==2)
# MAGIC   or (dept == 'TECH' and drnk==3)