# Databricks notebook source
"""
Q. Write a SQL query to display employees with even-numbered empids first, followed by employees with odd-numbered empids. Within each group, empids should be in ascending order.

Table Script:
create table employee(empid int,empname varchar(50),location varchar(50));
INSERT INTO EMPLOYEE VALUES(1,'John','Delhi');
INSERT INTO EMPLOYEE VALUES(2,'Tom','Chennai');
INSERT INTO EMPLOYEE VALUES(3,'Sara','Pune');
INSERT INTO EMPLOYEE VALUES(4,'Abdul','Delhi');
INSERT INTO EMPLOYEE VALUES(5,'Rajesh','Delhi');

"""
print()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table employee(empid int,empname varchar(50),location varchar(50));
# MAGIC INSERT INTO EMPLOYEE VALUES(1,'John','Delhi');
# MAGIC INSERT INTO EMPLOYEE VALUES(2,'Tom','Chennai');
# MAGIC INSERT INTO EMPLOYEE VALUES(3,'Sara','Pune');
# MAGIC INSERT INTO EMPLOYEE VALUES(4,'Abdul','Delhi');
# MAGIC INSERT INTO EMPLOYEE VALUES(5,'Rajesh','Delhi');

# COMMAND ----------

"""
Q. Write a SQL query to display employees with even-numbered empids first, followed by employees with odd-numbered empids. Within each group, empids should be in ascending order.
"""
import pyspark.sql.functions as F

df = spark.read.table('employee')
# df.display()

df_even_empids = df.filter((df.empid%2)==0).orderBy('empid')
# df_even_empids.show()
df_odd_empids = df.filter((df.empid%2)!=0).orderBy('empid')
# df_odd_empids.show()

df_even_empids.union(df_odd_empids).display()

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC with emp_even as (select * from employee where empid%2==0 order by empid),
# MAGIC  emp_odd as (select * from employee where empid%2!=0 order by empid)
# MAGIC select * from emp_even
# MAGIC union all
# MAGIC select * from emp_odd;
# MAGIC