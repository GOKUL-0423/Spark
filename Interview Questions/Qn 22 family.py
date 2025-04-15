# Databricks notebook source
"""
https://www.linkedin.com/posts/sneha-majumder_selfabrjoin-theabrchallange-firstabrselfabrjoin-activity-7311600183027318784-36Vw?utm_source=share&utm_medium=member_desktop&rcm=ACoAADC8k3MBr4BkJYHxs3S3v6NEzSWeAYOuvqc

👀 hashtag#The_Challange:
Given a family hierarchy table, how do we find the count of people whose grandparent is alive?

📌 Understanding the Data:
Each row represents a person, their parent, and status (Alive/Dead). 
🛑 Important Clarification: The status column represents the person’s status, not their parent’s or grandparent’s. That means when checking for an alive grandparent, we must filter based on the grandparent’s status, not the person’s!

CREATE TABLE family (
 person_name VARCHAR(10),
 parent_name VARCHAR(10),
 status ENUM('Alive', 'Dead')
);
INSERT INTO family (person_name, parent_name, status) VALUES
('A', 'X', 'Alive'),
('B', 'Y', 'Dead'),
('X', 'X1', 'Alive'),
('Y', 'Y1', 'Alive'),
('X1', 'X2', 'Alive'),
('Y1', 'Y2', 'Dead');

"""
print()


# COMMAND ----------

try:
    dbutils.fs.rm('dbfs:/user/hive/warehouse/family',True)
    # dbutils.fs.rm('dbfs:/user/hive/warehouse/billings',True)
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE family (
# MAGIC  person_name VARCHAR(10),
# MAGIC  parent_name VARCHAR(10),
# MAGIC  status VARCHAR(10)
# MAGIC );
# MAGIC INSERT INTO family (person_name, parent_name, status) VALUES
# MAGIC ('A', 'X', 'Alive'),
# MAGIC ('B', 'Y', 'Dead'),
# MAGIC ('X', 'X1', 'Alive'),
# MAGIC ('Y', 'Y1', 'Alive'),
# MAGIC ('X1', 'X2', 'Alive'),
# MAGIC ('Y1', 'Y2', 'Dead');

# COMMAND ----------

import pyspark.sql.functions as F

df = spark.read.table('family')
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.sql.analyzer.failAmbiguousSelfJoin = False

# COMMAND ----------

df_alive = df.filter(df['status']=='Alive')

df_alive.alias('child').join(df_alive.alias('parent'),F.col("child.parent_name") == F.col("parent.person_name"),'inner')\
    .join(df_alive.alias("grandpa"),F.col("parent.parent_name")==F.col("grandpa.person_name"),"inner")\
    .selectExpr('child.person_name as child','parent.person_name as parent',"grandpa.person_name as grandpa","grandpa.status as granda_status").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from family where status='Alive';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC with alive_fam as(
# MAGIC     select * from family where status='Alive'
# MAGIC )
# MAGIC select child.person_name child,child.parent_name parent,parent.parent_name grandpa
# MAGIC from alive_fam child join alive_fam parent on child.parent_name = parent.person_name
# MAGIC join alive_fam gradpa on gradpa.person_name = parent.parent_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from family;

# COMMAND ----------

# MAGIC %sql
# MAGIC with family_alive as (
# MAGIC     select
# MAGIC       *
# MAGIC     from
# MAGIC       family
# MAGIC     where
# MAGIC       status = 'Alive'
# MAGIC   )
# MAGIC select
# MAGIC   child.person_name child,
# MAGIC   parent.person_name parent,
# MAGIC   parent.parent_name grandpa,
# MAGIC   grandpa.status grandpa_alive
# MAGIC from
# MAGIC   family_alive child
# MAGIC   join family_alive parent on child.parent_name = parent.person_name
# MAGIC   join family_alive grandpa on parent.parent_name = grandpa.person_name;