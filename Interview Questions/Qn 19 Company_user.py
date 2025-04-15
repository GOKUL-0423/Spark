# Databricks notebook source
"""
Identify companies that have at least 2 users who speak both English and German.

CREATE TABLE Company_user (
 Company_Id VARCHAR(512),
 User_Id INT,
 Language VARCHAR(512)
); 
INSERT INTO Company_user (Company_Id, User_Id, Language) VALUES ('1', '1', 'German'),
('1', '1', 'English'),('1', '2', 'German'),
('1', '3', 'English'),('1', '3', 'German'),
('1', '4', 'English'),('2', '5', 'German'),
('2', '5', 'English'),('2', '6', 'Spanish'),
('2', '6', 'English'),('2', '7', 'English');

"""
print()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Company_user (
# MAGIC  Company_Id VARCHAR(512),
# MAGIC  User_Id INT,
# MAGIC  Language VARCHAR(512)
# MAGIC ); 
# MAGIC INSERT INTO Company_user (Company_Id, User_Id, Language) VALUES ('1', '1', 'German'),
# MAGIC ('1', '1', 'English'),('1', '2', 'German'),
# MAGIC ('1', '3', 'English'),('1', '3', 'German'),
# MAGIC ('1', '4', 'English'),('2', '5', 'German'),
# MAGIC ('2', '5', 'English'),('2', '6', 'Spanish'),
# MAGIC ('2', '6', 'English'),('2', '7', 'English');

# COMMAND ----------

"""
Identify companies that have at least 2 users who speak both English and German.
"""

import pyspark.sql.functions as F
import pyspark.sql.window as W

df = spark.read.table('Company_user')

df.display()

# COMMAND ----------

# DBTITLE 1,Solution -1
"""
Identify companies that have at least 2 users who speak both English and German.
"""
import pyspark.sql.functions as F
import pyspark.sql.window as W

df = spark.read.table('Company_user')\
        .filter(F.col('Language').isin('English','German'))

df.groupBy('Company_Id','User_Id').agg(F.count('User_Id').alias('count_user_id'))\
    .filter('count_user_id = 2')\
    .groupBy('company_Id').count()\
    .filter('count=2')\
    .select('company_id')\
    .show()

# COMMAND ----------

# DBTITLE 1,Solution - 2
windowSpec = W.Window().partitionBy('Company_Id','User_Id').orderBy('Language')

df.withColumn('dense_rank',F.dense_rank().over(windowSpec))\
        .filter('dense_rank=2')\
        .groupBy('Company_Id').count()\
        .filter('count = 2')\
        .select('Company_Id')\
        .show()

# COMMAND ----------

df.groupBy('Company_Id','User_Id').agg(F.count('User_Id').alias('count_user_id'))\
    .filter('count_user_id = 2')\
    .groupBy('company_Id').count()\
    .filter('count=2')\
    .select('company_id')\
    .show()


# COMMAND ----------

# MAGIC %sql
# MAGIC select Company_Id,User_Id,count(User_Id) from company_user
# MAGIC where Language in ('German','English')
# MAGIC group by Company_Id,User_Id
# MAGIC having count(user_id)==2;