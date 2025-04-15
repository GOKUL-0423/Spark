# Databricks notebook source
"""
Given a dataset of Indian cities with their respective state and population, the goal was to find the most and least populated city per state — side by side.

CREATE TABLE city_pop(
 state VARCHAR(50),
 city VARCHAR(50),
 population INT
);

INSERT INTO city_pop (state, city, population) VALUES ('haryana', 'ambala', 100),('haryana', 'panipat', 200),
('haryana', 'gurgaon', 300),('punjab', 'amritsar', 150),('punjab', 'ludhiana', 400),
('punjab', 'jalandhar', 250),('maharashtra', 'mumbai', 1000),('maharashtra', 'pune', 600),
('maharashtra', 'nagpur', 300),('karnataka', 'bangalore', 900),('karnataka', 'mysore', 400),
('karnataka', 'mangalore', 200);
"""
print()

# COMMAND ----------

dbutils.widgets.text("table",'')

# COMMAND ----------

try:
    dbutils.fs.rm('dbfs:/user/hive/warehouse/city_pop/',True)
except Exception as e:
    print('Failed')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE city_pop(
# MAGIC  state VARCHAR(50),
# MAGIC  city VARCHAR(50),
# MAGIC  population INT
# MAGIC );
# MAGIC
# MAGIC INSERT INTO city_pop (state, city, population) VALUES ('haryana', 'ambala', 100),('haryana', 'panipat', 200),
# MAGIC ('haryana', 'gurgaon', 300),('punjab', 'amritsar', 150),('punjab', 'ludhiana', 400),
# MAGIC ('punjab', 'jalandhar', 250),('maharashtra', 'mumbai', 1000),('maharashtra', 'pune', 600),
# MAGIC ('maharashtra', 'nagpur', 300),('karnataka', 'bangalore', 900),('karnataka', 'mysore', 400),
# MAGIC ('karnataka', 'mangalore', 200);

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.window as W
# creating spark dataframe on city_pop table 
df = spark.read.table('city_pop')
windowSpecAsc = W.Window().partitionBy('state').orderBy('population')
windowSpecDesc = W.Window().partitionBy('state').orderBy(F.desc('population'))
# Taking the min_population state,city 
df_pop_min = df.withColumn('rownum',F.row_number().over(windowSpecAsc))\
            .filter('rownum=1')\
            .select(df.columns)
# Taking the max_population state,city 
df_pop_max = df.withColumn('rownum',F.row_number().over(windowSpecDesc))\
            .filter('rownum=1')\
            .select(df.columns)
# joining the max and min population dataframe to get a final output
df_pop_max.alias('max_pop').join(df_pop_min.alias('min_pop'),df_pop_max['state']==df_pop_min['state'],'inner')\
    .selectExpr('max_pop.state as state','max_pop.city as max_populated','min_pop.city as least_populated')\
    .show()


# COMMAND ----------

windowSpecAsc = W.Window().partitionBy('state').orderBy('population')
windowSpecDesc = W.Window().partitionBy('state').orderBy(F.desc('population'))
df_pop_min = df.withColumn('rownum',F.row_number().over(windowSpecAsc))\
            .filter('rownum=1')\
            .select(df.columns)

df_pop_max = df.withColumn('rownum',F.row_number().over(windowSpecDesc))\
            .filter('rownum=1')\
            .select(df.columns)

df_pop_max.alias('max_pop').join(df_pop_min.alias('min_pop'),df_pop_max['state']==df_pop_min['state'],'inner')\
    .selectExpr('max_pop.state as state','max_pop.city as max_populated','min_pop.city as least_populated')\
    .show()