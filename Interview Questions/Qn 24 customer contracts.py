# Databricks notebook source
"""
https://www.linkedin.com/posts/sneha-majumder_supercloudabrcustomers-theabrchallenge-solutionabrbreakdown-activity-7309052091816153088-eU8p?utm_source=share&utm_medium=member_desktop&rcm=ACoAADC8k3MBr4BkJYHxs3S3v6NEzSWeAYOuvqc

A "Supercloud" customer is defined as someone who's purchased at least one product from every product category in products table. We needed to identify these customer IDs. 📊

CREATE TABLE customer_contracts (
 customer_id INT,
 product_id INT,
 amount INT
);

INSERT INTO customer_contracts (customer_id, product_id, amount) VALUES
(1, 1, 1000),(2, 2, 2000),(3, 1, 1100),
(4, 1, 1000),(7, 1, 1000),(7, 3, 4000),
(6, 4, 2000),(1, 5, 1500),(2, 5, 2000),
(4, 5, 2200),(7, 6, 5000),(1, 2, 2000);

CREATE TABLE products (
 product_id INT,
 product_category VARCHAR(255),
 product_name VARCHAR(255)
);
INSERT INTO products (product_id, product_category, product_name) VALUES
(1, 'Analytics', 'Azure Databricks'),
(2, 'Analytics', 'Azure Stream Analytics'),
(3, 'Containers', 'Azure Kubernetes Service'),
(4, 'Containers', 'Azure Service Fabric'),
(5, 'Compute', 'Virtual Machines'),
(6, 'Compute', 'Azure Functions');

"""
print()

# COMMAND ----------

try:
    dbutils.fs.rm('dbfs:/user/hive/warehouse/',True)
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists customer_contracts;
# MAGIC CREATE TABLE customer_contracts (
# MAGIC  customer_id INT,
# MAGIC  product_id INT,
# MAGIC  amount INT
# MAGIC );
# MAGIC
# MAGIC INSERT INTO customer_contracts (customer_id, product_id, amount) VALUES
# MAGIC (1, 1, 1000),(2, 2, 2000),(3, 1, 1100),
# MAGIC (4, 1, 1000),(7, 1, 1000),(7, 3, 4000),
# MAGIC (6, 4, 2000),(1, 5, 1500),(2, 5, 2000),
# MAGIC (4, 5, 2200),(7, 6, 5000),(1, 2, 2000);
# MAGIC
# MAGIC CREATE TABLE products (
# MAGIC  product_id INT,
# MAGIC  product_category VARCHAR(255),
# MAGIC  product_name VARCHAR(255)
# MAGIC );
# MAGIC INSERT INTO products (product_id, product_category, product_name) VALUES
# MAGIC (1, 'Analytics', 'Azure Databricks'),
# MAGIC (2, 'Analytics', 'Azure Stream Analytics'),
# MAGIC (3, 'Containers', 'Azure Kubernetes Service'),
# MAGIC (4, 'Containers', 'Azure Service Fabric'),
# MAGIC (5, 'Compute', 'Virtual Machines'),
# MAGIC (6, 'Compute', 'Azure Functions');
# MAGIC

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.window as W

df_cust_contracts = spark.read.table('customer_contracts')
df_products = spark.read.table('products')

df_join = df_products.join(df_cust_contracts,df_products.product_id == df_cust_contracts.product_id,'inner')\
    .select(df_cust_contracts.customer_id,df_products.product_id,df_products.product_category)

df_join.groupBy('customer_id').agg(F.countDistinct('product_category').alias('cnt'))\
    .filter(F.col('cnt') == (df_products.select('product_category').distinct().count()))\
    .select('customer_id')\
    .show()

# COMMAND ----------

"""
A "Supercloud" customer is defined as someone who's purchased at least one product from every product category in products table. We needed to identify these customer IDs. 📊
"""

df_join = df_products.join(df_cust_contracts,df_products.product_id == df_cust_contracts.product_id,'inner')\
    .select(df_cust_contracts.customer_id,df_products.product_id,df_products.product_category)

windowSpec = W.Window().partitionBy('customer_id','product_category').orderBy('customer_id')

df_join.withColumn('row_num',F.row_number().over(windowSpec))\
    .filter('row_num = 1')\
    .groupBy('customer_id').agg(F.count('customer_id').alias('cnt'))\
    .filter('cnt = 3')\
    .select('customer_id')\
    .show()

# COMMAND ----------

# df_join.display()
df_join.groupBy('customer_id').agg(F.countDistinct('product_category').alias('cnt'))\
    .filter(F.col('cnt') == (df_products.select('product_category').distinct().count()))\
    .select('customer_id')\
    .show()