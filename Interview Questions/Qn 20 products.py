# Databricks notebook source
"""
When accessing Accenture's retailer client's database, you observe that the category column in products table contains null values.

Write a query that returns the updated product table with all the category values filled in
hashtag#Assumption: The first product in each category will always have a defined category value.

CREATE TABLE products (
 product_id INT,
 category VARCHAR(255),
 name VARCHAR(255)
);

INSERT INTO products (product_id, category, name) VALUES
(1, 'Shoes', 'Sperry Boat Shoe'),
(2, NULL, 'Adidas Stan Smith'),
(3, NULL, 'Vans Authentic'),
(4, 'Jeans', 'Levi 511'),
(5, NULL, 'Wrangler Straight Fit'),
(6, 'Shirts', 'Lacoste Classic Polo'),
(7, NULL, 'Nautica Linen Shirt');
"""
print()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE products (
# MAGIC  product_id INT,
# MAGIC  category VARCHAR(255),
# MAGIC  name VARCHAR(255)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO products (product_id, category, name) VALUES
# MAGIC (1, 'Shoes', 'Sperry Boat Shoe'),
# MAGIC (2, NULL, 'Adidas Stan Smith'),
# MAGIC (3, NULL, 'Vans Authentic'),
# MAGIC (4, 'Jeans', 'Levi 511'),
# MAGIC (5, NULL, 'Wrangler Straight Fit'),
# MAGIC (6, 'Shirts', 'Lacoste Classic Polo'),
# MAGIC (7, NULL, 'Nautica Linen Shirt');

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.window as W

df = spark.read.table('products')

windowSpec = W.Window().orderBy('product_id')
windowSpec_prev = W.Window().partitionBy('cnt').orderBy('product_id')

df = df.withColumn('cnt',F.count('category').over(windowSpec))

df.withColumn('category',\
    F.when(F.isnull(F.col('category')),F.first_value('category').over(windowSpec_prev))\
        .otherwise(F.col('category'))
        )\
        .select('product_id','category','name').show()

# COMMAND ----------




