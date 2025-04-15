# Databricks notebook source
"""
💡𝐐𝐮𝐞𝐬𝐭𝐢𝐨𝐧:- 

/*
Write an SQL query to find the percentage of immediate orders in 
the first orders of all customers, rounded to 2 decimal places.

The first order of a customer is the order with the earliest order
date that customer made. It is guaranteed that a customer has exactly one first order.

If the preferred delivery date of the customer is the same as 
the order date then the order is called immediate otherwise it's called scheduled.
*/

-- 𝐓𝐚𝐛𝐥𝐞 
CREATE TABLE Delivery (
 delivery_id INT PRIMARY KEY,
 customer_id INT NOT NULL,
 order_date DATE NOT NULL,
 customer_pref_delivery_date DATE NOT NULL
)

-- 𝐈𝐧𝐬𝐞𝐫𝐭 𝐭𝐡𝐞 𝐝𝐚𝐭𝐚 
INSERT INTO Delivery (delivery_id, customer_id, order_date, customer_pref_delivery_date) VALUES
(1, 1, '2019-08-01', '2019-08-02'),(2, 2, '2019-08-02', '2019-08-02'),
(3, 1, '2019-08-11', '2019-08-12'),(4, 3, '2019-08-24', '2019-08-24'),
(5, 3, '2019-08-21', '2019-08-22'),(6, 2, '2019-08-11', '2019-08-13'),
(7, 4, '2019-08-09', '2019-08-09');
"""
print()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not exists Delivery (
# MAGIC  delivery_id INT ,
# MAGIC  customer_id INT NOT NULL,
# MAGIC  order_date DATE NOT NULL,
# MAGIC  customer_pref_delivery_date DATE NOT NULL
# MAGIC );
# MAGIC
# MAGIC INSERT INTO Delivery 
# MAGIC (delivery_id, customer_id, order_date, customer_pref_delivery_date) 
# MAGIC VALUES
# MAGIC (1, 1, '2019-08-01', '2019-08-02'),(2, 2, '2019-08-02', '2019-08-02'),
# MAGIC (3, 1, '2019-08-11', '2019-08-12'),(4, 3, '2019-08-24', '2019-08-24'),
# MAGIC (5, 3, '2019-08-21', '2019-08-22'),(6, 2, '2019-08-11', '2019-08-13'),
# MAGIC (7, 4, '2019-08-09', '2019-08-09');

# COMMAND ----------

"""
Write an SQL query to find the percentage of immediate orders in 
the first orders of all customers, rounded to 2 decimal places.

The first order of a customer is the order with the earliest order
date that customer made. It is guaranteed that a customer has exactly one first order.

If the preferred delivery date of the customer is the same as 
the order date then the order is called immediate otherwise it's called scheduled.
"""

import pyspark.sql.functions as F
import pyspark.sql.window as W

df_delivery = spark.read.table('Delivery')

windowSpec = W.Window().partitionBy('customer_id').orderBy('order_date')

df_first_order = df_delivery.withColumn('rownum',F.row_number().over(windowSpec))\
    .filter(F.col('rownum')==1)

df_first_order.groupBy()\
    .agg((F.count(F.when(F.col('order_date')==F.col('customer_pref_delivery_date'),1))/F.count('*')*100)\
        .alias('immediate_order_perc'))\
    .select(F.round(F.col('immediate_order_perc'),2).alias('immediate_order_perc'))\
    .show()

    