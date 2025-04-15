# Databricks notebook source
"""
Faced a real-world data scenario — how to find products that were sold exclusively during the first quarter (Jan–Mar), and never in any other part of the year.
CREATE TABLE sales (
 product_id INT,
 sale_date DATE,
 sale_amount INT
);
INSERT INTO sales (product_id, sale_date, sale_amount) VALUES
(101, '2023-01-15', 200),
(101, '2023-04-10', 300),
(101, '2023-08-12', 250),
(101, '2023-11-22', 400),
(102, '2023-01-20', 100),
(102, '2023-02-05', 120),
(103, '2023-03-18', 80),
(103, '2023-07-25', 150),
(103, '2023-12-01', 200),
(104, '2023-08-18', 300),
(105, '2023-05-12', 400);

"""
print()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE sales (
# MAGIC  product_id INT,
# MAGIC  sale_date DATE,
# MAGIC  sale_amount INT
# MAGIC );
# MAGIC INSERT INTO sales (product_id, sale_date, sale_amount) VALUES
# MAGIC (101, '2023-01-15', 200),
# MAGIC (101, '2023-04-10', 300),
# MAGIC (101, '2023-08-12', 250),
# MAGIC (101, '2023-11-22', 400),
# MAGIC (102, '2023-01-20', 100),
# MAGIC (102, '2023-02-05', 120),
# MAGIC (103, '2023-03-18', 80),
# MAGIC (103, '2023-07-25', 150),
# MAGIC (103, '2023-12-01', 200),
# MAGIC (104, '2023-08-18', 300),
# MAGIC (105, '2023-05-12', 400);

# COMMAND ----------

import pyspark.sql.functions as F

df = spark.read.table('sales')

df.display()

# COMMAND ----------

# DBTITLE 1,Solution I
    """
    how to find products that were sold exclusively during the first quarter (Jan–Mar), and never in any other part of the year.
    """
    import pyspark.sql.functions as F

    df = spark.read.table('sales')
    # Filtering the products only sold on first quarter
    df_first_quarter = df.filter(F.month('sale_date').isin(1,2,3))

    #collecting all the product id's which are not sold in first quarter
    df_product_ids = df.select("product_id").filter(~F.month('sale_date').isin(1,2,3)).rdd.flatMap(lambda x:x).collect()

    df_first_quarter.select('product_id')\
        .filter(~df_first_quarter.product_id.isin(df_product_ids))\
        .distinct()\
        .show()



# COMMAND ----------

# DBTITLE 1,Solution - II

# Collecting the products which are not sold in first quarter
df_except_first_quarter = df.select("product_id").filter(~F.month('sale_date').isin(1,2,3))
df_first_quarter.alias('first').join(df_except_first_quarter.alias('all'),F.col('first.product_id')==F.col('all.product_id'),'left_anti')\
    .select('first.product_id')\
    .distinct()\
    .show()