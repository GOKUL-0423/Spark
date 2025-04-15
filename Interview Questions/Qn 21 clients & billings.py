# Databricks notebook source
"""
https://www.linkedin.com/posts/sneha-majumder_theabrchallenge-topabr10-join-activity-7309418204076945408-nDha?utm_source=share&utm_medium=member_desktop&rcm=ACoAADC8k3MBr4BkJYHxs3S3v6NEzSWeAYOuvqc

Find the hashtag#top_10 EY clients by total billing amount within the past year, using client and billing data, sorted descending.

CREATE TABLE clients (
 client_id INT PRIMARY KEY,
 client_name VARCHAR(255),
 industry VARCHAR(255)
);
INSERT INTO clients (client_id, client_name, industry) VALUES
(100, 'ABC Corp', 'Technology'),(101, 'DEF Inc', 'Healthcare'),
(102, 'GHI LLC', 'Finance'),(103, 'JKL Ltd', 'Retail'),
(104, 'MNO Group', 'Manufacturing'),(105, 'PQR Solutions', 'Technology'),
(106, 'STU Enterprises', 'Finance'),(107, 'VWX Holdings', 'Healthcare'),
(108, 'YZ Corp', 'Retail'),(109, 'AA Industries', 'Manufacturing');

CREATE TABLE billings (
 bill_id INT PRIMARY KEY,
 client_id INT,
 billing_date DATE,
 amount DECIMAL(10, 2)
);
INSERT INTO billings (bill_id, client_id, billing_date, amount) VALUES
(200, 100, '2023-06-22', 50000.00),(201, 101, '2023-07-13', 75000.00),
(202, 102, '2023-05-08', 36000.00),(203, 100, '2023-11-15', 25000.00),
(204, 102, '2023-09-05', 40000.00),(205, 103, '2023-01-10', 80000.00),
(206, 104, '2023-02-20', 60000.00),(207, 105, '2023-03-15', 90000.00),
(208, 106, '2023-04-01', 70000.00),(209, 107, '2023-05-08', 55000.00),
(210, 108, '2023-06-22', 45000.00),(211, 109, '2023-07-13', 65000.00),
(212, 100, '2024-01-01', 30000.00),(213, 101, '2024-02-01', 85000.00),
(214, 102, '2024-03-01', 50000.00),(215, 103, '2024-04-01', 95000.00),
(216, 104, '2024-05-01', 75000.00),(217, 105, '2024-06-01', 100000.00),
(218, 106, '2024-07-01', 80000.00),(219, 106, '2024-07-03', 15000.00),
(220, 107, '2024-08-01', 60000.00),(221, 108, '2024-09-01', 50000.00),
(222, 109, '2024-10-01', 70000.00),(223, 100, '2025-01-01', 10000.00),
(224, 101, '2025-02-01', 10000.00),(225, 102, '2025-03-01', 10000.00);
"""
print()

# COMMAND ----------

try:
    dbutils.fs.rm('dbfs:/user/hive/warehouse/clients',True)
    dbutils.fs.rm('dbfs:/user/hive/warehouse/billings',True)
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists clients;
# MAGIC
# MAGIC CREATE TABLE clients (
# MAGIC  client_id INT ,
# MAGIC  client_name VARCHAR(255),
# MAGIC  industry VARCHAR(255)
# MAGIC );
# MAGIC INSERT INTO clients (client_id, client_name, industry) VALUES
# MAGIC (100, 'ABC Corp', 'Technology'),(101, 'DEF Inc', 'Healthcare'),
# MAGIC (102, 'GHI LLC', 'Finance'),(103, 'JKL Ltd', 'Retail'),
# MAGIC (104, 'MNO Group', 'Manufacturing'),(105, 'PQR Solutions', 'Technology'),
# MAGIC (106, 'STU Enterprises', 'Finance'),(107, 'VWX Holdings', 'Healthcare'),
# MAGIC (108, 'YZ Corp', 'Retail'),(109, 'AA Industries', 'Manufacturing');
# MAGIC
# MAGIC CREATE TABLE billings (
# MAGIC  bill_id INT ,
# MAGIC  client_id INT,
# MAGIC  billing_date DATE,
# MAGIC  amount DECIMAL(10, 2)
# MAGIC );
# MAGIC INSERT INTO billings (bill_id, client_id, billing_date, amount) VALUES
# MAGIC (200, 100, '2023-06-22', 50000.00),(201, 101, '2023-07-13', 75000.00),
# MAGIC (202, 102, '2023-05-08', 36000.00),(203, 100, '2023-11-15', 25000.00),
# MAGIC (204, 102, '2023-09-05', 40000.00),(205, 103, '2023-01-10', 80000.00),
# MAGIC (206, 104, '2023-02-20', 60000.00),(207, 105, '2023-03-15', 90000.00),
# MAGIC (208, 106, '2023-04-01', 70000.00),(209, 107, '2023-05-08', 55000.00),
# MAGIC (210, 108, '2023-06-22', 45000.00),(211, 109, '2023-07-13', 65000.00),
# MAGIC (212, 100, '2024-01-01', 30000.00),(213, 101, '2024-02-01', 85000.00),
# MAGIC (214, 102, '2024-03-01', 50000.00),(215, 103, '2024-04-01', 95000.00),
# MAGIC (216, 104, '2024-05-01', 75000.00),(217, 105, '2024-06-01', 100000.00),
# MAGIC (218, 106, '2024-07-01', 80000.00),(219, 106, '2024-07-03', 15000.00),
# MAGIC (220, 107, '2024-08-01', 60000.00),(221, 108, '2024-09-01', 50000.00),
# MAGIC (222, 109, '2024-10-01', 70000.00),(223, 100, '2025-01-01', 10000.00),
# MAGIC (224, 101, '2025-02-01', 10000.00),(225, 102, '2025-03-01', 10000.00);

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.window as W

df_clients = spark.read.table('clients')
df_billings =  spark.read.table('billings')

df_billings\
    .filter(F.year(df_billings.billing_date)==F.year(F.current_date())-1)\
    .groupBy('client_id').agg(F.sum('amount').alias('Total_amount'))\
    .join(df_clients,df_clients['client_id']==df_billings['client_id'],'inner')\
    .select(df_clients['client_name'],'Total_amount')\
    .orderBy(F.desc('Total_amount'))\
    .limit(10)\
    .show()


# COMMAND ----------

df_billings.filter(df_billings.billing_date>F.add_months(F.current_date(),-12))\
    .groupBy('client_id').agg(F.sum('amount').alias('Total_amount'))\
    .join(df_clients,df_clients['client_id']==df_billings['client_id'],'inner')\
    .select(df_clients['client_name'],'Total_amount')\
    .orderBy(F.desc('Total_amount'))\
    .show()


# COMMAND ----------

# MAGIC %sql
# MAGIC select current_date(),add_months(current_date(),-12 ),current_date()+interval 1 year,date_add(current_date(),-365 );

# COMMAND ----------


# df_billings.select(F.to_date(F.lit("31-12-2024"),'dd-MM-yyyy').alias('new_dr')).display()

df_billings.filter(df_billings.billing_date>F.add_months(F.to_date(F.lit("31-12-2024"),'dd-MM-yyyy'),-12))\
    .groupBy('client_id').agg(F.sum('amount').alias('Total_amount'))\
    .join(df_clients,df_clients['client_id']==df_billings['client_id'],'inner')\
    .select(df_clients['client_name'],'Total_amount')\
    .orderBy(F.desc('Total_amount'))\
    .limit(10)\
    .show()


# COMMAND ----------

df_billings.filter(F.year(df_billings.billing_date)==F.lit('2024')).show()

df_billings\
    .filter(df_billings.billing_date.between(F.add_months(F.to_date(F.lit("31-12-2024"),'dd-MM-yyyy'),-12),F.to_date(F.lit("31-12-2024"),'dd-MM-yyyy')))\
    .groupBy('client_id').agg(F.sum('amount').alias('Total_amount'))\
    .join(df_clients,df_clients['client_id']==df_billings['client_id'],'inner')\
    .select(df_clients['client_name'],'Total_amount')\
    .orderBy(F.desc('Total_amount'))\
    .limit(10)\
    .show()



# COMMAND ----------

df_billings\
    .filter(F.year(df_billings.billing_date)==F.year(F.current_date())-1)\
    .groupBy('client_id').agg(F.sum('amount').alias('Total_amount'))\
    .join(df_clients,df_clients['client_id']==df_billings['client_id'],'inner')\
    .select(df_clients['client_name'],'Total_amount')\
    .orderBy(F.desc('Total_amount'))\
    .limit(10)\
    .show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select year(current_date())-1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find the #top_10 EY clients by total billing amount within the past year, using client and billing data, sorted descending.
# MAGIC
# MAGIC select * from clients;
# MAGIC select * from billings;
# MAGIC
# MAGIC --Find the total invest for each client
# MAGIC explain plan for
# MAGIC with cte as (select client_id,sum(amount) sum_amount from billings
# MAGIC group by client_id),
# MAGIC cte1 as (select current_date() curr_date)
# MAGIC select t1.client_id,t2.client_name,t1.sum_amount from cte t1 join clients t2 on t1.client_id=t2.client_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- explain formatted
# MAGIC with cte as (
# MAGIC   select
# MAGIC     client_id,
# MAGIC     sum(amount) sum_amount
# MAGIC   from
# MAGIC     billings
# MAGIC   group by
# MAGIC     client_id
# MAGIC ),
# MAGIC cte1 as (
# MAGIC   select
# MAGIC     current_date() curr_date
# MAGIC )
# MAGIC select
# MAGIC   t1.client_id,
# MAGIC   t2.client_name,
# MAGIC   t1.sum_amount
# MAGIC from
# MAGIC   cte t1
# MAGIC   join clients t2 on t1.client_id = t2.client_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC select c.client_id,c.client_name,sum(b.amount) Total_amt from billings b join clients c on b.client_id=c.client_id
# MAGIC group by c.client_id,c.client_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from billings order by client_id,billing_date;
# MAGIC
# MAGIC select c.* from(
# MAGIC select dense_rank() over(partition by client_id order by billing_date) drnk,
# MAGIC        * from billings) c
# MAGIC -- where drnk=2
# MAGIC ;