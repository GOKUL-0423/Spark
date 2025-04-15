# Databricks notebook source
"""
🔹 Condition:
 Customers receive an instant cashback of 2% on every transaction where they spend money using a credit card.
📊 Question:
 Write an SQL query to compute the cumulative running net balance for each customer, factoring in this cashback condition.

Table Script:
create table bankstatement(custid int,txndate date,status varchar(50),mode varchar(50),amount int);
insert into bankstatement values(101,CAST('20220425' AS DATE),'credit','Cash',800);
insert into bankstatement values(101,CAST('20220425' AS DATE),'credit','UPI',600);
insert into bankstatement values(101,CAST('20220428' AS DATE),'credit','UPI',200);
insert into bankstatement values(101,CAST('20220429' AS DATE),'debit','Credit Card',300);

insert into bankstatement values(102,CAST('20220315' AS DATE),'credit','UPI',1800);
insert into bankstatement values(102,CAST('20220320' AS DATE),'debit','UPI',200);
insert into bankstatement values(102,CAST('20220420' AS DATE),'debit','Credit Card',700);
"""
print()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table if not exists bankstatement(custid int,txndate date,status varchar(50),mode varchar(50),amount int);
# MAGIC insert into bankstatement values(101,CAST('2022-04-25' AS DATE),'credit','Cash',800);
# MAGIC insert into bankstatement values(101,CAST('2022-04-25' AS DATE),'credit','UPI',600);
# MAGIC insert into bankstatement values(101,CAST('2022-04-28' AS DATE),'credit','UPI',200);
# MAGIC insert into bankstatement values(101,CAST('2022-04-29' AS DATE),'debit','Credit Card',300);
# MAGIC
# MAGIC insert into bankstatement values(102,CAST('2022-03-15' AS DATE),'credit','UPI',1800);
# MAGIC insert into bankstatement values(102,CAST('2022-03-20' AS DATE),'debit','UPI',200);
# MAGIC insert into bankstatement values(102,CAST('2022-04-20' AS DATE),'debit','Credit Card',700);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bankstatement order by custid,txndate;

# COMMAND ----------

"""
🔹 Condition:
 Customers receive an instant cashback of 2% on every transaction where they spend money using a credit card.
📊 Question:
 Write an SQL query to compute the cumulative running net balance for each customer, factoring in this cashback condition.
"""
import pyspark.sql.functions as F
import pyspark.sql.window as W

df = spark.read.table('bankstatement').orderBy('custid','txndate')
df = df.withColumn('cashback',F.when(df['mode']=='Credit Card',df['amount']*0.02).otherwise(F.lit(0)))
windowSpec = W.Window().partitionBy('custid').orderBy('custid','txndate').rowsBetween(W.Window.unboundedPreceding,0)

df_netprice = df.withColumn('net_balance',F.sum(\
        F.when(df['status']=='credit',df.amount+df.cashback)\
        .when(df['status']=='debit',-df.amount+df.cashback)\
        ).over(windowSpec))

df_netprice.display()



# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as
# MAGIC (
# MAGIC   select 
# MAGIC         *, 
# MAGIC         case when mode = 'Credit Card' then round(amount * 0.02) else 0 end as Cashback
# MAGIC         from bankstatement 
# MAGIC
# MAGIC )
# MAGIC select 
# MAGIC     *,sum(case when status = 'credit' then amount+Cashback
# MAGIC                when status = 'debit' then -amount+Cashback
# MAGIC               end) over(partition by custid order by txndate rows between unbounded preceding and current row) as net_balance
# MAGIC     from cte