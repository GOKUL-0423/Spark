# Databricks notebook source
"""
#The_Challenge:

Given a PageVisits table with user_id, entry_page, and exit_page columns, we needed to find the first and last pages visited by each user.

Key_Assumptions:

hashtag#Sequential_Visits: Each row represents a sequential visit by a user.
hashtag#Single_User_Sessions: The data represents individual user sessions.
hashtag#Direct_Page_Transitions: The exit_page of one visit is assumed to be the entry_page of the next visit for the same user.
hashtag#No_Time_Information: The analysis is purely based on the sequence of page visits.

CREATE TABLE PageVisits (
 user_id INT,
 entry_page VARCHAR(50),
 exit_page VARCHAR(50)
);
INSERT INTO PageVisits (user_id, entry_page, exit_page) VALUES
(1, 'Home', 'Product1'),
(1, 'Product1', 'Cart'),
(1, 'Cart', 'Checkout'),
(2, 'About', 'Contact'),
(2, 'Contact', 'Blog'),
(3, 'Home', 'Services'),
(3, 'Services', 'About'),
(4, 'Product3', 'Cart'),
(4, 'Cart', 'Checkout');
"""
print()

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists pagevisits;
# MAGIC CREATE TABLE PageVisits (
# MAGIC  user_id INT,
# MAGIC  entry_page VARCHAR(50),
# MAGIC  exit_page VARCHAR(50)
# MAGIC )
# MAGIC -- using csv
# MAGIC -- bucket by (user_id)
# MAGIC ;
# MAGIC INSERT INTO PageVisits (user_id, entry_page, exit_page) VALUES
# MAGIC (1, 'Home', 'Product1'),
# MAGIC (1, 'Product1', 'Cart'),
# MAGIC (1, 'Cart', 'Checkout'),
# MAGIC (2, 'About', 'Contact'),
# MAGIC (2, 'Contact', 'Blog'),
# MAGIC (3, 'Home', 'Services'),
# MAGIC (3, 'Services', 'About'),
# MAGIC (4, 'Product3', 'Cart'),
# MAGIC (4, 'Cart', 'Checkout');

# COMMAND ----------

"""
Given a PageVisits table with user_id, entry_page, and exit_page columns, we needed to find the first and last pages visited by each user.
"""
import pyspark.sql.functions as F
df = spark.read.table('PageVisits')
df.display()

# COMMAND ----------



df_entry_page = df.alias('entry').join(df.alias('exit'),F.expr('entry.user_id=exit.user_id and entry.entry_page=exit.exit_page'),'leftanti')
df_exit_page = df.alias('exit').join(df.alias('entry'),F.expr('entry.user_id=exit.user_id and entry.entry_page=exit.exit_page'),'leftanti')

df_final = df_entry_page.alias('entry').join(df_exit_page.alias('exit'),df_entry_page.user_id==df_exit_page.user_id,'inner')\
            .selectExpr('entry.user_id','entry.entry_page','exit.exit_page')

df_final.display()


# COMMAND ----------

# MAGIC %sql
# MAGIC select p1.user_id,p1.entry_page,p2.user_id,p2.exit_page from pagevisits p1 left join pagevisits p2
# MAGIC on p1.user_id = p2.user_id and p1.entry_page = p2.exit_page
# MAGIC where p2.user_id is null;
# MAGIC
# MAGIC select p1.*,p2.* from pagevisits p1 right join pagevisits p2
# MAGIC on p1.user_id = p2.user_id and p1.entry_page = p2.exit_page
# MAGIC -- where p1.user_id is null
# MAGIC ;