# Databricks notebook source
# MAGIC %run ./Remove_hive_tables

# COMMAND ----------

"""
Today's challenge? Identify phone numbers that meet two conditions:
✅ They have both incoming and outgoing calls.
✅ Their total outgoing call duration is greater than their total incoming call duration.

💡 Understanding the Data
We have a call_details table that logs:
📌 call_type → Incoming (INC), Outgoing (OUT), or SMS
📌 call_number → The phone number involved in the call
📌 call_duration → Duration of the call in seconds(for SMS it is always 1)

create table call_details (
call_type varchar(10),
call_number varchar(12),
call_duration int
);
insert into call_details
values ('OUT','181868',13),('OUT','2159010',8)
,('OUT','2159010',178),('SMS','4153810',1),('OUT','2159010',152),('OUT','9140152',18),('SMS','4162672',1)
,('SMS','9168204',1),('OUT','9168204',576),('INC','2159010',5),('INC','2159010',4),('SMS','2159010',1)
,('SMS','4535614',1),('OUT','181868',20),('INC','181868',54),('INC','218748',20),('INC','2159010',9)
,('INC','197432',66),('SMS','2159010',1),('SMS','4535614',1);
"""
print()

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog hive_metastore;
# MAGIC drop table if exists call_details1;
# MAGIC
# MAGIC
# MAGIC
# MAGIC create table if not exists call_details1 (
# MAGIC call_type varchar(10),
# MAGIC call_number varchar(12),
# MAGIC call_duration int
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into call_details1
# MAGIC values ('OUT','181868',13),('OUT','2159010',8)
# MAGIC ,('OUT','2159010',178),('SMS','4153810',1),('OUT','2159010',152),('OUT','9140152',18),('SMS','4162672',1)
# MAGIC ,('SMS','9168204',1),('OUT','9168204',576),('INC','2159010',5),('INC','2159010',4),('SMS','2159010',1)
# MAGIC ,('SMS','4535614',1),('OUT','181868',20),('INC','181868',54),('INC','218748',20),('INC','2159010',9)
# MAGIC ,('INC','197432',66),('SMS','2159010',1),('SMS','4535614',1);

# COMMAND ----------

"""
Today's challenge? Identify phone numbers that meet two conditions:
✅ They have both incoming and outgoing calls.
✅ Their total outgoing call duration is greater than their total incoming call duration.
"""
import pyspark.sql.functions as F

df_call = spark.read.table('call_details1')

# creating separate dataframe for INC and OUT calls
df_inc = df_call.filter(df_call.call_type == 'INC')
df_out = df_call.filter(df_call.call_type == 'OUT')
# Join the INC and OUT dataframe to get the INC and OUT call duration as column values
df_both_calls = df_inc.alias('inc').join(df_out.alias('out'),df_inc.call_number == df_out.call_number,'inner')\
    .selectExpr('inc.call_number','inc.call_duration as inc_call_dur','out.call_duration as out_call_dur')
# Perform groupby on call number column to get the total INC and OUT call duration
df_group = df_both_calls.groupBy('call_number').agg(F.sum('inc_call_dur').alias('total_inc')\
                                        ,F.sum('out_call_dur').alias('total_out'))\
# Filtering rows where total outgoing call duration is greater than their total incoming call duration
df_group.filter(F.col('total_out') > F.col('total_inc'))\
    .select('call_number').show()
                                        
    
