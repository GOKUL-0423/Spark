# Databricks notebook source
"""
💡𝐐𝐮𝐞𝐬𝐭𝐢𝐨𝐧:- 
/*
Risk is calculated by the addition of CBC, RBH, and LBH. If the sum is more than 20, "High", between 16-20 then "Medium", else "Low"
*/

-- 𝐓𝐚𝐛𝐥𝐞 -𝟏 
CREATE TABLE insurance (
 Patient_id INT PRIMARY KEY,
 Insurance_id INT,
 Insurance_Name VARCHAR(50)
);

-- 𝐈𝐧𝐬𝐞𝐫𝐭 𝐭𝐡𝐞 𝐝𝐚𝐭𝐚 
INSERT INTO insurance (Patient_id, Insurance_id, Insurance_Name)
VALUES 
 (1, 1001, 'India Insurance'),(2, 1002, 'ICICI Lombard'),
 (3, 1001, 'India Insurance'),(4, 1003, 'Star Health'),
 (5, 1003, 'Star Health');

-- 𝐓𝐚𝐛𝐥𝐞 - 𝟐
CREATE TABLE test (
 Patient_id INT,
 Test_type VARCHAR(10),
 Test_score INT,
 FOREIGN KEY (Patient_id) REFERENCES insurance(Patient_id)
);

 -- 𝐈𝐧𝐬𝐞𝐫𝐭 𝐭𝐡𝐞 𝐝𝐚𝐭𝐚 
INSERT INTO test (Patient_id, Test_type, Test_score)
VALUES 
(1, 'CBC', 7),(1, 'RBC', 6),(1, 'LBH', 6),(2, 'CBC', 7),(2, 'RBC', 8),(2, 'LBH', 8),
(3, 'CBC', 5),(3, 'RBC', 4),(3, 'LBH', 4),(4, 'CBC', 4),(4, 'RBC', 6),(4, 'LBH', 6),
(5, 'CBC', 5),(5, 'RBC', 6),(5, 'LBH', 7);
"""
print()


# COMMAND ----------

# MAGIC %sql
# MAGIC -- 𝐓𝐚𝐛𝐥𝐞 -𝟏 
# MAGIC CREATE TABLE insurance (
# MAGIC  Patient_id INT ,
# MAGIC  Insurance_id INT,
# MAGIC  Insurance_Name VARCHAR(50)
# MAGIC );
# MAGIC
# MAGIC -- 𝐈𝐧𝐬𝐞𝐫𝐭 𝐭𝐡𝐞 𝐝𝐚𝐭𝐚 
# MAGIC INSERT INTO insurance (Patient_id, Insurance_id, Insurance_Name)
# MAGIC VALUES 
# MAGIC  (1, 1001, 'India Insurance'),(2, 1002, 'ICICI Lombard'),
# MAGIC  (3, 1001, 'India Insurance'),(4, 1003, 'Star Health'),
# MAGIC  (5, 1003, 'Star Health');
# MAGIC
# MAGIC -- 𝐓𝐚𝐛𝐥𝐞 - 𝟐
# MAGIC CREATE TABLE test (
# MAGIC  Patient_id INT,
# MAGIC  Test_type VARCHAR(10),
# MAGIC  Test_score INT
# MAGIC );
# MAGIC
# MAGIC  -- 𝐈𝐧𝐬𝐞𝐫𝐭 𝐭𝐡𝐞 𝐝𝐚𝐭𝐚 
# MAGIC INSERT INTO test (Patient_id, Test_type, Test_score)
# MAGIC VALUES 
# MAGIC (1, 'CBC', 7),(1, 'RBC', 6),(1, 'LBH', 6),(2, 'CBC', 7),(2, 'RBC', 8),(2, 'LBH', 8),
# MAGIC (3, 'CBC', 5),(3, 'RBC', 4),(3, 'LBH', 4),(4, 'CBC', 4),(4, 'RBC', 6),(4, 'LBH', 6),
# MAGIC (5, 'CBC', 5),(5, 'RBC', 6),(5, 'LBH', 7);

# COMMAND ----------

"""
Risk is calculated by the addition of CBC, RBH, and LBH. If the sum is more than 20, "High", between 16-20 then "Medium", else "Low"
"""
import pyspark.sql.functions as F

df_insurance = spark.read.table('insurance')
df_test = spark.read.table('test')

df_test_pivot = df_test\
                .filter(df_test['Test_Type'].isin('CBC','LBH','RBC'))\
                .groupBy('patient_id').pivot('Test_Type').agg(F.sum('Test_score'))

df_test_pivot.alias('test')\
    .join(df_insurance.alias('ins'),df_test_pivot['patient_id']==df_insurance['patient_id'],'inner')\
    .select('ins.patient_id','ins.Insurance_id','ins.insurance_name','test.CBC','test.LBH','test.RBC')\
    .withColumn('Risk',\
        F.when((F.col('CBC')+F.col('LBH')+F.col('RBC')) > 20,"High" )\
        .when((F.col('CBC')+F.col('LBH')+F.col('RBC')).between(16,20),"Medium")\
        .otherwise("Low"))\
    .show()

# COMMAND ----------

"""
Risk is calculated by the addition of CBC, RBH, and LBH. If the sum is more than 20, "High", between 16-20 then "Medium", else "Low"
"""
df_test_pivot = df_test\
                .filter(df_test['Test_Type'].isin('CBC','LBH','RBC'))\
                .groupBy('patient_id').pivot('Test_Type').agg(F.sum('Test_score'))

df_test_pivot.alias('test')\
    .join(df_insurance.alias('ins'),df_test_pivot['patient_id']==df_insurance['patient_id'],'inner')\
    .select('ins.patient_id','ins.Insurance_id','ins.insurance_name','test.CBC','test.LBH','test.RBC')\
    .withColumn('Risk',\
        F.when((F.col('CBC')+F.col('LBH')+F.col('RBC')) > 20,"High" )\
        .when((F.col('CBC')+F.col('LBH')+F.col('RBC')).between(16,20),"Medium")\
        .otherwise("Low"))\
    .show()
