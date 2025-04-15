# Databricks notebook source
"""
/*
The Bloomberg terminal is the go-to resource for financial professionals, offering convenient access to a wide array of financial datasets. As a Data Analyst at Bloomberg,you have access to historical data on stock performance.

Currently, you're analyzing the highest and lowest open prices for each FAANG stock by month over the years.

For each FAANG stock, display the ticker symbol, the month and year ('Mon-YYYY') with the corresponding highest and lowest open prices (refer to the Example Output format).Ensure that the results are sorted by ticker symbol.
*/

-- 𝐓𝐚𝐛𝐥𝐞 
CREATE TABLE stockprices (stock_date DATETIME,ticker VARCHAR(10),
 openprice DECIMAL(10,2),highprice DECIMAL(10,2),
 lowprice DECIMAL(10,2),closeprice DECIMAL(10,2)
);
"""
print()

# COMMAND ----------

df = spark.read.csv(path='dbfs:/FileStore/linkedin_dataset/stock_prices.csv',header=True)
df = df.withColumn('stock_date',F.date_format(F.to_date('stock_date','MM/dd/yyyy'),'MMM-YYYY'))\
    .withColumn('openprice',F.col('openprice').cast(I.FloatType()))\
    .select('stock_date','ticker','openprice')

# df.withColumn('stock_date',F.col('stock_date').cast(I.DateType())).display()
df.display()

# COMMAND ----------

spark.conf.set('spark.sql.legacy.timeParserPolicy','LEGACY')

# COMMAND ----------

"""
The Bloomberg terminal is the go-to resource for financial professionals, offering convenient access to a wide array of financial datasets. As a Data Analyst at Bloomberg,you have access to historical data on stock performance.

Currently, you're analyzing the highest and lowest open prices for each FAANG stock by month over the years.

For each FAANG stock, display the ticker symbol, the month and year ('Mon-YYYY') with the corresponding highest and lowest open prices (refer to the Example Output format).Ensure that the results are sorted by ticker symbol.
"""

import pyspark.sql.functions as F
import pyspark.sql.window as W
import pyspark.sql.types as I

# Reading the dataset
df = spark.read.csv(path='dbfs:/FileStore/linkedin_dataset/stock_prices.csv',header=True)

df = df.withColumn('stock_date',F.date_format(F.to_date('stock_date','MM/dd/yyyy'),'MMM-YYYY'))\
    .withColumn('openprice',F.col('openprice').cast(I.FloatType()))\
    .select('stock_date','ticker','openprice')

windowSpec_asc = W.Window.partitionBy('ticker').orderBy('openprice')
windowSpec_desc = W.Window.partitionBy('ticker').orderBy(F.desc('openprice'))

df_stock_max_price = df.withColumn('row_num',F.row_number().over(windowSpec_desc))\
    .filter('row_num ==1')
    
df_stock_min_price = df.withColumn('row_num',F.row_number().over(windowSpec_asc))\
    .filter('row_num ==1')
        
df_stock_max_price.alias('max_stock')\
    .join(df_stock_min_price.alias('min_stock'),df_stock_max_price.ticker == df_stock_min_price.ticker,'inner')\
    .selectExpr('max_stock.ticker as ticker','max_stock.stock_date as highest_month','max_stock.openprice as highest_open','min_stock.stock_date as lowest_month','min_stock.openprice as lowest_open')\
    .orderBy('max_stock.stock_date').show()
