# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# COMMAND ----------

## Create 1st DF

Data1 = [('Alice','HR',1000),
         ('Bob','IT',2000),
         ('Charlie','HR', 1500),
         ('David','Finance', 2500),
         ('Eva','IT', 1800),
         ('Frank', 'Finance', 2200)]

Columns = ['Name' , 'Dept', 'Salary']        

Df1  = spark.createDataFrame(Data1,Columns)        

# Write DF Using PartitionBy

Df1.write.format('parquet').mode('overwrite')\
    .partitionBy('Dept')\
    .save('/FileStore/ApacheSpark/Output')    



# COMMAND ----------

Df1.display()

# COMMAND ----------

Df1.write.format('parquet').mode('overwrite')\
    .save('/FileStore/ApacheSpark/OutputPathWithoutPartition')    


# COMMAND ----------

Df1.display()