# Databricks notebook source


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

## Create 1st DF

Data1 = [(1,'Alice'),
         (2,'Bob'),
         (3,'Charlie'),
         (4,'David'),
         (5,'Eva')]

Df1  = spark.createDataFrame(Data1,['ID','Name'])        

## Create 2nd DF

Data2 = [(1,50000),
        (2,60000),
        (3,70000),
        (4,80000)]

Df2 = spark.createDataFrame(Data2,['ID', 'Salary'])         


# COMMAND ----------

df_join_Brod = Df1.join(broadcast(Df2), Df1['ID']==Df2['ID'],'left')

# COMMAND ----------

df_join_Brod.display()