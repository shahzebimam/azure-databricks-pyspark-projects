# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# Create First Dataframe

Data1 = [(1,'Alice'),
         (2,'Bob'),
         (3,'Charlie'),
         (4,'David'),
         (5,'Eva')]

df1 = spark.createDataFrame(Data1, ['ID', 'Name'])         

# COMMAND ----------

df1.display()

# COMMAND ----------

df1 = df1.withColumn('Flag', lit('Yes'))

# COMMAND ----------

df1.cache()

# COMMAND ----------

df1.display()

# COMMAND ----------

df2 = df1.filter(col('id')==1)

# COMMAND ----------

display(df2)

# COMMAND ----------

df2.explain()