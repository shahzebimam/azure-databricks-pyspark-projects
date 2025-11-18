# Databricks notebook source
spark

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

data = [('Alice',25,'NewYork'),
        ('Bob', 30, 'San Francisco'),
        ('Charlie',35, 'Chicago')]

schema = StructType([
        StructField('Name', StringType(), True),
        StructField('Age', StringType(), True),
        StructField('City', StringType(), True)])

df_New = spark.createDataFrame(data, schema=schema)



# COMMAND ----------

df_New = df_New.filter(col('City')=='NewYork')

# COMMAND ----------

df_New = df_New.select(col('City'))

# COMMAND ----------

df_New.display()

# COMMAND ----------

df_New.explain()