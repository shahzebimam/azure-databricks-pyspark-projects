# Databricks notebook source
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

df = spark.createDataFrame(data, schema=schema)



# COMMAND ----------

# MAGIC %md
# MAGIC **Narrow Transformation**

# COMMAND ----------

df1 = df.filter(col('City')=='NewYork')

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC **Wide Transformation**

# COMMAND ----------

df2 = df.groupBy('City').agg(max(col('Age')))

# COMMAND ----------

df2.display()

# COMMAND ----------

df2.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC **Repartition Vs Coalsce**

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df = df.repartition(3)

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df = df.coalesce(1)

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df.explain()