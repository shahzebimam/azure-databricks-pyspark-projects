# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format('csv').option('header',True).option('inferschema', True).load('/FileStore/ApacheSpark/MegaMart.csv').display()