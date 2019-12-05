# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Spark Simple Notebook

# COMMAND ----------

dbutils.widgets.text('input','dbfs:/in')
dbutils.widgets.text('output','dbfs:/out')
dataInput = dbutils.widgets.get('input')
dataOutput = dbutils.widgets.get('output')

# COMMAND ----------

rdd = spark.sparkContext.textFile(dataInput)

rdd1 = rdd.map(lambda s: (len(s.split(",")), ) )

rdd1.toDF().write.mode('overwrite').csv(dataOutput)

# COMMAND ----------


