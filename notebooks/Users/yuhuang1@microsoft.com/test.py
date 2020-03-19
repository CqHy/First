# Databricks notebook source
from pyspark.sql.functions import *
from pyspark import Row
from pyspark.sql.types import *

df = spark.createDataFrame([Row(index = 1, finalArray = [0, 2, 0, 3, 1, 4, 2, 7]),Row(index = 1, finalArray = [0, 4, 4, 3, 4, 2, 2, 5])])



display(df)

# COMMAND ----------

def combineSum(array):
  sumArray = []
  i = 0
  while i < len(array):
    if i == len(array) - 1:
      sumArray.append(array[i])
    else:
      sumArray.append(array[i] + array[i+1])
    i = i + 2
  return sumArray
    

# COMMAND ----------

spark.udf.register("combineSum",combineSum)
df.createTempView("tt")
dd = spark.sql("SELECT index,combineSum(finalArray) from tt")
display(dd)

# COMMAND ----------

sumUdf = udf(combineSum)
ii = df.withColumn("finalArray", sumUdf(df["finalArray"]))
ii.show()

# COMMAND ----------

dd = df.withColumn("test", array([col("finalArray")[i] + col("finalArray")[i+1] for i in range(0, 8, 2)]))

# COMMAND ----------

dd.show()

# COMMAND ----------

ping 10.0.0.0

# COMMAND ----------

