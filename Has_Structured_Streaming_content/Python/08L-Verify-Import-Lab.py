# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Verify Import Lab
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Verify that both SparkSQL and Hive return the same results

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ##Verify Import
# MAGIC 
# MAGIC When the devops team dumped the Hive data, they also ran a series of queries against the Hive tables and logged the results. 
# MAGIC 
# MAGIC The queries and results were placed in a JSON file and written to the data dump directory.
# MAGIC 
# MAGIC The purpose of this lab is to verify one or more of these queries.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Set a variable to point to the JSON file

# COMMAND ----------

pathToJSON = userhome + "/hiveData/HiveDataDump/queries_and_results.json"
print(pathToJSON)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Verify the file is as expected

# COMMAND ----------

print(dbutils.fs.head(pathToJSON))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Create a Dataframe from the JSON file
# MAGIC 
# MAGIC Using ```spark.read.json```, or ```spark.read.format("json")``` create a Dataframe from the JSON file.

# COMMAND ----------

# TODO

YOUR CODE HERE

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Pick a number for your random seed
# MAGIC 
# MAGIC This is to allow each participant to test a different query.
# MAGIC 
# MAGIC In a migration of a production environment from Hadoop to Databricks, you would test more than one query, and queries that would catch errors across all records such as sum/avg/min/max etc. but for the purposes of the lab you will test one randomly selected query.

# COMMAND ----------

# TODO

yourseed = <any_integer_here>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Randomly select a query to validate
# MAGIC 
# MAGIC The output of the cell below will be the query as run on Hive, and the results that Hive generated. 
# MAGIC 
# MAGIC Take the query and run it against a table in SparkSQL and verify the results match. 
# MAGIC 
# MAGIC **Warning**: Order by Rand is an expensive operation on large tables.
# MAGIC 
# MAGIC This table is small, so sort is done in memory.

# COMMAND ----------

from pyspark.sql.functions import rand

# import pyspark.sql.functions.rand

display(df.orderBy(rand(seed=yourseed)).limit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Run the query above against your table and verify that you get the same result

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC 
# MAGIC Your query here

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Congratulations you are done with the lab! 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC A data migration should verify that no data was altered during the move.
# MAGIC 
# MAGIC Badly formatted records may show up as:
# MAGIC * Null records
# MAGIC * Misrepresentation of Nulls vs empty strings
# MAGIC * Lost Partitions
# MAGIC * Hive buckets failing as they differ from Spark buckets

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>