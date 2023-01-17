# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Migrating User Defined Functions from Hive
# MAGIC 
# MAGIC Hive user defined functions (UDFs) can be imported and used in Spark using the following process:
# MAGIC 1. Create a JAR file that includes dependencies (sometimes called an uber JAR, Fat JAR, or Assembly JAR) 
# MAGIC 1. Upload the JAR to Databricks
# MAGIC 1. Register the function from the JAR as a UDF
# MAGIC 
# MAGIC Hive UDFs are functions written in Java or Scala that typically will complete functions not natively supported in Hive. Spark has native support for UDFs as well, so the migration of this code is extremely simple.
# MAGIC 
# MAGIC This lesson will use a simple UDF to demonstrate this process.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lessons, you should be able to:
# MAGIC * Import a UDF from Hive
# MAGIC * Register a UDF for use in a SQL command
# MAGIC * Describe the benefits of refactoring UDFs to native Spark

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Using a Hive UDF from a JAR
# MAGIC 
# MAGIC [Registering a function from a JAR](https://docs.databricks.com/spark/latest/spark-sql/language-manual/create-function.html) is very straightforward. Here, we'll register our Hive UDF as a temporary function using the following syntax:
# MAGIC 
# MAGIC ```CREATE TEMPORARY FUNCTION function_name AS class_name USING JAR file_uri```
# MAGIC 
# MAGIC Our JAR file presently resides in a cloud-based object store with public read/list permissions. As such, we can just use the URL for this file as the `file_uri`.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> It is not possible to write Java code directly in Databricks notebooks. Here, we are importing a class from a JAR of compiled Java code. If your Hive UDFs are written in Scala, you can [define a Hive UDF in notebooks](https://kb.databricks.com/data/hive-udf.html) so that your UDF and Spark code can be easily coversioned.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create the Temporary Function
# MAGIC 
# MAGIC Here we'll demo a very simple UDF that just accepts a string and casts it to lowercase.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TEMPORARY FUNCTION IF EXISTS tolower;
# MAGIC 
# MAGIC CREATE TEMPORARY FUNCTION tolower AS "com.databricks.training.examples.ExampleUDF" USING JAR "https://files.training.databricks.com/courses/hadoop-migration/CombinedUDF-1.0-SNAPSHOT.jar";

# COMMAND ----------

# MAGIC %md
# MAGIC Now we use the function.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT tolower("Hi THERE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge: Register a Hive UDF
# MAGIC 
# MAGIC A second class is also present in this JAR called `ReverseUDF`. Adapt the code above to register this class for use in Spark SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC 
# MAGIC DROP TEMPORARY FUNCTION IF EXISTS myreverse;
# MAGIC 
# MAGIC -- FILL-IN
# MAGIC 
# MAGIC SELECT myreverse("HI THERE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC In this notebook you learned how easy it is to migrate Hive UDFs to Spark for use with Spark SQL.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC * [New Builtin and Higher Order Functions](https://databricks.com/blog/2018/11/16/introducing-new-built-in-functions-and-higher-order-functions-for-complex-data-types-in-apache-spark.html)
# MAGIC * [Hive UDFs](https://kb.databricks.com/data/hive-udf.html)
# MAGIC * [Pandas UDF for PySpark](https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html)
# MAGIC * [Spark Hive UDF](https://github.com/bmc/spark-hive-udf)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>