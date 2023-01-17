# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Optimization Best Practices Lab 1
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Understand how to use a broadcast join hint

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Introduction
# MAGIC 
# MAGIC In this Lab, we will see how to use broadcast. We create standard tables using Parquet format and run queries with and without broadcast. We will also use the Spark UI to check the use of broadcast.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %run ./Includes/Utility-Methods

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import org.apache.spark.storage.StorageLevel

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC First, let's define the tables we will query.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- Match cluster
# MAGIC SET spark.sql.shuffle.partitions = 8;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS sales;
# MAGIC DROP TABLE IF EXISTS cities;
# MAGIC DROP TABLE IF EXISTS stores;
# MAGIC 
# MAGIC CREATE TABLE sales
# MAGIC USING PARQUET
# MAGIC LOCATION "/mnt/training/global-sales/solutions/2017-fast.parquet";
# MAGIC 
# MAGIC CREATE TABLE cities
# MAGIC USING PARQUET
# MAGIC LOCATION "/mnt/training/global-sales/cities/all.parquet";
# MAGIC 
# MAGIC CREATE TABLE stores
# MAGIC USING PARQUET
# MAGIC LOCATION "/mnt/training/global-sales/retailers/all.parquet";

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's look at the cities table. Specifically, it's size.
# MAGIC 
# MAGIC If it's small enough, we should be able to broadcast it.
# MAGIC 
# MAGIC Let's start by caching the data. We are going to use a temp view to control the **RDD Name** in the Spark UI.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC spark.catalog.clearCache()
# MAGIC 
# MAGIC val cacheName = "temp_cities"
# MAGIC 
# MAGIC // Cache and materialize with the name "temp_cities"
# MAGIC cacheAs(spark.table("cities"), cacheName, StorageLevel.MEMORY_ONLY).count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Notice the use of `getRDDStorageInfo` below. This gives us programmatic access to the storage info (cache).

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val rddName = "In-memory table %s".format(cacheName)
# MAGIC val fullSize = sc.getRDDStorageInfo.filter(_.name == rddName).map(_.memSize).head
# MAGIC 
# MAGIC println("Cached Size: %,.1f MB".format(fullSize/1024.0/1024.0))
# MAGIC println("-"*80)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Is it suitable for auto-broadcasting?

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold").toLong
# MAGIC val broadcastable = fullSize < threshold
# MAGIC 
# MAGIC println("Broadcastable: %s".format(broadcastable))
# MAGIC println("Cached Size:   %,.1f MB".format(fullSize/1024.0/1024.0))
# MAGIC println("Threshold:     %,.1f MB".format(threshold/1024.0/1024.0))
# MAGIC println("-"*80)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC What if we were to limit the cities to the US only?
# MAGIC 
# MAGIC Let's see how much data that would represent.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC spark.catalog.clearCache()
# MAGIC 
# MAGIC val cacheName = "temp_us_cities"
# MAGIC val rddName = "In-memory table %s".format(cacheName)
# MAGIC 
# MAGIC val usOnlyDF = spark.table("cities").filter($"state".isNotNull)
# MAGIC cacheAs(usOnlyDF, cacheName, StorageLevel.MEMORY_ONLY).count()
# MAGIC 
# MAGIC val usSize = sc.getRDDStorageInfo.filter(_.name == rddName).map(_.memSize).head
# MAGIC 
# MAGIC val usBroadcastable = usSize < threshold
# MAGIC 
# MAGIC println("Broadcastable: %s".format(usBroadcastable))
# MAGIC println("Cached Size:   %,.1f KB".format(usSize/1024.0))
# MAGIC println("Threshold:     %,.1f MB".format(threshold/1024.0/1024.0))
# MAGIC println("-"*80)
# MAGIC 
# MAGIC // Clean up after ourselves
# MAGIC spark.catalog.clearCache()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC If we limit to the US cities only, then we should be under the auto-broadcast threshold.
# MAGIC 
# MAGIC Same query as before, this time we are limiting the cities to the US only.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC spark.table("sales")
# MAGIC      .join(spark.table("cities").filter($"state".isNotNull), "city_id")
# MAGIC      .join(spark.table("stores"), "retailer_id")
# MAGIC      .foreach(x => ())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We can add a broadcast hint to the filtered table to let Spark know that it can be broadcast.
# MAGIC 
# MAGIC The downside of this is that developer needs to know to ask for this behavior.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC spark.table("sales")
# MAGIC      .join(broadcast(spark.table("cities").filter($"state".isNotNull)), "city_id")
# MAGIC      .join(spark.table("stores"), "retailer_id")
# MAGIC      .foreach(x => ())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's remove the hint.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC spark.table("sales")
# MAGIC      .join(spark.table("cities").filter($"state".isNotNull), "city_id")
# MAGIC      .join(spark.table("stores"), "retailer_id")
# MAGIC      .foreach(x => ())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC If we give Catalyst just a little more information, it can do a better job including auto-broadcasting the filtered dataset.
# MAGIC 
# MAGIC All we need to do is re-analyze the cities table, and specifically the **city_id** and **state** columns.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC ANALYZE TABLE cities COMPUTE STATISTICS FOR COLUMNS city_id, state

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE EXTENDED cities city_id

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC And now that we have column-specific stats, let's try our query again.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC spark.table("sales")
# MAGIC      .join(spark.table("cities").filter($"state".isNotNull), "city_id")
# MAGIC      .join(spark.table("stores"), "retailer_id")
# MAGIC      .foreach(x => ())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This time it worked.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>