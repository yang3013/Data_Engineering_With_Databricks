# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Spark UI Ganglia Demo
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Have a basic understanding of Ganglia

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Introduction
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/ganglia.png" style="max-height:400px"/></p>
# MAGIC </div>
# MAGIC 
# MAGIC To access the Ganglia UI, navigate to the Metrics tab on the cluster details page. CPU metrics are available in the Ganglia UI for all Databricks runtimes. GPU metrics are available for GPU-enabled clusters running Databricks Runtime 4.1 and above.
# MAGIC 
# MAGIC By default, Databricks collects Ganglia metrics every 15 minutes. To configure the collection period, set the `DATABRICKS_GANGLIA_SNAPSHOT_PERIOD_MINUTES` environment variable using an init script or in the `spark_env_vars` field in the Cluster Create API.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC First, we will get some data from the internet and save it to the driver.
# MAGIC 
# MAGIC Next, we will copy the data from the local filesystem on the driver to blob storage.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // Demo with a 3-node cluster (2 worker, 1 driver) using the Standard_DS3_v2
# MAGIC 
# MAGIC import java.net.URL
# MAGIC import java.io.File
# MAGIC import org.apache.commons.io.FileUtils
# MAGIC import java.util.concurrent.TimeUnit
# MAGIC 
# MAGIC var a = 1987
# MAGIC for (a <- 1987 to 2008) {
# MAGIC   val tmpFile = new File(s"tmp/flightdata/${a}.csv.bz2")
# MAGIC   FileUtils.copyURLToFile(new URL(s"http://www.rdatasciencecases.org/Data/Airline/${a}.csv.bz2"), tmpFile)
# MAGIC }
# MAGIC 
# MAGIC TimeUnit.SECONDS.sleep(180);
# MAGIC 
# MAGIC val flightDataPath = userhome + "/flightdata"
# MAGIC 
# MAGIC dbutils.fs.rm(flightDataPath, true)
# MAGIC dbutils.fs.mv("file:/databricks/driver/tmp/flightdata/", flightDataPath, true)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's see the data we have moved.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC display(dbutils.fs.ls(flightDataPath))

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC TimeUnit.SECONDS.sleep(180);

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC For examining several aspects of the Ganglia UI, we are going to read the data into a Dataframe, cache it and fill the cache with a `count()`.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val df = spark.read.format("csv").load(flightDataPath).cache()
# MAGIC 
# MAGIC df.count()

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC TimeUnit.SECONDS.sleep(180);

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We will now repartition on one of the fields and do a `count()`.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC df.repartition($"_c2").count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) What are we seeing?
# MAGIC 
# MAGIC ### Network
# MAGIC * We are retrieving the data from the web (spike in - green)
# MAGIC * We are moving the data from the driver's local filesystem to blob storage (spike out - blue)
# MAGIC * We are reading the data from blob storage
# MAGIC   * Reading in data is much slower than the first write
# MAGIC   * Reading in data as fast as the workers can
# MAGIC 
# MAGIC ### CPU
# MAGIC * We are retrieving the data from blob storage and starting to decompress it
# MAGIC * The load is at 66% (2 executors, 1 driver, max we can do is 66%)
# MAGIC 
# MAGIC ### Memory
# MAGIC * Rapid increase in memory usage due to cache filling

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>