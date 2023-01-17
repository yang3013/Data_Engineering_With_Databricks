# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Delta Optimization Best Practices Lab 3
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC 
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Understand how to combine batch and streaming data using Delta Lake

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## ![Delta Logo Tiny](https://files.training.databricks.com/courses/hadoop-migration/delta-lake-tiny-logo.png) Introduction
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/bi_architecture_db.png" style="max-height:400px"/></p>
# MAGIC </div>
# MAGIC 
# MAGIC ### Fitness Tracker Analytics
# MAGIC 
# MAGIC Let's now look at an example that combines batch and streaming data.
# MAGIC 
# MAGIC A fitness tracker is a specialized IOT device that transmits information, such as current pulse, miles walked, and calories spent in a day. This information when combined with anonymized clinical data can be used to solve many use-cases in health and insurance industries, for example:
# MAGIC * Predicting if the user may be at risk of a heart attack in the near future
# MAGIC * Predicting health risk based upon family history and current lifestyle
# MAGIC * Analyzing a user's fitness over time
# MAGIC 
# MAGIC This lesson uses Databricks-generated datasets which are then streamed to demonstrate:
# MAGIC * An end-to-end data engineering pipeline including data transformation, extraction and loading using Databricks Delta
# MAGIC * How to answer business questions by analyzing the transformed data, using a combination of SparkSQL and Databricks Visualizations

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Delta Logo Tiny](https://files.training.databricks.com/courses/hadoop-migration/delta-lake-tiny-logo.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Define some paths and clean-up.

# COMMAND ----------

fitness_batch  = "/databricks-datasets/iot-stream/data-user/"
fitness_stream = "/databricks-datasets/iot-stream/data-device/"

user_path    = userhome + "/user_delta"
tracker_path = userhome + "/tracker_delta"

dbutils.fs.rm(user_path, recurse = True)
dbutils.fs.rm(tracker_path, recurse = True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Delta Logo Tiny](https://files.training.databricks.com/courses/hadoop-migration/delta-lake-tiny-logo.png) Extract Batch and Streaming Data
# MAGIC 
# MAGIC User data is batch, and fitness tracker data is streamed.
# MAGIC 
# MAGIC Batch source is a CSV file.

# COMMAND ----------

display(dbutils.fs.ls(fitness_batch))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Streaming source consists of JSON files.

# COMMAND ----------

display(dbutils.fs.ls(fitness_stream))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Extract user data.

# COMMAND ----------

# Read CSV user data
user_data = (spark.read
              .options(header = "true", inferSchema = "true")
              .csv(fitness_batch))

display(user_data)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Delta Logo Tiny](https://files.training.databricks.com/courses/hadoop-migration/delta-lake-tiny-logo.png) Load into Delta Lake
# MAGIC 
# MAGIC First, we'll create a temporary table.

# COMMAND ----------

user_data.createOrReplaceTempView("temp_users")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Next, we'll create a table using `DELTA` from the data above.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS tracker_users")

spark.sql("CREATE TABLE tracker_users USING DELTA LOCATION '{}' AS SELECT * from temp_users".format(user_path))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's take a look at the metadata.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE DETAIL tracker_users

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now let's see the gender count.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT gender, COUNT(*)
# MAGIC FROM tracker_users
# MAGIC GROUP BY gender

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Next, let's get the schema for the stream fitness tracker data.

# COMMAND ----------

stream_file = fitness_stream + "part-00000.json.gz"

# Extract the Schema of the streaming data
fitness_sample = spark.read.json(stream_file)

display(fitness_sample)

# COMMAND ----------

tracker_schema = fitness_sample.schema

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We'll now create a streaming Dataframe.

# COMMAND ----------

from pyspark.sql.types import DateType

# Limit to 1 file per trigger for demo purposes
tracker_data = (spark
                 .readStream
                 .option("maxFilesPerTrigger", "1")
                 .schema(tracker_schema)
                 .json(fitness_stream))

tracker_data = (tracker_data
                 .withColumn("eventDate", tracker_data["timestamp"].cast(DateType())))

# COMMAND ----------

# Stop the notebook in case of a "run all"

dbutils.notebook.exit("stop")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We can start the steam.

# COMMAND ----------

(tracker_data.writeStream
  .format("delta")
  .partitionBy("eventDate")
  .option("path", tracker_path)
  .option("checkpointLocation", tracker_path + "/checkpoint/")
  .start())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now we'll create a table using `DELTA` for the tracker data.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS tracker_data")

spark.sql("CREATE TABLE tracker_data USING DELTA LOCATION '{}'".format(tracker_path))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We can analyze the data.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT eventDate, gender, sum(miles_walked) miles
# MAGIC FROM tracker_data td, tracker_users tu
# MAGIC WHERE td.user_id = tu.userid
# MAGIC GROUP BY eventDate, gender

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Stop all active streams.

# COMMAND ----------

for s in spark.streams.active:
  s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC In this lesson we discussed how to combine batch and streaming data using Delta Lake.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC [Databricks Documentation - Delta Lake](https://docs.databricks.com/delta/index.html)
# MAGIC 
# MAGIC [Databricks Documentation - Delta Lake - Optimizations](https://docs.databricks.com/delta/optimizations/index.html)
# MAGIC 
# MAGIC [Databricks Knowledgebase - Delta Lake](https://kb.databricks.com/delta/index.html)
# MAGIC 
# MAGIC [Faster SQL Queries on Delta Lake with Dynamic File Pruning](https://databricks.com/blog/2020/04/30/faster-sql-queries-on-delta-lake-with-dynamic-file-pruning.html)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>