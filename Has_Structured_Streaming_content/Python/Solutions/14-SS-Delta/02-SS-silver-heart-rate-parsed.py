# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Level: Parsing Raw Data
# MAGIC 
# MAGIC In this notebook, you will configure a query to consume and parse raw data from a single topic as it lands in the multiplex bronze table configured in the last lesson. The following lab will replicate this process for two additional topics.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC - Use various built-in functions to parse data
# MAGIC - Configure streams with various triggers and batch sizes
# MAGIC - Describe how Delta isolates transactions
# MAGIC - Deploy append-only streams to promote data from bronze to silver while parsing raw data

# COMMAND ----------

# MAGIC %md
# MAGIC Declare database and set all path variables.

# COMMAND ----------

# MAGIC %run "./_module-management/ss-delta-setup"

# COMMAND ----------

# dbutils.fs.rm(Paths.silverCheckpointPath, True)
# dbutils.fs.rm(Paths.silverPath, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define a Batch Read
# MAGIC 
# MAGIC Before building our streams, we'll start with a static view of our data. Working with static data can be easier during interactive development as no streams will be triggered. Because we're working with Delta Lake as our source, we'll still get the most up-to-date version of our table each time we execute a query.
# MAGIC 
# MAGIC If you're working with SQL, you can just directly query the table registered in the previous lesson `big_bronze`. Python and Scala users can easily create a Dataframe from a registered table.

# COMMAND ----------

batchDF = spark.table("big_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC Delta Lake stores our schema information. Let's print it out, just to make sure we remember.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE big_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC Preview your data. Note that the vast majority of your records will be from heart rate monitors.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM big_bronze
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC There are multiple topics being ingested. So, we'll need to define logic for each of these topics separately.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT DISTINCT(topic)
# MAGIC FROM big_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC We'll cast our binary fields as strings, as this will allow us to manually review their contents.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT cast(key AS STRING), cast(value AS STRING)
# MAGIC FROM big_bronze
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse Heart Rate Recordings
# MAGIC 
# MAGIC Let's start by defining logic to parse our heart rate recordings. We'll write this logic against our static data. Note that there are some [unsupported operations](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#unsupported-operations) in Structured Streaming, so we may need to refactor some of our logic if we don't build our current queries with these limitations in mind.
# MAGIC 
# MAGIC Together, we'll iteratively develop a single query that parses our `heart_rate_monitor` topic to the following schema.
# MAGIC 
# MAGIC | field | type |
# MAGIC | --- | --- |
# MAGIC | timestamp | LONG |
# MAGIC | key | STRING |
# MAGIC | device_id | LONG | 
# MAGIC | time | TIMESTAMP | 
# MAGIC | heartrate | DECIMAL(5,2) |
# MAGIC 
# MAGIC We'll be creating the table `heart_rate_silver` in our architectural diagram.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/courses/hadoop-migration/ss_diagrams/02-SS-heart_rate_silver.png" width="60%" />

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The steps we will use to accomplish this are: 
# MAGIC 
# MAGIC ![heart_rate_silver](https://files.training.databricks.com/courses/hadoop-migration/SS_Delta_Omar/heart_rate_silver.png)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC 
# MAGIC SELECT timestamp, cast(key as STRING) key, v.device_id device_id, v.time time, v.heartrate heartrate
# MAGIC FROM (
# MAGIC   SELECT *, from_json(cast(value AS STRING), "device_id LONG, time TIMESTAMP, heartrate DECIMAL(5,2)") v
# MAGIC   FROM big_bronze
# MAGIC   WHERE topic = "heart_rate_monitor")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define a Streaming Read
# MAGIC 
# MAGIC We can define a streaming read directly against our Delta table. By registering this back as a temporary view, we will have access to writing our logic using SparkSQL on streaming data.

# COMMAND ----------

(spark.readStream
  .table("big_bronze")
  .createOrReplaceTempView("TEMP_big_bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert Queries to Streaming Source
# MAGIC 
# MAGIC We'll do a light refactor of each of our queries to point to this new streaming temp view. We'll register each query as a temporary view so that we can later pass it back to our DataStreamWriter.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW TEMP_heart_rate_silver AS
# MAGIC (SELECT timestamp, cast(key as STRING) key, v.device_id device_id, v.time time, v.heartrate heartrate
# MAGIC FROM (
# MAGIC   SELECT *, from_json(cast(value AS STRING), "device_id LONG, time TIMESTAMP, heartrate DECIMAL(5,2)") v
# MAGIC   FROM TEMP_big_bronze
# MAGIC   WHERE topic = "heart_rate_monitor"))

# COMMAND ----------

# MAGIC %md
# MAGIC Remember that we can use `spark.table` to gain quick access to Dataframe operations on registered views and tables. Let's check out our schema.

# COMMAND ----------

spark.table("TEMP_heart_rate_silver").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Using trigger once logic, we can process all new data from our source as a single batch.
# MAGIC 
# MAGIC :NOTE: Here we're _sharing_ a checkpoint with what will later be our streaming jobs. This allows us to use the following single batch to catch up with our stream as a single batch (assuming we provision a large enough cluster to handle the backlog of data). This assumes that **our streaming job is not actively running**, as two concurrent streams cannot share a single checkpoint.

# COMMAND ----------

# ANSWER
(spark.table("TEMP_heart_rate_silver")
.writeStream
.format("delta")
.trigger(once=True)
.option("checkpointLocation", Paths.silverRecordingsCheckpoint)
.start(Paths.silverRecordingsTable))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Parsed Heart Rate Recordings Silver Table to the Metastore
# MAGIC 
# MAGIC In future workflows, we will be able to refer to these files directly as a table rather than passing the filepath.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS heart_rate_silver")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS heart_rate_silver
USING DELTA
LOCATION '{Paths.silverRecordingsTable}'
""")


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>