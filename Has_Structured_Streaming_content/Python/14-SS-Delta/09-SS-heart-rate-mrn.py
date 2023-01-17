# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Fast Changing Facts with Stream-Static Join
# MAGIC 
# MAGIC In this lesson, you will join parsed heart rate recordings with data from the `charts_valid` table to create an append-only table.
# MAGIC 
# MAGIC We'll be creating the table `recordings_mrn` in our architectural diagram.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/courses/hadoop-migration/ss_diagrams/09-SS-recordings_mrn.png" width="60%" />

# COMMAND ----------

# MAGIC %run "./_module-management/ss-delta-setup"

# COMMAND ----------

# MAGIC %md
# MAGIC Set up your streaming temp view. Note that we will only be streaming from **one** of our tables. Our SCD Type 2 table is no longer streamable as it breaks that requirement of an ever-appending source for Structured Streaming. However, when performing a stream-static join with a Delta table, each batch will confirm that the newest version of the static Delta table is being used.

# COMMAND ----------

spark.readStream.table("heart_rate_silver").createOrReplaceTempView("TEMP_heart_rate_silver")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Perform Stream-Static Join to Add MRN to Heart Rate Recordings
# MAGIC 
# MAGIC Below we'll configure our query to join our stream to our `charts_valid` table. Matching on `device_id` where our recording time falls between a start and end time will allow us access to our medical record numbers, which will be necessary to ID our patients. 
# MAGIC 
# MAGIC Importantly, our devices occasionally send messages with negative recordings, which represent a potential error in the recorded values. We'll need to define predicate conditions to ensure that only positive recordings are processed. 
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> We'll create a dashboard to monitor errors in heart rate recordings (negative values) in a later step.
# MAGIC 
# MAGIC Run the following cell to see how many errors are presently in your parsed recordings.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*) 
# MAGIC FROM heart_rate_silver
# MAGIC WHERE heartrate <= 0

# COMMAND ----------

# MAGIC %md
# MAGIC Target schema for our join.
# MAGIC 
# MAGIC | field | type |
# MAGIC | --- | --- |
# MAGIC | mrn | LONG |
# MAGIC | device_id | LONG | 
# MAGIC | time | TIMESTAMP | 
# MAGIC | heartrate | DECIMAL(5,2) |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC 
# MAGIC Use this cell to explore and build your query

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Stream in Append Mode
# MAGIC 
# MAGIC Below, we'll use our streaming temp view from above to insert new values into our `recordings_mrn` table.

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register our Delta Table

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS recordings_mrn")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS recordings_mrn
USING DELTA
LOCATION '{Paths.recordingsMRNTable}'
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM recordings_mrn

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>