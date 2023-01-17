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
# MAGIC In this notebook, you will configure multiple pipelines to consume and parse raw data as it lands in the multiplex bronze table configured earlier. The general workflow follows the pattern demonstrated in the previous lesson.
# MAGIC 
# MAGIC We'll be creating the tables `pii_silver` and `charts_silver` in our architectural diagram.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/courses/hadoop-migration/ss_diagrams/03-LAB-SS-pii_silver_charts_silver.png" width="60%" />
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC - Use various built-in functions to parse data
# MAGIC - Configure streams with various triggers and batch sizes
# MAGIC - Describe how Delta isolates transactions
# MAGIC - Deploy append-only streams to promote data from bronze to silver while parsing raw data

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The steps to build the `charts_silver` table are:
# MAGIC 
# MAGIC ![charts_silver](https://files.training.databricks.com/courses/hadoop-migration/SS_Delta_Omar/charts_silver.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The steps to build the `pii_silver` table are:
# MAGIC 
# MAGIC ![pii_silver](https://files.training.databricks.com/courses/hadoop-migration/SS_Delta_Omar/pii_silver.png)

# COMMAND ----------

# MAGIC %md
# MAGIC Declare database and set all path variables.

# COMMAND ----------

# MAGIC %run "./_module-management/ss-delta-setup"

# COMMAND ----------

# dbutils.fs.rm(Paths.silverChartsTable, True)
# dbutils.fs.rm(Paths.silverPIITable, True)
# dbutils.fs.rm(Paths.silverChartsCheckpoint, True)
# dbutils.fs.rm(Paths.silverPIICheckpoint, True)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Review Topics in `big_bronze`
# MAGIC 
# MAGIC Let's quickly review the unique topics in the bronze table. 
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> We'll work with a static query on our table as we're exploring and building out our queries, which prevents the triggering of arbitrary streams.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT DISTINCT(topic)
# MAGIC FROM big_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse Chart Status Data
# MAGIC 
# MAGIC Write a query that parses the topic `chart_status` to the following schema.
# MAGIC 
# MAGIC | field | type |
# MAGIC | --- | --- |
# MAGIC | timestamp | LONG |
# MAGIC | key | STRING |
# MAGIC | device_id | LONG | 
# MAGIC | mrn | LONG | 
# MAGIC | time | TIMESTAMP | 
# MAGIC | status | STRING |
# MAGIC 
# MAGIC Start by writing a query that displays a few rows of the JSON payload as a string, then iteratively parse the schema and apply transformations. **Ultimately, we should be able to reach our target schema with a single action per batch.**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC 
# MAGIC SELECT timestamp, 
# MAGIC   cast(key AS STRING) key, 
# MAGIC   v.device_id device_id, 
# MAGIC   v.mrn mrn, 
# MAGIC   CAST(v.time AS TIMESTAMP) time, 
# MAGIC   v.status status
# MAGIC FROM (
# MAGIC   SELECT *, from_json(cast(value AS STRING), "device_id LONG, mrn LONG, time FLOAT, status STRING") v
# MAGIC   FROM big_bronze
# MAGIC   WHERE topic = "chart_status"
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse PII Update Data
# MAGIC 
# MAGIC Write a query that parses the topic `pii_updates` to the following schema.
# MAGIC 
# MAGIC | field | type |
# MAGIC | --- | --- |
# MAGIC | timestamp | LONG |
# MAGIC | key | STRING |
# MAGIC | mrn | LONG |
# MAGIC | dob | DATE |
# MAGIC | sex | STRING | 
# MAGIC | gender | STRING | 
# MAGIC | first_name | STRING | 
# MAGIC | last_name | STRING |
# MAGIC | street_address | STRING | 
# MAGIC | zip | LONG |
# MAGIC | city | STRING | 
# MAGIC | state | STRING |
# MAGIC 
# MAGIC Start by writing a query that displays a few rows of the JSON payload as a string, then iteratively parse the schema and apply transformations. **Ultimately, we should be able to reach our target schema with a single action per batch.**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC 
# MAGIC SELECT timestamp, 
# MAGIC   CAST(key AS STRING) key, 
# MAGIC   v.mrn mrn, 
# MAGIC   to_date(v.dob, "MM/dd/yyyy") dob, 
# MAGIC   v.sex sex, 
# MAGIC   v.gender gender, 
# MAGIC   v.first_name first_name, 
# MAGIC   v.last_name last_name, 
# MAGIC   v.address.street_address street_address, 
# MAGIC   v.address.zip zip, 
# MAGIC   v.address.city city, 
# MAGIC   v.address.state state
# MAGIC FROM (
# MAGIC   SELECT *, from_json(cast(value AS STRING), 
# MAGIC                   "mrn LONG, 
# MAGIC                   dob STRING, 
# MAGIC                   sex STRING, 
# MAGIC                   gender STRING, 
# MAGIC                   first_name STRING, 
# MAGIC                   last_name STRING,
# MAGIC                   address STRUCT<
# MAGIC                     street_address: STRING, 
# MAGIC                     zip: LONG, 
# MAGIC                     city: STRING, 
# MAGIC                     state: STRING
# MAGIC                   >") v
# MAGIC   FROM big_bronze
# MAGIC   WHERE topic = "pii_updates"
# MAGIC )

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

# MAGIC %md-sandbox
# MAGIC ## Convert Queries to Streaming Source
# MAGIC 
# MAGIC We'll do a light refactor of each of our queries to point to this new streaming temp view. We'll register each query as a temporary view so that we can later pass it back to our DataStreamWriter.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> When working with the Dataframes API, we would just save this query as a new Dataframe object. We have 2 options when working in SQL. We can either:
# MAGIC 0. Use a CTAS statement and save our query as a temp view which we'll pass to a Dataframe using `spark.table()`
# MAGIC 0. Save our SQL query as a string and pass it back to the Dataframes API with `spark.sql()`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register Streaming Temp View for PII Updates
# MAGIC 
# MAGIC Ensure that you replace the static table with your streaming temp view for `big_bronze`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW TEMP_pii_silver AS (
# MAGIC SELECT timestamp, CAST(key AS STRING) key, v.mrn mrn, to_date(v.dob, "MM/dd/yyyy") dob, v.sex sex, v.gender gender, v.first_name first_name, v.last_name last_name, v.address.street_address street_address, v.address.zip zip, v.address.city city, v.address.state state
# MAGIC FROM (
# MAGIC   SELECT *, from_json(cast(value AS STRING), "mrn LONG, dob STRING, sex STRING, gender STRING, first_name STRING, last_name STRING,
# MAGIC   address STRUCT<street_address: STRING, zip: LONG, city: STRING, state: STRING>") v
# MAGIC   FROM TEMP_big_bronze
# MAGIC   WHERE topic = "pii_updates"
# MAGIC ))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Write Stream for PII Updates
# MAGIC 
# MAGIC Pass your streaming query to a `.writeStream` operation. Ensure you specify:
# MAGIC - format as `delta`
# MAGIC - trigger once
# MAGIC - the checkpoint location, `silverPIICheckpoint`
# MAGIC - the table location, `silverPIITable`
# MAGIC 
# MAGIC <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Adding `.awaitTermination()` to the end of a `trigger once` cell will block execution of subsequent cells until your batch finishes processing.

# COMMAND ----------

# ANSWER

(spark.table("TEMP_pii_silver")
.writeStream
.format("delta")
.trigger(once=True)
.option("checkpointLocation", Paths.silverPIICheckpoint)
.start(Paths.silverPIITable)).awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register Streaming Temp View for Chart Status
# MAGIC 
# MAGIC Ensure that you replace the static table with your streaming temp view for `big_bronze`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW TEMP_charts_silver AS(
# MAGIC   SELECT timestamp, cast(key AS STRING) key, v.device_id device_id, v.mrn mrn, CAST(v.time AS TIMESTAMP) time, v.status status
# MAGIC   FROM (
# MAGIC     SELECT *, from_json(cast(value AS STRING), "device_id LONG, mrn LONG, time FLOAT, status STRING") v
# MAGIC     FROM TEMP_big_bronze
# MAGIC     WHERE topic = "chart_status"
# MAGIC   )
# MAGIC )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Write Stream for Chart Status
# MAGIC 
# MAGIC Pass your streaming query to a `.writeStream` operation. Ensure that you specify:
# MAGIC - format as `delta`
# MAGIC - trigger once
# MAGIC - the checkpoint location, `silverChartsCheckpoint`
# MAGIC - the table location, `silverChartsTable`
# MAGIC 
# MAGIC <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Adding `.awaitTermination()` to the end of a `trigger once` cell will block execution of subsequent cells until your batch finishes processing.

# COMMAND ----------

# ANSWER

(spark.table("TEMP_charts_silver")
.writeStream
.format("delta")
.trigger(once=True)
.option("checkpointLocation", Paths.silverChartsCheckpoint)
.start(Paths.silverChartsTable)).awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register all Parsed Silver Tables to the Metastore
# MAGIC 
# MAGIC This will allow easy queries in future notebooks.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS charts_silver")
spark.sql("DROP TABLE IF EXISTS pii_silver")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS charts_silver
USING DELTA
LOCATION '{Paths.silverChartsTable}'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS pii_silver
USING DELTA
LOCATION '{Paths.silverPIITable}'
""")


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>