# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Type 2 Slowly Changing Data
# MAGIC 
# MAGIC In this notebook, we'll create a silver table that links PII to devices through our `chart_status` dataset.
# MAGIC 
# MAGIC We'll use a Type 2 table to record this data, which will provide us the ability to match our heart rate recordings to patients through this information.
# MAGIC 
# MAGIC This operation will be executed as a batch on static data. Time permitting, we can explore additional options for trying to implement this as a stream.
# MAGIC 
# MAGIC | field | type |
# MAGIC | --- | --- |
# MAGIC | device_id | LONG |
# MAGIC | mrn | LONG |
# MAGIC | start_time | TIMESTAMP |
# MAGIC | end_time | TIMESTAMP |
# MAGIC | valid | BOOLEAN |
# MAGIC 
# MAGIC We'll be creating the table `charts_valid` in our architectural diagram.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/courses/hadoop-migration/ss_diagrams/08-charts_valid.png" width="60%" />

# COMMAND ----------

# MAGIC %md
# MAGIC Set up path and checkpoint variables (these will be used later).

# COMMAND ----------

# MAGIC %run "./_module-management/ss-delta-setup"

# COMMAND ----------

# MAGIC %md
# MAGIC Be careful about how your paths are nested if you're resetting your notebook through deletion.

# COMMAND ----------

# dbutils.fs.rm(Paths.chartsValidTable, True)

# COMMAND ----------

# MAGIC %md
# MAGIC Because we'll be triggering a shuffle in this notebook, we'll be explicit about how many partitions we want at the end of our shuffle.

# COMMAND ----------

sqlContext.setConf("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Charts Valid Table
# MAGIC 
# MAGIC In this dataset, we know that eventually we will see 2 records for each unique `device_id, mrn` pair. The earlier record indicates when a patient's recordings `START`, and a later record will indicate when these recordings `END`.
# MAGIC 
# MAGIC We'll take advantage of the `status` field to write logic that defines a join between those messages representing `START` and `END` times for our recordings. We'll define a `LEFT JOIN` so that we capture all start times, even if ends have not yet been received. 
# MAGIC 
# MAGIC We'll be working toward defining logic that results in a table with the following schema.
# MAGIC 
# MAGIC | field | type |
# MAGIC | --- | --- |
# MAGIC | device_id | LONG |
# MAGIC | mrn | LONG |
# MAGIC | start_time | TIMESTAMP |
# MAGIC | end_time | TIMESTAMP |
# MAGIC | valid | BOOLEAN |

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC To accomplish this, we'll split rows on status and join them back together on "END". 
# MAGIC 
# MAGIC ![charts_valid](https://files.training.databricks.com/courses/hadoop-migration/SS_Delta_Omar/charts_valid.png)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW TEMP_charts_valid AS (
# MAGIC   SELECT a.device_id, a.mrn, a.start_time start_time, b.end_time end_time, a.valid AND (b.valid IS NULL) valid
# MAGIC   FROM (
# MAGIC     SELECT device_id, mrn, time start_time, null end_time, true valid
# MAGIC     FROM charts_silver
# MAGIC     WHERE status = "START") a
# MAGIC   LEFT JOIN (
# MAGIC     SELECT device_id, mrn, null start_time, time end_time, false valid
# MAGIC     FROM charts_silver
# MAGIC     WHERE status = "END") b
# MAGIC   ON a.device_id = b.device_id AND a.mrn = b.mrn
# MAGIC )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Register Target Table
# MAGIC 
# MAGIC The following cell is provided to allow for easy re-setting of this demo. In production, you will _not_ want to drop your target table each time. As such, once you have this notebook working, you should comment out the following cell.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** To comment out an entire block of code, select all text and then hit "**CMD** + **/**" (Mac)

# COMMAND ----------

# spark.sql("DROP TABLE IF EXISTS charts_valid")

# spark.sql(f"""
# CREATE TABLE charts_valid
#   (device_id LONG, mrn LONG, start_time TIMESTAMP, end_time TIMESTAMP, valid BOOLEAN)
#   USING DELTA
#   LOCATION '{Paths.chartsValidTable}'
# """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Results as a Batch Overwrite
# MAGIC 
# MAGIC Our present implementation will replace the `charts_valid` table entirely with each triggered batch. What are some potential benefits and drawbacks of this approach?

# COMMAND ----------

(spark.table("TEMP_charts_valid").write
  .format("delta")
  .mode("overwrite")
  .save(Paths.chartsValidTable))

# COMMAND ----------

# MAGIC %md
# MAGIC You can now perform a query directly on your `charts_valid` table to check your results. Uncomment the `WHERE` clauses below to confirm various functionality of the logic above.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- SELECT COUNT(*)
# MAGIC -- FROM charts_valid
# MAGIC -- WHERE valid=true                        -- where record is still awaiting end time
# MAGIC -- WHERE end_time IS NOT NULL              -- where end time has been recorded
# MAGIC -- WHERE start_time IS NULL                -- where end time arrived before start time
# MAGIC -- WHERE valid=true AND end_time IS NULL   -- confirm that no entries are valid with end_time

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scheduling this Notebook
# MAGIC 
# MAGIC This notebook is designed to be scheduled as a batch job (if desired, you can schedule the version in the `Solutions` directory). Before scheduling, make sure you comment out:
# MAGIC - Any file removal commands
# MAGIC - Any commands dropping or creating databases or tables
# MAGIC - Any arbitrary actions/SQL queries that materialize results to the notebook
# MAGIC 
# MAGIC ### Scheduling Against an Interactive Cluster
# MAGIC 
# MAGIC Because our data is small and the query we run here will complete fairly quickly, we'll take advantage of our already-on compute while scheduling this notebook.
# MAGIC 
# MAGIC After defining a new job and selecting this notebook, click `Edit` on the far right of your cluster specs. On the screen that follows:
# MAGIC 
# MAGIC ![existing-cluster](https://files.training.databricks.com/images/enb/med_data/existing-cluster.png)
# MAGIC 
# MAGIC 1. Click the arrows under `Cluster Type` and choose "Existing Interactive Cluster"
# MAGIC 2. Select the cluster you've been using throughout class
# MAGIC 3. Click confirm
# MAGIC 
# MAGIC Once you're back to the jobs definition screen:
# MAGIC 
# MAGIC ![schedule-batch](https://files.training.databricks.com/images/enb/med_data/schedule-batch.png)
# MAGIC 
# MAGIC 1. Click `Edit` next to **Schedule**: None
# MAGIC 2. Change the scheduled frequency to every minute
# MAGIC 3. Click confirm
# MAGIC 
# MAGIC You can click `Run Now` if desired, or just wait until the top of the next minute for this to trigger automatically.
# MAGIC 
# MAGIC ### Best Practice: Warm Pools
# MAGIC 
# MAGIC During this demo, we're making the conscious choice to take advantage of already-on compute to reduce friction and complexity for getting our code running. In production, jobs like this one (short duration and triggered frequently) should be scheduled against [warm pools](https://docs.microsoft.com/en-us/azure/databricks/clusters/instance-pools/).
# MAGIC 
# MAGIC Pools provide you the flexibility of having compute resources ready for scheduling jobs against while removing DBU charges for idle compute. DBUs billed are for jobs rather than all-purpose workloads (which is a lower cost). Additionally, using pools instead of interactive clusters eliminates the potential for resource contention between jobs sharing a single cluster or between scheduled jobs and interactive queries.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>