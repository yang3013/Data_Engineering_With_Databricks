# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Error Logging
# MAGIC 
# MAGIC This notebook is designed to populate a table that will be used to identify high error rates in devices over the past 7 days.
# MAGIC 
# MAGIC We'll be creating the table `error_rate` in our architectural diagram.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/courses/hadoop-migration/ss_diagrams/10-LAB-error_rate.png" width="60%" />

# COMMAND ----------

# MAGIC %run "./_module-management/ss-delta-setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Identify Minimum Date
# MAGIC 
# MAGIC In order to programmatically pass a predicate for our time field, we'll need to identify what our minimum threshold is. Write a query below that returns the date that is 7 days before the max time seen in `heart_rate_silver`. Register this as a Python string variable with the format `YYYY-MM-DD`.

# COMMAND ----------

# ANSWER

week_lag = spark.sql("""
SELECT cast(date_sub(max(time), 7) AS STRING) FROM
heart_rate_silver""").collect()[0][0]

week_lag

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join Recording Errors with Clinic Data
# MAGIC 
# MAGIC Query all recordings <= 0 from the past week, and get the weekly count for errors on each device. You will want to pass the date variable defined in the previous step back into your query.
# MAGIC 
# MAGIC Your target schema for this query is as follows.
# MAGIC 
# MAGIC | field | type |
# MAGIC | --- | --- |
# MAGIC | device_id | long |
# MAGIC | clinic_id | int |
# MAGIC | errors | int |

# COMMAND ----------

# ANSWER

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW TEMP_error_rate AS (
  SELECT a.device_id, b.clinic_id, COUNT(*) errors
  FROM heart_rate_silver a
  INNER JOIN clinic_lookup b
  ON a.device_id = b.device_id
  WHERE heartrate <= 0
  AND time > "{week_lag}"
  GROUP BY a.device_id, b.clinic_id
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Perform a batch overwrite to replace the `error_rate` gold table.

# COMMAND ----------

# ANSWER

(spark.table("TEMP_error_rate")
  .write
  .format("delta")
  .mode("overwrite")
  .save(Paths.errorRateTable))

# COMMAND ----------

# MAGIC %md
# MAGIC Ensure that you run the following cell the first time you complete this notebook (or if you change the path where you're saving the `error_rate` table).

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS error_rate")

spark.sql(f"""
CREATE TABLE error_rate
USING DELTA
LOCATION '{Paths.errorRateTable}'
""")


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>