# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Table: Clinic Report
# MAGIC 
# MAGIC 
# MAGIC This notebook will broadcast a static table with clinic_id, device_id onto the mrn_heartrate recordings and then pull those PII records for patients in 1 clinic.
# MAGIC 
# MAGIC We'll be creating the table `clinic_6` in our architectural diagram.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/courses/hadoop-migration/ss_diagrams/12-LAB-SS-clinic_6.png" width="60%" />

# COMMAND ----------

# MAGIC %run "./_module-management/ss-delta-setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Review the Contents of the Source Tables
# MAGIC 
# MAGIC Our `clinic_lookup` and `pii_current` tables are both quite small. We'll be joining these together with the `recordings_mrn` table to generate our clinic reports.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM clinic_lookup

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM pii_current

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM recordings_mrn
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM recordings_mrn

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Build Out a Query Using Static Tables
# MAGIC 
# MAGIC Write a query that gets the daily average, standard deviation, max, and min heart recording for each patient in clinic 6. Enrich this with the full current personal information for these patients.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Using the SQL built-in `DATE` method on our `time` field will provide us with the correct time-granularity for this report.
# MAGIC 
# MAGIC Our target schema for this query:
# MAGIC 
# MAGIC | field | type |
# MAGIC | --- | --- |
# MAGIC | mrn | LONG | 
# MAGIC | device_id | LONG |
# MAGIC | date | DATE|
# MAGIC | avg_heartrate | DECIMAL(9,6) |
# MAGIC | std_heartrate | DOUBLE |
# MAGIC | max_heartrate | DECIMAL(5,2) |
# MAGIC | min_heartrate | DECIMAL(5,2) |
# MAGIC | clinic_id | INTEGER |
# MAGIC | dob | DATE | 
# MAGIC | sex | STRING | 
# MAGIC | gender | STRING | 
# MAGIC | first_name | STRING | 
# MAGIC | last_name | STRING | 
# MAGIC | street_address | STRING | 
# MAGIC | zip | LONG | 
# MAGIC | city | STRING | 
# MAGIC | state | STRING | 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC 
# MAGIC Use this cell to explore and build your query

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define a Streaming Read on `recordings_mrn`
# MAGIC 
# MAGIC And then refactor your above query to point to this streaming temp view.

# COMMAND ----------

spark.readStream.table("recordings_mrn").createOrReplaceTempView("TEMP_recordings_mrn")

# COMMAND ----------

# TODO

query = """<FILL_IN>"""

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define a Streaming Write in Complete Output Mode
# MAGIC 
# MAGIC Here, we'll use `trigger once` logic. Ensure that you set the `outputMode` to `complete`.

# COMMAND ----------

# TODO

(spark.sql(query)
#  <FILL_IN>
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register Table

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS clinic_6")

spark.sql(f"""
CREATE TABLE clinic_6
USING DELTA
LOCATION '{Paths.clinic6Table}'
""")


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>