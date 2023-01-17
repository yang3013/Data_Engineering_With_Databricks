# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Table: Clinic Device Lookup
# MAGIC 
# MAGIC Each device is assigned to a specific clinic. We'll use these clinic IDs in a later step to generate daily reports for individual clinics that include all the patients present that day.
# MAGIC 
# MAGIC We'll be creating the table `clinic_lookup` in our architectural diagram. For simplicity, we are treating this as a static table in this demo.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/courses/hadoop-migration/ss_diagrams/06-LAB-clinic_lookup.png" width="60%" />
# MAGIC 
# MAGIC 
# MAGIC Delta Lake supports `COPY INTO` syntax that allows users to easily load data from many different static sources into their Delta Lakes. [Documentation here](https://docs.databricks.com/spark/latest/spark-sql/language-manual/copy-into.html).

# COMMAND ----------

# MAGIC %md
# MAGIC Begin by running the following cell to set up relevant databases and paths.

# COMMAND ----------

# MAGIC %run "./_module-management/ss-delta-setup"

# COMMAND ----------

clinic_device_csv = "/mnt/med-recordings/clinic-devices.csv"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC After reviewing the documentation from above, see if you can define a query using the `COPY INTO` syntax supported by Delta.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> You can choose to create your target table before or after writing the files, but do ensure that you register a `clinic_lookup` table to the Hive Metastore.

# COMMAND ----------

# ANSWER

clinic_device_csv = "/mnt/med-recordings/clinic-devices.csv"

spark.sql(f"""
COPY INTO delta.`{Paths.clinicLookupTable}`
FROM '{clinic_device_csv}'
FILEFORMAT = CSV
FORMAT_OPTIONS('inferSchema'='true','header'='true')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS clinic_lookup
USING DELTA 
LOCATION '{Paths.clinicLookupTable}'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Note that we see a new directory nested within the `_delta_log`.

# COMMAND ----------

dbutils.fs.ls(Paths.clinicLookupTable + "/_delta_log/")

# COMMAND ----------

# MAGIC %md
# MAGIC This `_copy_into_log` stores information that ensures idempotency of this operation.

# COMMAND ----------

dbutils.fs.ls(Paths.clinicLookupTable + "/_delta_log/_copy_into_log/")

# COMMAND ----------

# MAGIC %md
# MAGIC Our present dataset has 500 unique devices spread out across 6 clinics. Feel free to explore this data below.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM clinic_lookup

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>