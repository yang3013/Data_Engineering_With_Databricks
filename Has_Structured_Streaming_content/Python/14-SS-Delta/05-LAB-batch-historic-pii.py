# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Migrate Historic PII Values
# MAGIC 
# MAGIC For simplicity in this demo, we have provided a static file that contains all the current valid patient information prior to the beginning of our data collection.
# MAGIC 
# MAGIC We'll be creating the table `pii_current` in our architectural diagram. In a moment, we'll define a streaming query to update this table.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/courses/hadoop-migration/ss_diagrams/05-LAB-pii_current.png" width="60%" />

# COMMAND ----------

# MAGIC %run "./_module-management/ss-delta-setup"

# COMMAND ----------

source = "/mnt/med-recordings/patient-export.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC Below is the schema for our exported data.

# COMMAND ----------

schema = "mrn LONG, dob DATE, sex STRING, gender STRING, first_name STRING, last_name STRING, street_address STRING, zip LONG, city STRING, state STRING, updated LONG"

# COMMAND ----------

# MAGIC %md
# MAGIC We'll read it to a temp view to explore and ensure that our schema is correct.
# MAGIC 
# MAGIC Write a static read for a CSV that registers a temp view.

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md
# MAGIC Use SQL to query your temp view.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC 
# MAGIC Use this cell to explore and build your query

# COMMAND ----------

# MAGIC %md
# MAGIC Note that our `updated` field is in unix time. We'll convert this to a timestamp.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC 
# MAGIC Use this cell to explore and build your query

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Use the query above to create an unmanaged silver table named `pii_current` with a `CREATE TABLE AS SELECT` (CTAS) statement. Ensure that you match the following schema.
# MAGIC 
# MAGIC | field | type |
# MAGIC | --- | --- |
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
# MAGIC | updated | TIMESTAMP |
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Ensure that you created an unmanaged table using the path variable defined above, `piiCurrentPath`.

# COMMAND ----------

# TODO

spark.sql("DROP TABLE IF EXISTS pii_current")

spark.sql(f"""CREATE TABLE pii_current
<FILL_IN>
""")


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>