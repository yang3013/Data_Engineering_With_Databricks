# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Current PII Table
# MAGIC 
# MAGIC In this notebook, we'll go through the process of implementing a Slowly Changing Dimension (SCD) Type 1 table to store only the most current demographic information for our patients.
# MAGIC 
# MAGIC We'll be updating the table `pii_current` in our architectural diagram.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/courses/hadoop-migration/ss_diagrams/07-SS-pii_current.png" width="60%" />

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Concept Review: Type 0/1/2 Tables
# MAGIC 
# MAGIC In this notebook, we'll demonstrate a Type 1 SCD table. Generally, when we're thinking about Delta tables, we should make conscious choices about type as we design our architecture.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Types 0, 1, & 2 are only a few types of [SCD tables](https://en.wikipedia.org/wiki/Slowly_changing_dimension). Most tables in your Delta architecture will be of these types. For fact tables, we should always seek to create type 0.
# MAGIC 
# MAGIC ### Type 0
# MAGIC - No changes allowed
# MAGIC - Tables are either static or append only
# MAGIC - Examples: static lookup tables, append-only fact tables
# MAGIC 
# MAGIC ### Type 1
# MAGIC - Overwrite
# MAGIC - No history is maintained
# MAGIC - May contain recording of when record was entered, but not previous values
# MAGIC - Useful when you only care about current values rather than historic comparisons
# MAGIC - Example: valid customer mailing address
# MAGIC 
# MAGIC ### Type 2
# MAGIC - Add a new row; mark old row as obsolete
# MAGIC - Strong history is maintained
# MAGIC - Several approaches, but will generally include version numbers, beginning/end timestamps, and/or a `valid` flag
# MAGIC - Example: tracking product price changes over time

# COMMAND ----------

# MAGIC %run "./_module-management/ss-delta-setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review Data

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM pii_silver
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM pii_current
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC Type 1 tables store only the most recent version of the data for each entry. During each visit, the patient is asked to confirm their personal information. Upstream processes capture changes to our Electronic Health Records as messages. These include updated patient information and new patient information, but also may just be messages confirming that none of this information has changed.
# MAGIC 
# MAGIC The most frequent changes will be to addresses as people move. Both first and last names may change for a variety of reasons, as may gender identity.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Define UPSERT Query
# MAGIC 
# MAGIC Write a query that checks if a patient with a given MRN exists. If yes, check if any of their personal information has changed. When a change is detected, update the entry, including the timestamp for the `updated` field. Use the timestamp from the Kafka message to fill in this value.
# MAGIC 
# MAGIC If the patient is not presently in the system, insert the new patient.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> We'll be configuring this to run as a stream in a later step. Your upsert logic should be robust enough that re-processing the same records will not result in duplicating or overwriting any unchanged data.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ![pii_current_upsert](https://files.training.databricks.com/courses/hadoop-migration/SS_Delta_Omar/pii_current_upsert.png)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC 
# MAGIC Use this cell to explore and build your query

# COMMAND ----------

# MAGIC %md
# MAGIC We can see how many records were changed by reviewing the `operationMetrics` when we run `DESCRIBE HISTORY` on our target table.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY pii_current

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert Helper Class
# MAGIC 
# MAGIC The following Python class is provided for you. This will allow you to UPSERT queries to your DataStreamWriter using a custom [`foreachBatch` method](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#foreachbatch).
# MAGIC 
# MAGIC :NOTE: When defining your class, you'll pass a SQL query. You can also pass an optional argument with the name of the temporary view used in your streaming query. If you are running multiple concurrent streaming updates in a single notebook, you will need to ensure that you define a different `update_temp` view for each.

# COMMAND ----------

class Upsert:
    def __init__(self, query, update_temp="stream_updates"):
        self.query = query
        self.update_temp = update_temp 
        
    def upsertToDelta(self, microBatchDF, batch):
        microBatchDF.createOrReplaceTempView(self.update_temp)
        microBatchDF._jdf.sparkSession().sql(self.query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge Into
# MAGIC 
# MAGIC Assign your UPSERT query from above as a Python string. Use the default temp view name `stream_updates` from the `Upsert` class defined in the last step.

# COMMAND ----------

# TODO

query = """<COPY YOUR QUERY FROM ABOVE HERE>"""

# COMMAND ----------

# MAGIC %md
# MAGIC Instantiate your `Upsert` class with this query.

# COMMAND ----------

streamingMerge=Upsert(query)

# COMMAND ----------

# MAGIC %md
# MAGIC Now pass the class method `upsertToDelta` to `foreachBatch` in a streaming write with trigger once.

# COMMAND ----------

# TODO


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>