# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Generate Hourly Aggregates by Demographics
# MAGIC 
# MAGIC In this notebook, we'll join our `recordings_mrn` table with the `current_pii` table to generate a de-identified report of hourly metrics.
# MAGIC 
# MAGIC We'll be creating the table `hourly_demographics` in our architectural diagram.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/courses/hadoop-migration/ss_diagrams/11-SS-hourly_demographics.png" width="60%" />

# COMMAND ----------

# MAGIC %run "./_module-management/ss-delta-setup"

# COMMAND ----------

# dbutils.fs.rm(Paths.goldCheckpointPath, True)
# dbutils.fs.rm(Paths.goldPath, True)

# COMMAND ----------

# MAGIC %md
# MAGIC We'll use the Dataframes API to define a sliding window on our `time` field. We'll import our SQL functions as `F` to give us quick access to each of our desired functions.

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC We'll define a streaming read on our `recordings_mrn` table.

# COMMAND ----------

heartrateDF = spark.readStream.table("recordings_mrn")

# COMMAND ----------

# MAGIC %md
# MAGIC By adding a watermark to our streaming query, we can manage the state information.

# COMMAND ----------

hourlyAvgDF = (heartrateDF
.withWatermark("time", "3 hour")
.groupby("device_id", "mrn", 
         F.window("time", "1 hour", "30 minutes")
  ).agg(
      F.mean("heartrate").alias("avg_heartrate"),
      F.stddev("heartrate").alias("std_heartrate"),
      F.max("heartrate").alias("max_heartrate"),
      F.min("heartrate").alias("min_heartrate"))
   .select(F.col("window.start").alias("start"), "*").drop("window"))

# COMMAND ----------

# MAGIC %md
# MAGIC We can use this streaming aggregate Dataframe to define a temp view so that we can continue our operations in SQL.

# COMMAND ----------

hourlyAvgDF.createOrReplaceTempView("TEMP_hourly_avg")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Review Patient Data

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM pii_current

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write a Query that Removes PII
# MAGIC 
# MAGIC Write an `INNER JOIN` between your `TEMP_hourly_avg` defined above and your `pii_current` table.
# MAGIC 
# MAGIC By removing MRN, device_id, DOB, and zip code, we will be left with demographic information that prevents patients from being identified (or recordings from being associated back with patients).
# MAGIC 
# MAGIC Your target schema:
# MAGIC 
# MAGIC | field | type |
# MAGIC | --- | --- |
# MAGIC | start | TIMESTAMP |
# MAGIC | age | DOUBLE |
# MAGIC | sex | STRING |
# MAGIC | city | STRING |
# MAGIC | state | STRING |
# MAGIC | avg_heartrate | DECIMAL(9,6) |
# MAGIC | std_heartrate | DOUBLE |
# MAGIC | max_heartrate | DECIMAL(5,2) |
# MAGIC | min_heartrate | DECIMAL(5,2) |

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW hourly_demographics AS (
# MAGIC   SELECT start, ROUND(months_between(start, dob)/12) age, sex, city, state, avg_heartrate, std_heartrate, max_heartrate, min_heartrate
# MAGIC   FROM TEMP_hourly_avg a
# MAGIC   INNER JOIN
# MAGIC   pii_current b
# MAGIC   ON a.mrn=b.mrn
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write in Complete Mode

# COMMAND ----------

(spark.table("hourly_demographics")
  .writeStream
  .format("delta")
  .outputMode("complete")
  .option("checkpointLocation", Paths.hourlyDemographicsCheckpoint)
  .trigger(once=True)
  .start(Paths.hourlyDemographicsTable))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register Delta Table

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS hourly_demographics")

spark.sql(f"""
CREATE TABLE hourly_demographics
USING DELTA
LOCATION '{Paths.hourlyDemographicsTable}'
""")


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>