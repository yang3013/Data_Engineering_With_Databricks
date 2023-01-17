# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Trigger Once and Batch Jobs
# MAGIC 
# MAGIC This notebook contains those workloads that would likely only need to be run around once daily. Remember that in our simulated environment we're processing around 6-10 hours of records each minute. As such, we'll schedule this notebook to run every 2 minutes (to ensure that we have enough fresh data in each of our tables).
# MAGIC 
# MAGIC Note that here we're not scheduling our `charts_valid` table, as it was already scheduled independently.
# MAGIC 
# MAGIC In order to schedule this notebook, follow the instructions provided in the `batch-charts-valid` notebook, but instead schedule every 2 minutes instead of each minute. Again, we'll use our interactive cluster in place of warm pools, acknowledging that this is not best practice for production, and that these 2 scheduled jobs may cause some slight slowdown for our interactive queries due to shared resources.

# COMMAND ----------

# MAGIC %md
# MAGIC Import SQL functions for later operations.

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC Define necessary paths

# COMMAND ----------

# MAGIC %run "./_module-management/ss-delta-setup"

# COMMAND ----------

# MAGIC %md
# MAGIC # pii_current

# COMMAND ----------

class Upsert:
    def __init__(self, query, update_temp="stream_updates"):
        self.query = query
        self.update_temp = update_temp 
        
    def upsertToDelta(self, microBatchDF, batch):
        microBatchDF.createOrReplaceTempView(self.update_temp)
        microBatchDF._jdf.sparkSession().sql(self.query)

query = """
MERGE INTO pii_current a
USING stream_updates b
ON a.mrn=b.mrn
WHEN MATCHED AND 
    a.dob <> b.dob OR
    a.sex <> b.sex OR
    a.gender <> b.gender OR
    a.first_name <> b.first_name OR
    a.last_name <> b.last_name OR
    a.street_address <> b.street_address OR
    a.zip <> b.zip OR
    a.city <> b.city OR
    a.state <> b.state
THEN
  UPDATE SET 
    a.dob = b.dob,
    a.sex = b.sex,
    a.gender = b.gender,
    a.first_name = b.first_name,
    a.last_name = b.last_name,
    a.street_address = b.street_address,
    a.zip = b.zip,
    a.city = b.city,
    a.state = b.state,
    a.updated = from_unixtime(b.timestamp/1000)   
WHEN NOT MATCHED THEN
  INSERT (mrn, dob, sex, gender, first_name, last_name, street_address, zip, city, state, updated)
  VALUES (mrn, dob, sex, gender, first_name, last_name, street_address, zip, city, state, from_unixtime(timestamp/1000))
"""

streamingMerge=Upsert(query)

(spark.readStream.table("pii_silver").writeStream
    .format("delta")
    .foreachBatch(streamingMerge.upsertToDelta)
    .outputMode("update")
    .option("checkpointLocation", Paths.piiCurrentCheckpoint)
    .trigger(once=True)
    .start()).awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC # error_rate

# COMMAND ----------

week_lag = spark.sql("""
SELECT cast(date_sub(max(time), 7) AS STRING) FROM
heart_rate_silver""").collect()[0][0]

query = f"""
SELECT a.device_id, b.clinic_id, COUNT(*) errors
FROM heart_rate_silver a
INNER JOIN clinic_lookup b
ON a.device_id = b.device_id
WHERE heartrate <= 0
AND time > "{week_lag}"
GROUP BY a.device_id, b.clinic_id
"""

(spark.sql(query)
  .write
  .format("delta")
  .mode("overwrite")
  .save(Paths.errorRateTable))

# COMMAND ----------

# MAGIC %md
# MAGIC # clinic_6

# COMMAND ----------

spark.readStream.table("recordings_mrn").createOrReplaceTempView("TEMP_recordings_mrn")

query = """
SELECT c.*, d.dob, d.sex, d.gender, d.first_name, d.last_name, d.street_address, d.zip, d.city, d.state
FROM (
  SELECT b.clinic_id, a.* 
  FROM (
    SELECT mrn, device_id, DATE(time) as date, 
      MEAN(heartrate) avg_heartrate, STDDEV(heartrate) std_heartrate, 
      MAX(heartrate) max_heartrate, MIN(heartrate) min_heartrate
    FROM TEMP_recordings_mrn
    GROUP BY mrn, device_id, date
  ) a
  INNER JOIN clinic_lookup b
  ON a.device_id=b.device_id
  WHERE clinic_id = 6) c
INNER JOIN pii_current d
ON c.mrn = d.mrn
"""

(spark.sql(query)
  .writeStream
  .format("delta")
  .outputMode("complete")
  .option("checkpointLocation", Paths.clinic6Checkpoint)
  .trigger(once=True)
  .start(Paths.clinic6Table)).awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC # hourly_demographics

# COMMAND ----------

(spark.readStream.table("recordings_mrn")
.withWatermark("time", "3 hour")
.groupby("device_id", "mrn", 
         F.window("time", "1 hour", "30 minutes")
  ).agg(
      F.mean("heartrate").alias("avg_heartrate"),
      F.stddev("heartrate").alias("std_heartrate"),
      F.max("heartrate").alias("max_heartrate"),
      F.min("heartrate").alias("min_heartrate"))
   .select(F.col("window.start").alias("start"), "*").drop("window")
.createOrReplaceTempView("TEMP_hourly_avg"))

query = """
SELECT start, ROUND(months_between(start, dob)/12) age, sex, city, state, avg_heartrate, std_heartrate, max_heartrate, min_heartrate
FROM TEMP_hourly_avg a
INNER JOIN
pii_current b
ON a.mrn=b.mrn"""

(spark.sql(query)
  .writeStream
  .format("delta")
  .outputMode("complete")
  .option("checkpointLocation", Paths.hourlyDemographicsCheckpoint)
  .trigger(once=True)
  .start(Paths.hourlyDemographicsTable)).awaitTermination()


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>