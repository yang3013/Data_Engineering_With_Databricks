# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Once Daily Batch
# MAGIC 
# MAGIC We'll use this notebook as a framework to launch a single batch job on each of our tables in the order of dependencies as mapped out in our architecture below. In our normal day-to-day operations, this will allow us to catch up on any data that arrives while our stream is down overnight.
# MAGIC 
# MAGIC **For the purposes of our class, we will re-execute queries completed during day 1 of this presentation using `trigger once` so that we can pick up with the development of our pipeline.** You should comment out those queries we have not yet run to ensure you don't get ahead of the class delivery.
# MAGIC 
# MAGIC ![Delta Architecture](https://files.training.databricks.com/images/enb/med_data/med_data_full_arch.png)

# COMMAND ----------

# MAGIC %md
# MAGIC Import SQL functions and define paths.

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %run "./_module-management/ss-delta-setup"

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingest to Bronze

# COMMAND ----------

def kafkaToBigBronze(source, sink, checkpoint, once=False, processing_time="5 seconds"):
    kafkaSchema = "timestamp LONG, topic STRING, key BINARY, value BINARY"

    if once == True:
        (spark.readStream
          .format("parquet")
          .schema(kafkaSchema)
          .load(source)
          .writeStream
          .format("delta")
          .option("checkpointLocation", checkpoint)
          .trigger(once=True)
          .start(sink)).awaitTermination()
    else:
        (spark.readStream
          .format("parquet")
          .schema(kafkaSchema)
          .option("maxFilesPerTrigger", 8)
          .load(source)
          .writeStream
          .format("delta")
          .option("checkpointLocation", checkpoint)
          .trigger(processingTime=processing_time)
          .start(sink))
        
kafkaToBigBronze(Paths.sourceDir, Paths.bronzeTable, Paths.bronzeCheckpoint, once=True)

# COMMAND ----------

# MAGIC %md
# MAGIC # heart_rate_silver, chart_silver, pii_silver

# COMMAND ----------

class ParseBronze:
    def __init__(self, Path_class, once=False, processing_time="5 seconds", source_table="big_bronze"):
        self.once = once
        self.processing_time = processing_time
        self.source_table = source_table
        self.streaming_source = f"TEMP_{source_table}"
        self.Paths=Path_class
        
    def write_query(self, query, sink, checkpoint, processing_time):
        if self.once == True:
            (spark.sql(query)
              .writeStream
              .format("delta")
              .trigger(once=True)
              .option("checkpointLocation", checkpoint)
              .start(sink)).awaitTermination()
        else:
            (spark.sql(query)
              .writeStream
              .format("delta")
              .trigger(processingTime=processing_time)
              .option("checkpointLocation", checkpoint)
              .start(sink))
            
    def heart_rate_monitor(self, sink, checkpoint):
        query = f"""
          SELECT timestamp, cast(key as STRING) key, v.device_id device_id, v.time time, v.heartrate heartrate
          FROM (
            SELECT *, from_json(cast(value AS STRING), "device_id LONG, time TIMESTAMP, heartrate DECIMAL(5,2)") v
            FROM {self.streaming_source}
            WHERE topic = "heart_rate_monitor")
          """
        self.write_query(query, sink, checkpoint, processing_time=self.processing_time)

    def chart_status(self, sink, checkpoint):
        query = f"""
          SELECT timestamp, cast(key AS STRING) key, v.device_id device_id, v.mrn mrn, CAST(v.time AS TIMESTAMP) time, v.status status
          FROM (
            SELECT *, from_json(cast(value AS STRING), "device_id LONG, mrn LONG, time FLOAT, status STRING") v
            FROM {self.streaming_source}
            WHERE topic = "chart_status")
        """
        self.write_query(query, sink, checkpoint, processing_time="30 seconds")

    def pii_silver(self, sink, checkpoint):
        query = f"""
          SELECT timestamp, 
            CAST(key AS STRING) key, 
            v.mrn mrn, 
            to_date(v.dob, "MM/dd/yyyy") dob, 
            v.sex sex, 
            v.gender gender, 
            v.first_name first_name, 
            v.last_name last_name, 
            v.address.street_address street_address, 
            v.address.zip zip, 
            v.address.city city, 
            v.address.state state
          FROM (
            SELECT *, from_json(cast(value AS STRING), 
                            "mrn LONG, 
                            dob STRING, 
                            sex STRING, 
                            gender STRING, 
                            first_name STRING, 
                            last_name STRING,
                            address STRUCT<
                              street_address: STRING, 
                              zip: LONG, 
                              city: STRING, 
                              state: STRING
                            >") v
            FROM {self.streaming_source}
            WHERE topic = "pii_updates"
          )"""

        self.write_query(query, sink, checkpoint, processing_time="90 seconds")

        
        
    def start(self):
        spark.readStream.table(self.source_table).createOrReplaceTempView(self.streaming_source)
        self.heart_rate_monitor(self.Paths.silverRecordingsTable, self.Paths.silverRecordingsCheckpoint)
        self.chart_status(self.Paths.silverChartsTable, self.Paths.silverChartsCheckpoint)
        self.pii_silver(self.Paths.silverPIITable, self.Paths.silverPIICheckpoint)
        
ParseToSilver = ParseBronze(Paths, once=True)
ParseToSilver.start()

# COMMAND ----------

# MAGIC %md
# MAGIC # charts_valid

# COMMAND ----------

query = """
SELECT a.device_id, a.mrn, a.start_time start_time, b.end_time end_time, a.valid AND (b.valid IS NULL) valid
FROM (
  SELECT device_id, mrn, time start_time, null end_time, true valid
  FROM charts_silver
  WHERE status = "START") a
LEFT JOIN (
  SELECT device_id, mrn, null start_time, time end_time, false valid
  FROM charts_silver
  WHERE status = "END") b
ON a.device_id = b.device_id AND a.mrn = b.mrn
"""

(spark.sql(query).write
  .format("delta")
  .mode("overwrite")
  .save(Paths.chartsValidTable))

# COMMAND ----------

# MAGIC %md
# MAGIC # recordings_mrn

# COMMAND ----------

def updateRecordingsMRN(source_table, sink, checkpoint, once=False, processing_time="10 seconds"):
    
    streamingTable = f"TEMP_{source_table}"
    spark.readStream.table(source_table).createOrReplaceTempView(streamingTable)
    
    query = f"""
      SELECT b.mrn, a.device_id, a.time, a.heartrate
      FROM {streamingTable} a
      INNER JOIN
      charts_valid b
      ON a.device_id=b.device_id
      WHERE heartrate > 0
        AND ((time > start_time AND time < end_time) 
        OR (time > start_time AND valid = true))
      """
  
    if once == True:
        (spark.sql(query)
          .writeStream
          .format("delta")
          .option("checkpointLocation", checkpoint)
          .trigger(once=True)
          .start(sink)).awaitTermination()
    else:
        (spark.sql(query)
          .writeStream
          .format("delta")
          .option("checkpointLocation", checkpoint)
          .trigger(processingTime=processing_time)
          .start(sink))
        
updateRecordingsMRN("heart_rate_silver", Paths.recordingsMRNTable, Paths.recordingsMRNCheckpoint, once=True)

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

# MAGIC %md
# MAGIC ### Optimize Our Tables
# MAGIC 
# MAGIC Any table not being written with the `complete` or `overwrite` output mode will benefit from occasional compaction. We'll run these commands at the end of our scheduled daily batch to ensure that we have optimally sized files to begin the new day.
# MAGIC 
# MAGIC **If tables have not yet been created, ensure that you comment those lines out (to avoid errors).**

# COMMAND ----------

spark.sql("OPTIMIZE big_bronze")
spark.sql("OPTIMIZE heart_rate_silver")
spark.sql("OPTIMIZE charts_silver")
spark.sql("OPTIMIZE pii_silver")
spark.sql("OPTIMIZE recordings_mrn")
spark.sql("OPTIMIZE pii_current")


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>