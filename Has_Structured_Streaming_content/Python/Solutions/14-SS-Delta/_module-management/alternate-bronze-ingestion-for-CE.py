# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Want to run on CE?
# MAGIC 
# MAGIC It's not going to be a great experience, to be honest. Advanced use of the Databricks platform is going to require using the Databricks platform.
# MAGIC 
# MAGIC However, the following instructions _should_ enable you to run this module (in a modified fashion) on Community Edition.
# MAGIC 
# MAGIC ![Delta Architecture](https://files.training.databricks.com/images/enb/med_data/med_data_full_arch.png)

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC Note that CE only has one executor; I'm setting partitions to 8 currently, but you may want to reduce this number to 1 or 2 (this will represent the number of serial tasks you'll need to complete with each batch, but we also don't want to overwhelm our single core with too much data in a single partition).

# COMMAND ----------

sqlContext.setConf("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

# MAGIC %md
# MAGIC Mount data, if necessary.

# COMMAND ----------

container = "med-recordings"
storage_acct = "enbpublic"

SASTOKEN = "?sv=2019-12-12&ss=b&srt=sco&sp=rl&se=2021-01-02T04:32:00Z&st=2020-09-04T19:32:00Z&spr=https&sig=1A0h%2BnS53PxreKO2iTTaJQkuRtDQPpboqM2hP7PyTOE%3D"
SOURCE = f"wasbs://{container}@{storage_acct}.blob.core.windows.net/"
URI = f"fs.azure.sas.{container}.{storage_acct}.blob.core.windows.net"

mountedSource = "/mnt/med-recordings/"

try:
  dbutils.fs.mount(
    source=SOURCE,
    mount_point=mountedSource,
    extra_configs={URI:SASTOKEN})
except Exception as e:
  if "Directory already mounted" in str(e):
    pass # Ignore error if already mounted.
  else:
    raise e

# COMMAND ----------

# MAGIC %run "./ss-delta-setup"

# COMMAND ----------

# MAGIC %md
# MAGIC We'll work directly from the mounted data here; we can glob the file paths while reading with Spark, but we _can't_ glob file paths when listing with `dbutils.fs.ls()`.
# MAGIC 
# MAGIC For ease of development, we'll only be looking at those records from the year 2020. Our path pattern is:
# MAGIC 
# MAGIC `/YYYY/MM/DD/HH/part-0000.snappy.parquet`

# COMMAND ----------

dbutils.fs.ls("/mnt/med-recordings/2020")

# COMMAND ----------

# MAGIC %md
# MAGIC If we do a `trigger(once=True)` read on this file source, we'll consume all of these records as a single batch. This may take a while, but feel free to do this. (You may see inconsistent performance in your downstream processes, as ALL records will arrive and process as a single batch, so time-based dependencies may not process correctly).
# MAGIC 
# MAGIC We'll reduce our processing time to "1 second" to consume our batches as quickly as possible, but we'll only read one hour at a time (1 hour should be 2 files, so `maxFilesPerTrigger, 2` will be set).

# COMMAND ----------

def kafkaToBigBronzeCE(source, sink, checkpoint, once=False, processing_time="1 second"):
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
          .option("maxFilesPerTrigger", 2)
          .load(source)
          .writeStream
          .format("delta")
          .option("checkpointLocation", checkpoint)
          .trigger(processingTime=processing_time)
          .start(sink))

# COMMAND ----------

# MAGIC %md
# MAGIC Remove all written files, if necessary.

# COMMAND ----------

dbutils.fs.rm("/ss-delta-demo/delta_tables", True)

# COMMAND ----------

# MAGIC %md
# MAGIC By passing the globbed path for our mounted blob, we can queue up all our 2020 records.

# COMMAND ----------

sourceDir = "/mnt/med-recordings/2020/*/*/*"

basePath = "/ss-delta-demo/delta_tables"
checkpointPath = f"{basePath}/_checkpoints"

bronzeTable = f"{basePath}/bronze"
bronzeCheckpoint = f"{checkpointPath}/bronze_checkpoint"

kafkaToBigBronzeCE(sourceDir, bronzeTable, bronzeCheckpoint)

# COMMAND ----------

# MAGIC %md
# MAGIC You should be able to kick everything off that reads from the Bronze table as normal. Just note that you only have 1 core for a driver and 1 core for an executor in CE, so having many streams concurrently running will result in a _lot_ of resource contention and very slow execution (that is no way indicative of Databricks performance, and should _not_ be demonstrated to customers).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>