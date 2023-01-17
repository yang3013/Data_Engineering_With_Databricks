# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Building a Multiplex Bronze Table
# MAGIC 
# MAGIC In this lesson you will create an MVP for a Bronze table.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC 
# MAGIC By the end of this lesson, you should be able to:
# MAGIC - Explain how Structured Streaming and Delta simplify CDC
# MAGIC - Diagram a multiplex and singleplex Delta architecture
# MAGIC - Describe the trade-offs between batch and streaming jobs, specifically:
# MAGIC   - Discuss cost, stability, and future-proofing for different workloads
# MAGIC   - Set expectations about latency and data-freshness
# MAGIC - Deploy a streaming notebook
# MAGIC   - Define streaming queries as simple functions
# MAGIC   - Run a stream as a job
# MAGIC   - Configure trigger intervals and batch sizes
# MAGIC   - Match number of partitions to number of cores
# MAGIC   - Use `trigger once` logic to convert streaming queries to batch cron jobs

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Layer
# MAGIC 
# MAGIC This notebook will be focusing primarily on the bronze layer of your data lake. For our implementation, we have decided to work with a multiplex table. The decisions on this initial ingestion layer will have been decided by the principal architect in conjunction with the customer and may include data
# MAGIC ingested directly into Delta using [Auto Loader](https://docs.databricks.com/delta/delta-ingest.html) from a variety of different data sources.
# MAGIC 
# MAGIC ## Multiplex Table
# MAGIC 
# MAGIC ![Multiplex Table](https://files.training.databricks.com/images/enb/med_data/multiplex.png)
# MAGIC 
# MAGIC You can think of a multiplex table as a "big bronze" table. Data from many different sources is loaded through this table in its raw format with sufficient metadata to trigger multiple downstream reads. The image above approximates the bronze table we'll be implementing. Note a few things about this design:
# MAGIC - Data from multiple datasets is being processed and stored side-by-side
# MAGIC - A single stream supports ingestion from multiple external data sources
# MAGIC - Multiple silver tables are generated via parallel queries against this single bronze table
# MAGIC 
# MAGIC ## Singleplex Tables
# MAGIC 
# MAGIC ![Singleplex Table](https://files.training.databricks.com/images/enb/med_data/singleplex.png)
# MAGIC 
# MAGIC Singleplex tables have less complexity than multiplex tables. Each source has its own bronze table where data is stored in raw format. These source "tables" may not yet be stored in Delta Lake, but should be retained indefinitely so that source data can be replayed if necessary. (While pub-sub messaging systems are durable, most have a message retention threshold that prevents them from filling this role). Note that:
# MAGIC - Multiple streams will need to be configured and managed to keep a current view of our raw data
# MAGIC - Versions for each of our bronze tables will be managed via a separate Delta log
# MAGIC - Silver tables will generally map 1:1 with bronze tables
# MAGIC 
# MAGIC ## Which is better?
# MAGIC Ultimately, the choice is up to the customer as they will need to use and maintain whatever system is delivered. Both approaches have benefits and drawbacks. Architectures may also include several multiplex tables or singleplex tables alongside a multiplex table.
# MAGIC 
# MAGIC Some important considerations:
# MAGIC - Multiple concurrent streams on a shared cluster will contend for driver resources
# MAGIC - Singleplex maintains [stream/table duality](https://docs.confluent.io/current/streams/concepts.html#duality-of-streams-and-tables)
# MAGIC - Decisions may be driven by how [Kakfa events and topics have been organized](https://www.confluent.io/blog/put-several-event-types-kafka-topic/)
# MAGIC 
# MAGIC ## Driver Resource Contention
# MAGIC 
# MAGIC If you plan to deploy multiple streams on a single cluster, this is an important topic to consider. The following all happen on the driver (and therefore do not horizontally scale with increased number of executors):
# MAGIC - Query planning and scheduling
# MAGIC - ABS-AQS queue processing
# MAGIC - Kafka source administration
# MAGIC - Delta transaction log administration
# MAGIC - Broadcasting
# MAGIC - Keeping track of metrics 
# MAGIC - Chauffeur/Driver connections with jobs API

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Building a Multiplex Bronze Table
# MAGIC 
# MAGIC Our chief architect has decided that we'll build a multiplex table that ingests and stores all updates ingested from our Kafka stream. The initial table will store data from all of our topics and have the following schema.
# MAGIC 
# MAGIC | Field | Type |
# MAGIC | --- | --- |
# MAGIC | timestamp | LONG |
# MAGIC | topic | STRING |
# MAGIC | key | BINARY |
# MAGIC | value | BINARY |
# MAGIC 
# MAGIC We'll be creating the table `big_bronze` in our architectural diagram. This includes data from multiple datasets, as in our EHR, pii, heart rate, etc.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/courses/hadoop-migration/ss_diagrams/01-SS-big_bronze.png" width="60%" />
# MAGIC 
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> For this demonstration, we will be _simulating_ data arriving from Kafka by reading from Parquet files with this same schema. Details on additional configurations for connecting to Kafka are available [here](https://docs.databricks.com/spark/latest/structured-streaming/kafka.html).

# COMMAND ----------

# MAGIC %md
# MAGIC The following cell declares the paths needed throughout this notebook and performs a recursive delete to reset the demo.

# COMMAND ----------

# MAGIC %md
# MAGIC **NOTE**: The following notebook only has SAS tokens currently valid through 2021-01-01.

# COMMAND ----------

# MAGIC %run "./_module-management/ss-delta-setup" $mode="reset"

# COMMAND ----------

# MAGIC %md
# MAGIC The `Paths` variable will be declared in each notebook for easy file management.

# COMMAND ----------

Paths

# COMMAND ----------

# MAGIC %md
# MAGIC ## Triggers
# MAGIC 
# MAGIC Note that the cell below has separate trigger logic defined for both our online streaming job as well as for a single batch. Both of these trigger options have important considerations when being used in production, which we'll discuss at length below.
# MAGIC 
# MAGIC Right now, we'll run our code with trigger once logic, which allows us to process all new data since the last time we ran our streaming query as a single micro-batch transaction.

# COMMAND ----------

kafkaSchema = "timestamp LONG, topic STRING, key BINARY, value BINARY"

(spark.readStream
  .format("parquet")
  .schema(kafkaSchema)
  .option("maxFilesPerTrigger", 8)
  .load(Paths.sourceDir)
  .writeStream
  .format("delta")
  .option("checkpointLocation", Paths.bronzeCheckpoint)
#   .trigger(processingTime="5 seconds")
  .trigger(once=True)
  .start(Paths.bronzeTable))

# COMMAND ----------

# MAGIC %md
# MAGIC ### When Is Trigger Once Right for Me?
# MAGIC 
# MAGIC There are many trade-offs when deciding between batch and streaming workloads. Using trigger once provides you many of the benefits of both of these designs. For a more detailed discussion, refer to [this blog post](https://databricks.com/blog/2017/05/22/running-streaming-jobs-day-10x-cost-savings.html).
# MAGIC 
# MAGIC The table below details some of the trade-offs between batch, trigger once, and streaming workloads:
# MAGIC 
# MAGIC ![Trade-offs](https://files.training.databricks.com/images/enb/med_data/streaming_batch_trade_offs.png)
# MAGIC 
# MAGIC The main trade-off when comparing batch and streaming workloads are the latency requirements. Note that trigger once allows you to benefit from the low cost of a scheduled batch but also allows for quick and easy adaptation to lower latency should requirements for your pipeline change in the future.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trigger Once to Catch Up
# MAGIC 
# MAGIC One of the great things about streaming is that it simplifies how changes to your data need to be evaluated before processing. While streams are running, they automatically process newly arriving records from the configured source. When streams are stopped, these new records will build up at the source. When the stream starts back up, it will automatically detect this backlog of new records and schedule them for processing. (With a traditional batch workload, you would design a system to specifically capture and indicate which records or files need to be processed each time you run a batch).
# MAGIC 
# MAGIC The system we are presently designing is primarily geared toward researchers in a regional hospital system who work standard hours. The CDO has decided that streams will run daily from 7 am to 6 pm in order to provide timely results during the working hours of research staff.
# MAGIC 
# MAGIC A trigger once job will be scheduled each morning to complete before 7 am. As such, _every query_ in the workload will need to be adaptable to run in either mode.
# MAGIC 
# MAGIC Trigger once will **automatically** detect and process all changes from the source to the sink as a single batch and then shut down the stream. **You can share checkpoint directories while changing your trigger**, so long as you ensure that you do not try to run your batch and always-on streams simultaneously.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using Processing Time to Define Trigger Intervals and Control Costs
# MAGIC 
# MAGIC When streaming from a directory of files, it is important to consider costs associated with our cloud-based object stores. Remember that the Delta Transaction Log lives within these object stores, and each time we run a query against a Delta table we need to ensure that we have the most up-to-date version of the underlying files. Each trigger will therefore result in several API calls to our object store (file list and file read operations), even if no changes have occurred at the source.
# MAGIC 
# MAGIC Streaming allows you to set a processing time through the trigger field. The default value is a processing time of 0 seconds, which means that your stream will be constantly polling the source for new records (in reality, this works out to be around every 10 ms). Passing a value of 1 second here will reduce API operations and associated costs by 100x. **You should always define a trigger interval when working with Delta Lake as a streaming source.**

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Define function to allow for either batch or always-on processing
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> We will write similar functions for each of our streaming queries. This will make it easier to re-use, test, and maintain our code in production.

# COMMAND ----------

# TODO
def kafkaToBigBronze(source, sink, checkpoint, once=False, processing_time="5 seconds"):
    kafkaSchema = ""
    if once == True:
        pass # replace with code
    else:
        pass # replace with code

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register your Bronze Table to the Hive Metastore
# MAGIC 
# MAGIC For easy querying in future notebooks, we'll register these files as a table in the metastore. The following logic registers an **unmanaged** or **external** table; this means that while the table registered to the metastore will be kept current with our underlying files, our files will not be changed by table-based metastore operations (e.g., `DROP TABLE`).

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS big_bronze")

spark.sql(f"""
CREATE TABLE big_bronze
USING DELTA
LOCATION '{Paths.bronzeTable}'
""")


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>