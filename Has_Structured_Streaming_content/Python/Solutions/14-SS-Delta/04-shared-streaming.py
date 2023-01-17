# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Scheduling Streaming Workloads
# MAGIC 
# MAGIC We'll use this notebook as a framework to launch multiple streams on shared resources.
# MAGIC 
# MAGIC This notebook will be built out iteratively during the module. By the end, it will contain refactored code to:
# MAGIC 
# MAGIC - Schedule an always-on stream into our multiplex `big_bronze` table
# MAGIC - Schedule always-on streams to each of our parsed silver tables (with different triggers based on requirements)
# MAGIC - Schedule an always-on stream to update the `recordings_mrn` table by joining our `charts_valid` table with our `heart_rate_silver` table
# MAGIC 
# MAGIC ![Delta Architecture](https://files.training.databricks.com/images/enb/med_data/med_data_full_arch.png)
# MAGIC 
# MAGIC The remainder of our streams will be scheduled with trigger once logic to run as cron jobs throughout the day.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The notebook stops short of refactoring all the way to a Python script with defined modules/libraries. Notebooks **can** be scheduled into production for streaming workloads, but packaging can facilitate testing and lead to more robust code.

# COMMAND ----------

# MAGIC %md
# MAGIC ![Streaming Job Concepts](https://files.training.databricks.com/images/enb/med_data/streaming_job_concepts.png)
# MAGIC 
# MAGIC - A **job definition** is a Databricks concept. This could be a scheduled notebook, a JAR file, or a Python egg/wheel.
# MAGIC - A **job run** is an actual execution of that job on a given cluster.
# MAGIC - A **cluster** will either be all-purpose compute (interactive clusters used for interactive notebooks, Databricks Connect, and BI integrations) or jobs compute (engineering clusters that only exist for the lifetime of a scheduled job).
# MAGIC - A **Structured Streaming query** is triggered for each `writeStream` operation that we're scheduling.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Best Practices for Structured Streaming Job Ratios
# MAGIC 
# MAGIC Unlike YARN, streams on a single Databricks cluster share a driver. The driver will not scale out horizontally as you add more streams to a given cluster, and you don't have the same isolation guarantees that you may be used to with YARN. **The only way to increase cost efficiency is to capacity plan and correctly size clusters.**
# MAGIC 
# MAGIC - Each job run should have its own "fresh" cluster; prevents potential "ghost" streams & clusters that can be polluted by failed runs.
# MAGIC - While it's possible to schedule multiple jobs against a single interactive cluster, if one stream fails, it may trigger a condition that restarts the cluster, which would then cancel out other streaming jobs.
# MAGIC - Multiple concurrent runs of streaming jobs can lead to issues with checkpoints, as each checkpoint directory can only be used by a single query. You should only ever have 1 concurrent run for each streaming job.
# MAGIC - After limiting concurrent runs, you can set retry to unlimited, which will ensure that your streaming job continues to repair itself as cloud resources experience failure and require cluster restart.
# MAGIC 
# MAGIC #### Workspace Limits
# MAGIC There is a global job run limit of **150 concurrent jobs per workspace**. Customers with large streaming workloads that schedule each stream independently will often run into this limit. One workaround is to create an additional workspace and schedule your streams across workspaces based on criteria such as departments, applications, or pipelines. However, our experience with customers has shown that **around 40 streams** can share a correctly-sized cluster if configured properly. As such, you should conceptualize the current upper limit for streaming queries in one workspace as roughly **6000**.
# MAGIC 
# MAGIC #### Trade-offs for running multiple streams on a shared cluster:
# MAGIC 
# MAGIC Extreme cluster utilization:
# MAGIC - Super cost efficient
# MAGIC - Less complicated management overhead (no load balancing)
# MAGIC - Fewest concurrent job runs required per workspace
# MAGIC 
# MAGIC Extreme isolation:
# MAGIC - Little to no resource contention
# MAGIC - Fault isolation: no other queries affected when one fails
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Delta is the least intensive source for resource contention on Databricks. Kafka similarly requires  few driver resources. Streaming from (non-Delta) files in an object store is one of the most intensive operations for driver resources.
# MAGIC 
# MAGIC #### The Golden Ratio
# MAGIC 
# MAGIC **1 job definition: 1 job run: 1 cluster: 40 stream**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Capacity Planning
# MAGIC 
# MAGIC It's important to note here that **you should not use auto-scaling clusters when scheduling Structured Streaming workloads**. Because streaming workloads assume a constant queue of tasks, clusters will not decrease in size once they have scaled up.
# MAGIC 
# MAGIC #### Binpack is case of similar streams
# MAGIC 
# MAGIC Many of your streaming queries may have similar sources, sinks, and scale of data. These similarities make them amenable to sharing a cluster, as they will likely benefit from the same cluster type, and it should be easy to balance resources between them.
# MAGIC 
# MAGIC #### Isolate streams that require their own clusters
# MAGIC 
# MAGIC Some streams require a large amount of resources. Scheduling a stream on its own cluster ensures that there will be no resource contention, and you can right-size your cluster for your job.
# MAGIC 
# MAGIC #### Isolate streams based on their domain/pipeline and update frequency
# MAGIC 
# MAGIC Isolating on update frequency allows you to easily future-proof your workloads for latency requirements while taking advantage of scheduled trigger once logic to reduce costs. Scheduling many steps of a pipeline that drive toward the same downstream tables/dashboards/views helps to ensure that all aspects of your pipeline are working, e.g. if you have a downstream query running but no data is making it to the source table, this will result in wasted idle compute.
# MAGIC 
# MAGIC #### Isolate streams based on failure isolation requirements
# MAGIC 
# MAGIC When streams share a cluster, a single stream failing will cause the cluster to terminate and restart. Isolate platform-critical streams to prevent downtime from retries caused by co-scheduled streams.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Sizing
# MAGIC 
# MAGIC Different sources have different options for limiting batch size.
# MAGIC 
# MAGIC > "But wait, I thought we wanted to process our data as quickly as possible?"
# MAGIC 
# MAGIC Right-sizing batches ensures that we have control of our data at each step. If we try to process too much data in a batch, we can end up with inconsistent performance that may compound into further problems downstream.
# MAGIC 
# MAGIC Our goals when sizing our batches:
# MAGIC 0. Create the right number of partitions to fully utilize available resources (N partitions = N cores)
# MAGIC 0. Prevent serial execution within a single batch (N tasks !> N cores)
# MAGIC 0. Prevent spilling to disk
# MAGIC 
# MAGIC Ultimately, this results in more reliable recovery. When a job fails and is scheduled to retry, a queue will have built up. Without limitations, we can easily try to process too much data and end up with cascading delays due to inconsistent batch sizes throughout our pipeline.
# MAGIC 
# MAGIC ### File Sources
# MAGIC 
# MAGIC When streaming from most file sources, the only batch sizing option available is `maxFilesPerTrigger`. When setting this option, ensure that you consider:
# MAGIC 0. What is the variability of file size produced by my upstream processes?
# MAGIC 0. What is the rate at which these files are landing in my source directory?
# MAGIC 
# MAGIC ### Kafka Source
# MAGIC 
# MAGIC Kafka (and other pub/sub messaging systems) allow you to limit the number of messages consumed per batch. The Kafka option is `maxOffsetsPerTrigger`. Again, it's important to consider:
# MAGIC 0. What is the variability of the data payload in my messages in each topic?
# MAGIC 0. What is the rate at which messages are being enqueued?
# MAGIC 
# MAGIC ### Delta Table Source
# MAGIC 
# MAGIC While streaming from Delta Lake is configuring a stream against a file source, Delta adds the option `maxBytesPerTrigger` in addition to `maxFilesPerTrigger`. `maxBytesPerTrigger` sets a soft max, meaning that a batch processes approximately this amount of data and may process more than the limit. If you use this option in conjunction with `maxFilesPerTrigger`, the micro-batch processes data until either the `maxFilesPerTrigger` or `maxBytesPerTrigger` limit is reached. Remember that when using Delta as a streaming sink:
# MAGIC 0. File size will depend on partitioning strategy and the number of partitions present in each batch ([see documentation on Auto Optimize](https://docs.databricks.com/delta/optimizations/auto-optimize.html))
# MAGIC 0. Each micro-batch transaction creates a new table version, which exposes committed files to downstream consumers. **Inconsistent batch size and duration on a single Delta table can influence all downstream batches**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Sizes and Triggers
# MAGIC 
# MAGIC Remember that if we configure our trigger intervals by setting `processingTime`, we can control how frequently micro-batches are generated. Together, setting max batch size and trigger intervals can guarantee the flow of data into your cluster will not exceed a given rate.
# MAGIC 
# MAGIC **However**, trigger once does not respect batch size limitations, and will seek to consume all of the changes since the last batch was run as a single transaction. Designing around this behavior is important: if we shut down our stream overnight to save on computing costs outside of business hours, a single batch into a bronze table could create a backlog of data that rate-limited downstream consumers may never catch up to.
# MAGIC 
# MAGIC For the sake of simplicity, in this notebook we'll only focus on our always-on streams.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure Apache Spark Scheduler Pools for Efficiency
# MAGIC 
# MAGIC By default, all queries started in a notebook run in the same [fair scheduling pool](https://spark.apache.org/docs/latest/job-scheduling.html#scheduling-within-an-application). Therefore, jobs generated by triggers from all of the streaming queries in a notebook run one after another in first in, first out (FIFO) order. This can cause unnecessary delays in the queries, because they are not efficiently sharing the cluster resources.
# MAGIC 
# MAGIC In particular, resource-intensive streams can hog the available compute in a cluster, preventing smaller streams from achieving low latency. Configuring pools provides the capacity to fine tune your cluster to ensure processing time.
# MAGIC 
# MAGIC To enable all streaming queries to execute jobs concurrently and to share the cluster efficiently, you can set the queries to execute in separate scheduler pools. This **local property configuration** will be in the same notebook cell where we start the streaming query. For example:
# MAGIC 
# MAGIC **Run streaming query1 in scheduler pool1**
# MAGIC ```
# MAGIC spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool1")
# MAGIC df.writeStream.queryName("query1").format("parquet").start(path1)
# MAGIC ```
# MAGIC **Run streaming query2 in scheduler pool2**
# MAGIC 
# MAGIC ```
# MAGIC spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool2")
# MAGIC df.writeStream.queryName("query2").format("delta").start(path2)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Path Dependencies
# MAGIC 
# MAGIC At the beginning of each notebook, we've been running another notebook to configure our environment. We'll remove this dependency while scheduling our this notebook to run against a production cluster. We'll assume that our necessary directories are already mounted to our workspace, and instead just define the class that we've been using to specify our directories.
# MAGIC 
# MAGIC We'll define a single class object that allows us to format all of our paths. This creates more extensible code that will allow us to redirect our streaming workloads to different object stores for our dev/QA/prod environments (without needing extensive refactoring).

# COMMAND ----------

class BuildPaths:
  
    def __init__(self, base_path="/ss-delta-demo/delta_tables", source_dir="/mnt/ss-delta-demo-source/med-recordings", db="ss_delta_demo"):
        spark.sql(f"USE {db}")
        self.sourceDir = source_dir

        self.basePath = base_path
        self.checkpointPath = f"{self.basePath}/_checkpoints"

        self.bronzeTable = f"{self.basePath}/bronze"
        self.bronzeCheckpoint = f"{self.checkpointPath}/bronze_checkpoint"

        self.silverPath = f"{self.basePath}/silver"
        self.silverCheckpointPath = f"{self.checkpointPath}/silver"

        self.silverRecordingsTable = f"{self.silverPath}/recordings"
        self.silverChartsTable = f"{self.silverPath}/charts"
        self.silverPIITable = f"{self.silverPath}/pii"
        self.recordingsMRNTable = f"{self.silverPath}/recordings_mrn"
        
        self.silverRecordingsCheckpoint = f"{self.silverCheckpointPath}/silver_recordings"
        self.silverChartsCheckpoint = f"{self.silverCheckpointPath}/silver_charts"
        self.silverPIICheckpoint = f"{self.silverCheckpointPath}/silver_pii"
        self.recordingsMRNCheckpoint = f"{self.silverCheckpointPath}/recordings_mrn"
        
        self.goldPath = f"{self.basePath}/gold"
        self.goldCheckpointPath = f"{self.checkpointPath}/gold"
        
        self.hourlyDemographicsTable = f"{self.goldPath}/hourly_demographics"
        self.clinic6Table = f"{self.goldPath}/clinic_6" 
        self.piiCurrentPath = f"{self.silverPath}/pii_current"        
        self.errorRateTable = f"{self.goldPath}/error_rate"
        
        self.hourlyDemographicsCheckpoint = f"{self.goldCheckpointPath}/hourly_demographics"
        self.clinic6Checkpoint = f"{self.goldCheckpointPath}/clinic_6"
        self.piiCurrentCheckpoint = f"{self.silverCheckpointPath}/pii_current"
        
    def __repr__(self):
        return self.__dict__.__repr__().replace(", ", ",\n")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> The logic we've be using to pull out the username from internal tagging **does not** work on jobs clusters. As such, all of our arguments will need to be hardcoded in this location.

# COMMAND ----------

Paths = BuildPaths() #HARDCODE VARIABLE

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Optimize and Auto Compaction
# MAGIC 
# MAGIC We'll want to ensure that our bronze table and 3 parsed silver tables don't contain too many small files. Turning on Auto Optimize and Auto Compaction help us to avoid this problem. For more information on these settings, [consult our documentation](https://docs.databricks.com/delta/optimizations/auto-optimize.html).

# COMMAND ----------

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)

# COMMAND ----------

# MAGIC %md
# MAGIC # big_bronze
# MAGIC 
# MAGIC Our `big_bronze` table is responsible for ingesting all data from our Kafka source. The expected size and frequency of data landing is fairly stable.
# MAGIC - We have 500 devices, each of which can only have a single patient attached at any given time.
# MAGIC - We send a recording every minute from our devices.
# MAGIC - Updated patient information would only come in at the beginning of a patient visit.
# MAGIC - Chart status update messages occur when a patient is connected or removed from a device.
# MAGIC 
# MAGIC Presently, we're processing hourly batches of these records. **Remember that we're currently processing this data in accelerated time -- a new hour of records is arriving every 5-15 seconds**. With 500 devices and 60 recordings per hour, we shouldn't expect any batch to be more than approximately 30k records. 
# MAGIC 
# MAGIC An hour of records should be landing as 2 Parquet files, but our data is small. We'll try setting our `maxFilesPerBatch` at 8 (which will help us catch up if we fall behind in processing for some reason), but we may want to tune this if we see suboptimal performance.
# MAGIC 
# MAGIC Here, we'll define logic that allows us to trigger our job `once` or with a specified processing time. We'll set our default processing time to 5 seconds, which is as frequently as we would expect new data to arrive in our source.

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

# COMMAND ----------

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "bronze")
kafkaToBigBronze(Paths.sourceDir, Paths.bronzeTable, Paths.bronzeCheckpoint)

# COMMAND ----------

# MAGIC %md
# MAGIC # Parsed Silver Tables
# MAGIC 
# MAGIC In the next cell, we define a Python class to handle the queries that result in our `heart_rate_silver`, `charts_silver`, and `pii_silver` tables.
# MAGIC 
# MAGIC Note how much code is replicated when defining your `writeStream` operations in the previous 2 notebooks. Use the `write_query` method to remove this redundancy and provide options for `trigger once` batch or always-on streaming execution modes.

# COMMAND ----------

# ANSWER

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

# COMMAND ----------

ParseToSilver = ParseBronze(Paths)

# COMMAND ----------

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_parsed")
ParseToSilver.start()

# COMMAND ----------

# MAGIC %md
# MAGIC # recordings_mrn
# MAGIC 
# MAGIC Define a function that allows for `trigger once` batch or always-on streaming execution of the query that builds the `recordings_mrn` table.

# COMMAND ----------

# ANSWER

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
          .queryName("SS_recordings_mrn")
          .option("checkpointLocation", checkpoint)
          .trigger(processingTime=processing_time)
          .start(sink))

# COMMAND ----------

# spark.sparkContext.setLocalProperty("spark.scheduler.pool", "final_facts")
# updateRecordingsMRN("heart_rate_silver", Paths.recordingsMRNTable, Paths.recordingsMRNCheckpoint)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scheduling this Notebook
# MAGIC 
# MAGIC This notebook is designed to be scheduled against a jobs cluster. Note that later in the course, we will come back and complete our `updateRecordingsMRN` function and include this as one of our scheduled always-on queries.
# MAGIC 
# MAGIC Feel free to schedule the version of this notebook in the `Solutions` directory if you prefer. You'll want to ensure that all of your functions defined above are properly defined to run always-on streams with the methods and parameters being passed.
# MAGIC 
# MAGIC ### Create a New Job
# MAGIC 0. Click the Jobs button on the left sidebar
# MAGIC 0. Click the blue `+ Create Job` button
# MAGIC 0. Name the job something unique but parseable, such as `shared-streaming-<your_initials>`
# MAGIC 0. Next to **Task**, click "Select Notebook" and use the file picker to select this notebook; click OK.
# MAGIC 0. Next to **Cluster**, click "Edit".
# MAGIC 0. Change the following settings **only** (and then click "Confirm"):
# MAGIC   - **Workers**: 2
# MAGIC   - Under **Advanced Options** in the **Spark Config**, set: `sql.shuffle.partitions 8`
# MAGIC 0. Click "Run Now" to start your job.
# MAGIC 
# MAGIC ![sql-shuffle](https://files.training.databricks.com/images/enb/med_data/sql-shuffle.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>