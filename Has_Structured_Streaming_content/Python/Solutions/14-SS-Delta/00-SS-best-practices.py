# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Advanced Structured Streaming & Delta Best Practices
# MAGIC 
# MAGIC This module will focus primarily on the core concepts and best practices to keep in mind while working with Structured Streaming and Delta Lake on Databricks. While learning these concepts, you will create a Minimum Viable Product (MVP) for a streaming Delta architecture. You will work with your instructor to learn while implementing the code necessary to build this processing pipeline.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC 
# MAGIC By the end of this module, you should be able to:
# MAGIC - Explain how Structured Streaming and Delta simplify Change Data Capture (CDC)
# MAGIC - Diagram a multiplex and singleplex Delta architecture
# MAGIC - Identify design patterns that ensure composable streams
# MAGIC - Define and implement Type 0, 1, & 2 tables in Delta Lake
# MAGIC - Build a series of bronze, silver, and gold tables
# MAGIC - Describe the trade-offs between batch and streaming jobs, specifically:
# MAGIC   - Discuss cost, stability, and future-proofing for different workloads
# MAGIC   - Set expectations about latency and data-freshness
# MAGIC   - Identify operations that aren't supported in streaming jobs
# MAGIC - Deploy a streaming notebook
# MAGIC   - Define streaming queries as simple functions
# MAGIC   - Run a stream as a job
# MAGIC   - Configure the frequency at which your stream checks for source data and how much data
# MAGIC   - Match number of partitions to number of cores
# MAGIC   - Use `trigger once` logic to convert streaming queries to batch cron jobs

# COMMAND ----------

# MAGIC %md
# MAGIC # Before We Start
# MAGIC 
# MAGIC You'll want to edit your interactive cluster to set `spark.sql.shuffle.partitions 8` under your Spark config, as shown here:
# MAGIC 
# MAGIC ![sql-shuffle](https://files.training.databricks.com/images/enb/med_data/sql-shuffle.png)
# MAGIC  
# MAGIC Generally, when working with Structured Streaming, you'll want to match your partitions to the number of available cores in your executors to ensure all compute resources are being optimally used in each batch.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario
# MAGIC 
# MAGIC A regional University hospital system has come to Databricks as part of a revamp to their overall data ecosystem. The migration was started by the in-house IT team and did not include Databricks. After a recent shake-up in the organization structure, a Chief Data Officer (CDO) was hired. Based on her previous experience working with Databricks, she has decided that the platform will provide the quickest and most secure path for her team to succeed. Necessary outcomes include:
# MAGIC 1. Enabling researchers to gain quick and secure access to current patient data
# MAGIC 1. Detecting errors in IoT medical devices
# MAGIC 1. Meta-analysis of patient data to detect trends
# MAGIC 1. Machine Learning to leverage data to improve early detection
# MAGIC 
# MAGIC This lesson will specifically be concerned with processing the following data:
# MAGIC 1. Patient demographics synced from an enterprise Electronic Health Record (EHR) provider
# MAGIC 1. Heartrate recordings provided by internet-enabled bedside monitors
# MAGIC 1. EHR chart updates that indicate when patients are assigned to a particular device
# MAGIC 
# MAGIC The hospital system has limited ability to control where these data sources live; the EHR provider and device manufacturer each sold data storage to the hospital as an integrated service, and the CDO needs to minimize costs while making these data available for analytics consumption. A Kafka service has already been configured in the cloud to accurately stream CDC data from these sources, organized into separate topics. The CDO is confident in her team's ability to maintain the Kafka service, but needs assistance to efficiently model their data in a data lake to drive further analytics. The majority of her team are most skilled in SQL, but a few recent hires have strong Python skills. End users will mostly be querying data using SQL, although some researchers will be using the Databricks platform to work with Python and R. The hospital has a small Data Science team, and some of the members of this team have worked with PySpark.
# MAGIC 
# MAGIC You will work alongside Databricks to build and implement a solution using Delta Lake and Spark Structured Streaming.

# COMMAND ----------

# MAGIC %md
# MAGIC # Reference Delta Architecture
# MAGIC 
# MAGIC In this module, you'll be builiding out the following architecture:
# MAGIC 
# MAGIC ![Delta Architecture](https://files.training.databricks.com/images/enb/med_data/med_data_full_arch.png)
# MAGIC 
# MAGIC For more details on the tables, see the [table descriptions](https://s3-us-west-2.amazonaws.com/files.training.databricks.com/courses/hadoop-migration/00-SS-Tables.pdf).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module Navigation
# MAGIC For quick access, links to all notebooks in this module are provided here (in the order they'll be taught):
# MAGIC 
# MAGIC | link | notes |
# MAGIC | --- | --- |
# MAGIC | [Multiplex Bronze notebook]($./01-SS-multiplex-bronze) | We'll be ingesting data from all three topics into a multiplex bronze table. |
# MAGIC | [Building a Silver Table: Parsing Binary JSON for Heart Rate Recordings]($./02-SS-silver-heart-rate-parsed) | The first level of silver tables will just involve parsing the payloads from Kafka. You'll use built-in functions to extract nested fields from binary-encoded JSON data. |
# MAGIC | [**LAB** Building a Silver Table: Parsing Chart & PII Updates]($./03-LAB-SS-silver-parsed) | You'll apply a similar iterative approach to unpack payloads from two additional topics. |
# MAGIC | [Scheduling Always-On Streaming Jobs]($./04-shared-streaming) | Together, we'll work to refactor the queries from the previous three notebooks and configure them for the job scheduler. |
# MAGIC | [**LAB** Migrate Historic PII Data]($./05-LAB-batch-historic-pii) | You'll create a Delta table by importing and lightly transforming CSV data. |
# MAGIC | [**LAB** COPY INTO: Create clinic_lookup through Batch Operation]($./06-LAB-batch-clinic-lookup) | You'll use Delta's `COPY INTO` syntax to batch load CSV data into a Delta table. |
# MAGIC | [Configure Streaming SCD Type 1 for PII Updates]($./07-SS-pii-current) | The `pii_updates` topic feed contains the relevant patient information as confirmed when the patient checks into the clinic. Our downstream tables and reports only require the most recent information for each patient, so SCD Type 1 is appropriate for storing these records. |
# MAGIC | [Configure Batch SCD Type 2 Table for Patient/Device Pairings]($./08-batch-charts-valid) | The `chart_status` topic feed contains events generated by electronic health charts. These events register when a patient is connected to a device and when a patient is removed from a device. This table will provide the information needed to align our medical recordings back with our PII. |
# MAGIC | [Feeding an ML Model: Stream-Static Join for Patient Heart Rate Recordings Facts Table]($./09-SS-heart-rate-mrn) | The `heart_rate_recordings` topic contains the majority of our data. We receive a record each minute from each of our recording devices. In order to identify which patient is connected to a device, we will do a stream-static join with the `charts_valid` Type 2 silver table created above. |
# MAGIC | [**LAB** Populating a Report: Device Errors by Clinic over Last Week]($./10-LAB-batch-device-errors) | Our IoT heart rate monitors occasionally send erroneous recordings. This table will be implemented to aggregate counts of these errors by device. We will also enrich this data with a stream-static join on our `clinic_lookup` table which will allow error tracking by clinic. |
# MAGIC | [Secure Dashboarding: Generating De-identified Patient Aggregates]($./11-SS-hourly-patients) | This table will join of FCF heart rate recordings table with our SCD Type 1 `pii_current` table to provide de-identified access to aggregates across a number of demographics. |
# MAGIC | [**LAB** Daily Summary: Secure Access to Daily Patient Aggregates by Clinic]($./12-LAB-SS-clinic-patients) | Researchers and clinicians with proper permissions will need access to records with patient information displayed clearly. Here we'll create a custom table with daily reports for a single clinic by joining our heart rate recordings, PII, and clinic information. |
# MAGIC | [Schedule Trigger Once Jobs]($./13-trigger-once) | Several of our queries will run as batch/`trigger once` queries. We'll use this notebook to schedule them together. |
# MAGIC | [Summary and Review]($./14-summary) | Ad-hoc exploration of our various gold tables, as well as the histories and counts of our other tables. |

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>