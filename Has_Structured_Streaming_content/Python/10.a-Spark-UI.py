# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Spark UI
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Navigate the Spark UI hierarchy (Jobs, Stages)
# MAGIC * Use the Spark UI to identify bottlenecks in your workloads

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Introduction
# MAGIC 
# MAGIC The Spark UI is an invaluable tool that can provide detailed insights into **jobs**, and **stages**. Recall that a job is a sequence of stages triggered by an action and a stage consists of a sequence of tasks that can run in parallel.
# MAGIC 
# MAGIC The Spark UI can be used to help identify potential issues and, once you are familiar with the interface, you can quickly navigate to the appropriate sections in the Spark UI to investigate problems further.
# MAGIC 
# MAGIC To access the Spark UI, use either:
# MAGIC 
# MAGIC * The cluster list: click the Spark UI link on the cluster row
# MAGIC * The cluster details page: click the Spark UI tab
# MAGIC 
# MAGIC The Spark UI contains a number of tabs titled **Jobs**, **Stages**, **Storage**, **Environment**, **Executors**, **SQL**, and **JDBC/ODBC Server**.
# MAGIC 
# MAGIC Next, we'll work through an example Spark job and see how we can use the Spark UI to identify potential problems and look for optimization opportunities.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Jobs
# MAGIC 
# MAGIC The jobs page shows all the jobs and their run times. Additionally, the page shows how many stages each job contains.
# MAGIC 
# MAGIC To find further details about a job, we must first find the job by its ID and then click on the link at the bottom of the **Description** column. Furthermore, if we are working with custom code or JARS, the Description column can be used to identify the line of code calling the action that started the job.
# MAGIC 
# MAGIC In the figure below, we are going to investigate the job with ID 2.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/jobid.png" style="max-height:400px"/></p>
# MAGIC </div>
# MAGIC 
# MAGIC We can see in the figure above that job ID 2 consists of 2 stages, 208 tasks and took 4 seconds to complete.
# MAGIC 
# MAGIC **Tip**: If we are working with Spark SQL queries via JDBC, the first part of the query will also be visible in the Description column on the stages tab. Double-click on the beginning of the SQL query in the Description cell and it will expand revealing the full SQL query. However, this only works when the pool name is from **thriftserver-session**.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Stages
# MAGIC 
# MAGIC A job consists of stages and the next step is to view the stages for job ID 2. The Spark UI provides a Directed Acyclic Graph (DAG) visualization that provides the following detailed information:
# MAGIC 
# MAGIC * How the data flowed through the job
# MAGIC * Where the shuffles occurred
# MAGIC 
# MAGIC Stages are shown in the figure below.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/dag.png" style="max-height:600px"/></p>
# MAGIC </div>
# MAGIC 
# MAGIC In the figure above, there are two stages. Multiple stages are each shown in their own pink box. Shuffles cause stage boundaries. The green dot in the **InMemoryTableScan** of Stage 2 represents caching.
# MAGIC 
# MAGIC **Tip**: The duration of the stages can provide useful clues about bottlenecks. For example, if the duration of a stage is very substantial, this may be due to slow reads or the operation in a **WholeStageCodegen** block.
# MAGIC 
# MAGIC This screen also provides details of the shuffle read and shuffle write sizes. We can see these columns in the figure below.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/srsw.png" style="max-height:400px"/></p>
# MAGIC </div>
# MAGIC 
# MAGIC In jobs with large shuffles, we may see spills to memory or disk. This occurs because the shuffle write partitions are larger than the available JVM memory on the executors. A job failure will occur if executor storage is exhausted during a shuffle or spill.
# MAGIC 
# MAGIC We can select the link at the bottom of the Description column to investigate stage 2 further.
# MAGIC 
# MAGIC **Tip**: When working with JARs or Eggs, expanding the **+details** section will show the full stack trace. A developer can then track down the exact line of code that called the stage and identify where the problem originated.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Stage Details
# MAGIC 
# MAGIC #### 1. Details
# MAGIC 
# MAGIC At the top of the screen, there are a series of very useful stage aggregate metrics. Let's discuss these in more detail.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/sam.png" style="max-height:200px"/></p>
# MAGIC </div>
# MAGIC 
# MAGIC ##### 1.1 Total Time Across All Tasks
# MAGIC 
# MAGIC The total time is the compute time. As mentioned earlier, this stage took 4 seconds. However, as shown in the figure above, it actually took 11 seconds of compute time. The compute time can help identify the true cost of a query in autoscaling environments and can help identify queries with slow tasks.
# MAGIC 
# MAGIC ##### 1.2 Locality Level Summary
# MAGIC 
# MAGIC Ideally, we want to see all our tasks completed with **Process local** locality, as shown in the figure above. Where this is not possible in the Databricks environment, we may see **Any** locality. In Any locality, a shuffle is required and workloads with larger Any locality values will require more time to run.
# MAGIC 
# MAGIC A common pattern in distributed processing is to try to bring the CPU and data closer together. However, early data movement and forcing shuffles could negatively impact total compute time. On the other hand, if there is no task locality at all, then this could be a strong signal that further investigation is required.
# MAGIC 
# MAGIC ##### 1.3 Input Size / Records
# MAGIC 
# MAGIC The input size shows two values:
# MAGIC 0. The total input size into the stage
# MAGIC 0. The number of records that came into the stage
# MAGIC 
# MAGIC Note that records can be simple or complex with nested data. A size/record compression ratio for a thousand or a million records can be very useful.
# MAGIC 
# MAGIC **Tip**: Often we may be working with multiple tables in a stage. Therefore, to calculate the size of a single table, we would omit the other tables from our compression ratio.
# MAGIC 
# MAGIC The size of the data is how large it is in memory, not on disk. Data in memory provides a more accurate picture rather than using the size of the data on disk. This is because data in memory has been deserialized and decompressed. The benefit of using the memory input size value shown in the above equation is that it provides a much better estimate of the data that is being worked upon across all the partitions during this stage.
# MAGIC 
# MAGIC After calculating the compression, if we are working with tables and the data seems very large, we can consult the SQL tab for more details and check that lazy loading techniques are being applied.
# MAGIC 
# MAGIC ##### 1.4 Shuffle Write
# MAGIC 
# MAGIC Two very important metrics are **Shuffle Read** and **Shuffle Write**. The metrics can provide very useful insights about the appropriate number of shuffle partitions based upon the size of the data sent to a shuffle stage.
# MAGIC 
# MAGIC **Tip**: If you see an upcoming shuffle that is not sized correctly, cancel the job, resize the partitions and restart the job. If the shuffle partitions are not sized correctly, a shuffle stage may fail, or the shuffle may be inefficient.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 2. DAG Visualization
# MAGIC 
# MAGIC The stages DAG can be useful to correlate stages to sections of the SQL tab and process location.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC #### 3. Summary Metrics
# MAGIC 
# MAGIC The two most important tools in the Spark UI for optimizations are:
# MAGIC 0. Summary metrics pane
# MAGIC 0. SQL tab
# MAGIC 
# MAGIC Here we'll discuss the first one. The summary metrics pane can show many issues. In the following figure, there is an aggregate breakdown of task metrics by quartile.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/sm.png" style="max-height:400px"/></p>
# MAGIC </div>
# MAGIC 
# MAGIC **Tip**: Focus on the task metrics and leave the executor metrics tab collapsed. Executor metrics are useful for identifying hardware issues and should only be used if there are other pointers to hardware problems.
# MAGIC 
# MAGIC In the figure above, the metrics apply to completed tasks. So, if using page refresh, different values may appear during stage execution. Furthermore, we may see that one task took 1 minute to complete and another task took 0.1 seconds of Garbage Collection (GC) time. Therefore, it is not necessarily the case that the values in the columns apply to a single task.
# MAGIC 
# MAGIC Let's now discuss how to use these metrics to gain insights.
# MAGIC 
# MAGIC ##### 3.1 Balance
# MAGIC 
# MAGIC To ensure that our spark partitions are appropriately sized, we can check that there is minimal skew across the quartiles for all metrics:
# MAGIC * Duration
# MAGIC * GC Time
# MAGIC * Input Size
# MAGIC * Shuffle Read
# MAGIC * Shuffle Write
# MAGIC * Output
# MAGIC * Shuffle Spill to Memory
# MAGIC * Shuffle Spill to Disk
# MAGIC 
# MAGIC Note that only the metrics relevant to a stage will be visible on the metrics pane.
# MAGIC 
# MAGIC ##### 3.2 Spill
# MAGIC 
# MAGIC If **Shuffle Spill to Memory** or **Shuffle Spill to Disk** are showing significant changes in all quartiles, this requires further investigation.
# MAGIC 
# MAGIC **Tip**: Check the 75th and Max quartiles for spillage and refer to the Details at the top of the page to see the total spill for the stage. If the spill starts to grow quickly, becomes higher or moves into the Median column, cancel the job, resize the partitions and restart the job.
# MAGIC 
# MAGIC ##### 3.3 Oversized Partitions
# MAGIC 
# MAGIC Spills will occur if the partitions are too large for the core/memory ratio we discussed earlier. This problem should be corrected immediately.
# MAGIC 
# MAGIC ##### 3.4 GC Time
# MAGIC 
# MAGIC GC time should stay under 10% of task time. If GC times are increasing to 20% or more, this requires further investigation.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC #### 4. Aggregated Metrics by Executor
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/am.png" style="max-height:400px"/></p>
# MAGIC </div>
# MAGIC 
# MAGIC Sometimes there may be infrastructure or node issues in a cluster and using this section can help identify such problems. Look for any metrics that show outlier node values and investigate further.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC #### 5. Tasks
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/t.png" style="max-height:400px"/></p>
# MAGIC </div>
# MAGIC 
# MAGIC This section can be used for two main purposes:
# MAGIC 0. Identifying imbalance/skew
# MAGIC 0. Estimating the time to complete the stage
# MAGIC 
# MAGIC If there is imbalance or skew in the summary metrics, this section can be used to investigate further. Focus on tasks and sort them by:
# MAGIC * Duration
# MAGIC * Status
# MAGIC * Shuffle Write Size / Records
# MAGIC * Other metrics
# MAGIC 
# MAGIC Look for all tasks that are stragglers (show evidence of skew). Once skew has been identified, work backwards to determine where the skew is being introduced in the stage.
# MAGIC 
# MAGIC If there is a long-running stage, it would be very useful to estimate how long it may take to complete. A quick method to do this is to use the following equation after 5% of the tasks in the stage have completed:
# MAGIC 
# MAGIC `estimated total stage time = (total number of tasks / concurrent tasks) * median time to complete tasks`
# MAGIC 
# MAGIC The two assumptions in this equation are:
# MAGIC 0. Tasks are balanced (long-running tasks will not be accounted for)
# MAGIC 0. Concurrent tasks equals the total number of cores

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC Detailed information about Spark jobs and stages is displayed in the Spark UI, which you can access from:
# MAGIC 
# MAGIC * The cluster list: click the Spark UI link on the cluster row
# MAGIC * The cluster details page: click the Spark UI tab
# MAGIC 
# MAGIC The Spark UI displays cluster history for both active and terminated clusters.
# MAGIC 
# MAGIC Use the Spark UI to identify:
# MAGIC * Bottlenecks in your workloads
# MAGIC * Potential optimizations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC [Apache Spark Core Deep Dive: Proper Optimization](https://www.youtube.com/watch?v=daXEp4HmS-E)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>