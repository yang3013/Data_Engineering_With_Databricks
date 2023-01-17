# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Spark UI Demo
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Understand an end-to-end Spark UI example
# MAGIC 
# MAGIC The example in this notebook is taken from [Spark - The Definitive Guide](https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/).
# MAGIC 
# MAGIC <div style="text-align:left">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/spark_book_cover.png" style="max-height:400px"/></p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Define some paths and clean-up.

# COMMAND ----------

retail_data_path_driver    = "file:/databricks/driver/online-retail-dataset.csv"
retail_data_path_directory = userhome + "/retail-data"
retail_data_path_file      = userhome + "/retail-data/online-retail-dataset.csv"

dbutils.fs.rm(retail_data_path_directory, recurse = True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC First, let's get the retail data.

# COMMAND ----------

# MAGIC %sh curl -O 'https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/retail-data/all/online-retail-dataset.csv'

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Next, let's list the files on the driver.

# COMMAND ----------

# MAGIC %fs ls "file:/databricks/driver"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now we'll move the retail data from the driver.

# COMMAND ----------

dbutils.fs.mv(retail_data_path_driver, retail_data_path_file)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's confirm that the file has been moved.

# COMMAND ----------

display(dbutils.fs.ls(retail_data_path_directory))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Finally, let's run our query.

# COMMAND ----------

(spark.read
  .option("header", "true")
  .csv(retail_data_path_file)
  .repartition(2)
  .selectExpr("instr(Description, 'GLASS') >= 1 as is_glass")
  .groupBy("is_glass")
  .count()
  .collect())

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC Let's trace its execution through the Spark UI.
# MAGIC 
# MAGIC This results in 3 rows of various values. This code kicks off an SQL query, so let's navigate to the SQL tab, where the first thing we see is aggregate statistics about this query:
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/ui_details.png" style="max-height:100px"/></p>
# MAGIC </div>
# MAGIC 
# MAGIC These will become important in a minute, but first let's take a look at the Directed Acyclic Graph (DAG) of Spark stages. Each blue box represents a stage of Spark tasks. The entire group of these stages represent our Spark job:
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/ui_sql_dag.png" style="min-height:200px"/></p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) First Stage
# MAGIC 
# MAGIC Let’s take a look at each stage in detail so that we can better understand what is going on at each level, starting with the following:
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/ui_stage_1.png" style="max-height:600px"/></p>
# MAGIC </div>
# MAGIC 
# MAGIC The box on top, labeled **WholeStageCodegen**, represents a full scan of the CSV file. The box below that represents a shuffle that we forced when we called repartition. This turned our original dataset, of a yet to be specified number of partitions, into 2 partitions.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Second Stage
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/ui_stage_2.png" style="max-height:600px"/></p>
# MAGIC </div>
# MAGIC 
# MAGIC The next step is our projection (selecting/adding/filtering columns) and the aggregation. Notice that the number of output rows is 6. This, convienently, lines up with the number of output rows (3) multiplied by the number of partitions (2) at aggregation time. This is because Spark performs an aggregation for each partition, in this case a hash-based aggregation, before shuffling the data around in preparation for the final stage.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Third Stage
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/ui_stage_3.png" style="max-height:400px"/></p>
# MAGIC </div>
# MAGIC 
# MAGIC The last stage is the aggregation of the sub-aggregations that we saw happen on a per-partition basis in the previous stage. We combine those two partitions in the final 3 rows that are the output of our total query.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Job Details
# MAGIC 
# MAGIC Let’s look further into the job’s execution. On the Jobs tab, next to **Succeeded Jobs**, find the job.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/ui_jobtab.png" style="max-height:400px"/></p>
# MAGIC </div>
# MAGIC 
# MAGIC As the figure above demonstrates, our job breaks down into 3 stages, which corresponds to what we saw on the SQL tab.
# MAGIC 
# MAGIC These stages have more or less the same information that we saw previously, but clicking the label for one of them takes us to the details for a given stage. In this example, 3 stages ran, with 8, 2, and then 200 tasks each. Before diving into the stage detail, let's review why this is the case.
# MAGIC 
# MAGIC The first stage has 8 tasks. CSV files can be split, and Spark broke up the work to be distributed relatively evenly between the different cores. This happens at the cluster level and points to an important optimization: how we store our files. The following stage has 2 tasks because we explicitly called a repartition to move the data into 2 partitions. The last stage has 200 tasks because the default shuffle partitions value is 200.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Stage Details
# MAGIC 
# MAGIC Now that we reviewed how we got here, click the stage with 8 tasks to see the next level of detail.
# MAGIC 
# MAGIC Spark provides a lot of detail about what this job did when it ran. Toward the top, notice the **Summary Metrics** section.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/ui_spark_summary.png" style="max-height:400px"/></p>
# MAGIC </div>
# MAGIC 
# MAGIC This provides a synopsis of statistics regarding various metrics. In general, these metrics are quite straightforward. What we want to be on the lookout for is uneven distributions of the values. In this case, everything looks very consistent; there are no wide swings in the distribution of values.
# MAGIC 
# MAGIC In the table at the bottom, we can also examine on a per-executor basis (one for every core, in this case). This can help identify whether a particular executor is struggling with its workload.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/ui_spark_tasks.png" style="max-height:400px"/></p>
# MAGIC </div>
# MAGIC 
# MAGIC Spark also makes available a set of more detailed metrics. To view those, click **Show Additional Metrics**, and then either choose **(De)select All** or select individual metrics, depending on what you want to see.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>