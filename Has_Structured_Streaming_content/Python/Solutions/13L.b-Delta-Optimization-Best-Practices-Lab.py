# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Optimization Best Practices Lab 2
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC 
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Understand the performance benefits of using Delta over Parquet
# MAGIC * Understand how to use `MERGE` programmatically

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Delta Logo Tiny](https://files.training.databricks.com/courses/hadoop-migration/delta-lake-tiny-logo.png) Introduction
# MAGIC 
# MAGIC In this Lab, we will see how Delta can optimize query performance.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Delta Logo Tiny](https://files.training.databricks.com/courses/hadoop-migration/delta-lake-tiny-logo.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Delta Logo Tiny](https://files.training.databricks.com/courses/hadoop-migration/delta-lake-tiny-logo.png) Parquet vs. Databricks Delta
# MAGIC 
# MAGIC We create a standard table using Parquet format and run a quick query to observe its latency. We then run a second query over the Databricks Delta version of the same table to see the performance difference between standard tables versus Databricks Delta tables.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Define some paths and clean-up.

# COMMAND ----------

flights_data = "/mnt/training/asa/flights/1987-1989.csv"

parquet_path = userhome + "/flights_parquet"
delta_path   = userhome + "/flights_delta"

dbutils.fs.rm(parquet_path, recurse = True)
dbutils.fs.rm(delta_path, recurse = True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We'll now read the flights data.

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "8")

flights = (spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load(flights_data))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now, we'll write a Parquet-based table using the flights data.

# COMMAND ----------

(flights.write.format("parquet")
  .mode("overwrite")
  .partitionBy("Origin")
  .save(parquet_path))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Once the step above completes, the "flights" table contains details of US flights for a year.
# MAGIC 
# MAGIC Next, we'll run a query that gets the top 20 cities with highest monthly total flights on the first day of a week.

# COMMAND ----------

from pyspark.sql.functions import count

flights_parquet = (spark.read.format("parquet")
                    .load(parquet_path))

(display(flights_parquet
  .filter("DayOfWeek = 1")
  .groupBy("Month", "Origin")
  .agg(count("*")
  .alias("TotalFlights"))
  .orderBy("TotalFlights", ascending = False)
  .limit(20)))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Once the above step completes, we can see the latency with the standard "flights_parquet" table.
# MAGIC 
# MAGIC Next, we'll do the same with a Databricks Delta table. This time, before running the query, we run the `OPTIMIZE` command with `ZORDER` to ensure data is optimized for faster retrieval.
# MAGIC 
# MAGIC First, we'll write a Databricks Delta based table using the flights data.

# COMMAND ----------

(flights.write.format("delta")
  .mode("overwrite")
  .partitionBy("Origin")
  .save(delta_path))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Next, we'll `OPTIMIZE` the Databricks Delta table.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS flights")

spark.sql("CREATE TABLE flights USING DELTA LOCATION '{}'".format(delta_path))
                  
display(spark.sql("OPTIMIZE flights ZORDER BY (DayofWeek)"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Finally, let's re-run the query and observe the latency.

# COMMAND ----------

flights_delta = (spark.read.format("delta")
                  .load(delta_path))

(display(flights_delta
  .filter("DayOfWeek = 1")
  .groupBy("Month", "Origin")
  .agg(count("*")
  .alias("TotalFlights"))
  .orderBy("TotalFlights", ascending = False)
  .limit(20)))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The query over the Databricks Delta table runs much faster after `OPTIMIZE` is run. How much faster the query runs will depend upon the configuration of the cluster. However, it should be 5-10 times faster compared to the standard table.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Compact files
# MAGIC 
# MAGIC If we continuously write data to a Delta table, it will over time accumulate a large number of files, especially if we add data in small batches. This can have an adverse effect on the efficiency of table reads, and it can also affect the performance of our file system. Ideally, a large number of small files should be rewritten into a smaller number of larger files on a regular basis. This is known as **compaction**.
# MAGIC 
# MAGIC We can compact a table by repartitioning it to a smaller number of files. In addition, we can specify the option `dataChange` to be false indicating that the operation does not change the data, only rearranges the data layout. This would ensure that other concurrent operations are minimally affected due to this compaction operation.
# MAGIC 
# MAGIC Here is an example:

# COMMAND ----------

# This code will fail as the num_files variable is commented-out.
# We already performed an OPTIMIZE above, so do not need to do this again.

# num_files = 4

(spark.read.format("delta")
  .load(delta_path)
  .repartition(num_files)
  .write
  .option("dataChange", "false")
  .format("delta")
  .mode("overwrite")
  .save(delta_path))

# COMMAND ----------

# Stop the notebook in case of a "run all"

dbutils.notebook.exit("stop")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## ![Delta Logo Tiny](https://files.training.databricks.com/courses/hadoop-migration/delta-lake-tiny-logo.png) Upserting change data to a table using MERGE
# MAGIC 
# MAGIC A common use case is Change Data Capture (CDC), where we need to replicate row changes made in an OLTP table to another table for OLAP workloads. Let's use a loan data example where we have a change table of financial loan information. Some of the loans are new loans and other loans are updates to existing loans. Furthermore, our change table has the same schema as an existing loan table. We can upsert these changes using the `DeltaTable.merge()` operation which is based on the SQL `MERGE` command.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### INSERT or UPDATE using Parquet
# MAGIC 
# MAGIC With a legacy data pipeline, to insert or update a table, we must:
# MAGIC 0. Identify the new rows to be inserted
# MAGIC 0. Identify the rows that will be replaced (updated)
# MAGIC 0. Identify all of the rows that are not impacted by the insert or update
# MAGIC 0. Create a new temp table based upon all the insert statements
# MAGIC 0. Delete the original table (and all of the associated files)
# MAGIC 0. "Rename" the temp table back to the original table name
# MAGIC 0. Drop the temp table
# MAGIC 
# MAGIC ### INSERT or UPDATE with Databricks Delta Lake
# MAGIC 
# MAGIC Very simple process:
# MAGIC 0. Identify the rows to insert or update
# MAGIC 0. Use `MERGE`

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Define path and clean-up.

# COMMAND ----------

loans_data = "/databricks-datasets/learning-spark-v2/loans/loan-risks.snappy.parquet"

delta_path = userhome + "/loans_delta"

dbutils.fs.rm(delta_path, recurse = True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We'll now read the loans data.

# COMMAND ----------

(spark.read.format("parquet")
  .load(loans_data)
  .write.format("delta")
  .save(delta_path))

(spark.read.format("delta")
  .load(delta_path)
  .createOrReplaceTempView("loans_delta"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's see how many loans we have.

# COMMAND ----------

display(spark.sql("SELECT count(*) FROM loans_delta"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's focus only on a part of the "loans_delta" table.

# COMMAND ----------

display(spark.sql("SELECT * FROM loans_delta ORDER BY loan_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now, let's say we have some new loan information:
# MAGIC 0. Existing loan_id = 2 has been fully repaid and the corresponding row needs to be updated
# MAGIC 0. New loan_id = 3 has been funded and this needs to be inserted as a new row

# COMMAND ----------

cols = ['loan_id', 'funded_amnt', 'paid_amnt', 'addr_state']

items_1 = [
  (2, 1000, 1000.0, 'TX'), # Existing loan's paid_amnt updated, loan paid in full
  (3, 2000, 0.0,    'CA')  # New loan's details
]

loan_updates_1 = spark.createDataFrame(items_1, cols)

display(loan_updates_1)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC `MERGE` can upsert this in a single atomic operation.
# MAGIC 
# MAGIC SQL `MERGE` command can do both `UPDATE` and `INSERT`.
# MAGIC 
# MAGIC `MERGE INTO target t`<br>
# MAGIC `USING source s`<br>
# MAGIC `WHEN MATCHED THEN UPDATE SET ...`<br>
# MAGIC `WHEN NOT MATCHED THEN INSERT ...`
# MAGIC 
# MAGIC Programmatic APIs can perform the same operation with the same semantics as the SQL command.

# COMMAND ----------

from delta.tables import *

delta_table = DeltaTable.forPath(spark, delta_path)

(delta_table.alias("t")
  .merge(loan_updates_1.alias("s"), "s.loan_id = t.loan_id")
  .whenMatchedUpdateAll()
  .whenNotMatchedInsertAll()
  .execute())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's check the table.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Note the changes in the table:
# MAGIC 0. Existing loan_id = 2 should have been updated with paid_amnt set to 1000
# MAGIC 0. New loan_id = 3 should have been inserted

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Delta Logo Tiny](https://files.training.databricks.com/courses/hadoop-migration/delta-lake-tiny-logo.png) Deduplication using "insert-only" MERGE
# MAGIC 
# MAGIC A common ETL use case is to collect logs into Delta Lake by appending them to a table. However, often the sources can generate duplicate log records and downstream deduplication steps are needed to take care of them. With `MERGE`, we can avoid inserting the duplicate records.

# COMMAND ----------

dbutils.fs.rm(delta_path, recurse = True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Prepare a new Delta Lake table.

# COMMAND ----------

cols = ['loan_id', 'funded_amnt', 'paid_amnt', 'addr_state']

# Define new loans table data
items_2 = [
  (0, 1000, 1000.0, 'TX'),
  (1, 2000, 0.0,    'CA')
]

# Write a new Delta Lake table with the loans data
loan_updates_2 = spark.createDataFrame(items_2, cols)

(loan_updates_2
  .write
  .format("delta")
  .save(delta_path))

display(loan_updates_2)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Prepare the new data to apply which has duplicates.

# COMMAND ----------

# Define a Dataframe containing new data, some of which is already present in the table
items_3 = [  
  (1, 2000, 0.0,    'CA'), # Duplicate, loan_id = 1 is already present in the table and we don't want to update
  (2, 5000, 1010.0, 'NY')  # New data, not present in the table
]

loan_updates_3 = spark.createDataFrame(items_3, cols)

display(loan_updates_3)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Run "insert-only" merge query (no update clause).

# COMMAND ----------

delta_table = DeltaTable.forPath(spark, delta_path)

(delta_table.alias("t")
  .merge(loan_updates_3.alias("s"), "s.loan_id = t.loan_id")
  .whenNotMatchedInsertAll()
  .execute())

display(delta_table.toDF())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Delta Logo Tiny](https://files.training.databricks.com/courses/hadoop-migration/delta-lake-tiny-logo.png) Advanced uses of MERGE
# MAGIC 
# MAGIC There is [very cool extended syntax for merge](https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge) that allows you to use merge for a lot of other advanced use cases. For example:
# MAGIC 0. [Continuously apply upserts into Delta Lake tables from Structured Streaming queries](https://docs.delta.io/latest/delta-update.html#upsert-from-streaming-queries-using-foreachbatch)
# MAGIC 0. [Slowly changing data (SCD) Type 2 operation into Delta tables](https://docs.delta.io/latest/delta-update.html#slowly-changing-data-scd-type-2-operation-into-delta-tables)
# MAGIC 0. [Write change data into a Delta table](https://docs.delta.io/latest/delta-update.html#write-change-data-into-a-delta-table)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>