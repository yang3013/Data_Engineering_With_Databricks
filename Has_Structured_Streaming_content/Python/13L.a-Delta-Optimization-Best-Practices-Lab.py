# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Delta Optimization Best Practices Lab 1
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC 
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Understand how to migrate workloads to Delta Lake
# MAGIC * Understand how to convert to Delta Lake on Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Delta Logo Tiny](https://files.training.databricks.com/courses/hadoop-migration/delta-lake-tiny-logo.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Delta Logo Tiny](https://files.training.databricks.com/courses/hadoop-migration/delta-lake-tiny-logo.png) Migrate Workloads to Delta Lake
# MAGIC 
# MAGIC Define some paths and clean-up.

# COMMAND ----------

retail_data      = "/mnt/training/online_retail/data-001/data.csv"
retail_data_mini = "/mnt/training/online_retail/outdoor-products/outdoor-products-mini.csv"

parquet_path = userhome + "/retail_parquet"
delta_path   = userhome + "/retail_delta"

dbutils.fs.rm(parquet_path, recurse = True)
dbutils.fs.rm(delta_path, recurse = True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Read the data into a Dataframe. We supply the schema.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType

retail_schema = StructType([
  StructField("InvoiceNo", IntegerType(), True),
  StructField("StockCode", StringType(), True),
  StructField("Description", StringType(), True),
  StructField("Quantity", IntegerType(), True),
  StructField("InvoiceDate", StringType(), True),
  StructField("UnitPrice", DoubleType(), True),
  StructField("CustomerID", IntegerType(), True),
  StructField("Country", StringType(), True)
])

retail = (spark.read.format("csv")
  .option("header", "true")
  .schema(retail_schema)
  .load(retail_data))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Use `overwrite` mode so that there will not be an issue in rewriting the data if we run the cell again.
# MAGIC 
# MAGIC Partition on `Country` because there are only a few unique countries and because we will use `Country` as a predicate in a `WHERE` clause. Then write the data to Delta Lake.

# COMMAND ----------

(retail.write.format("delta")
   .mode("overwrite")
   .partitionBy("Country")
   .save(delta_path))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC While we show creating a table in the next section, Spark SQL queries can run directly on a directory of data.
# MAGIC 
# MAGIC For delta use the following syntax:
# MAGIC 
# MAGIC `` SELECT * FROM delta.`/path/to/delta_directory` ``

# COMMAND ----------

display(spark.sql("SELECT * FROM delta.`{}` LIMIT 5".format(delta_path)))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Perform a `COUNT` query to verify the number of records.

# COMMAND ----------

display(spark.sql("SELECT COUNT(*) FROM delta.`{}`".format(delta_path)))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The schema is stored in the `_delta_log` directory as shown below.

# COMMAND ----------

display(dbutils.fs.ls(delta_path + "/_delta_log"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We can show the contents of the json file.

# COMMAND ----------

display(spark.read.format("json").load(delta_path + "/_delta_log/"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Metadata is displayed through `DESCRIBE DETAIL`.

# COMMAND ----------

display(spark.sql("DESCRIBE DETAIL delta.`{}`".format(delta_path)))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Create a table called `retail` using `DELTA` out of the above data.
# MAGIC 
# MAGIC The notation is:
# MAGIC > `CREATE TABLE <table-name>`<br>
# MAGIC   `USING DELTA`<br>
# MAGIC   `LOCATION <path-to-data>`<br>
# MAGIC 
# MAGIC Tables created with a specified `LOCATION` are considered unmanaged by the metastore. Unlike a managed table, where no path is specified, an unmanaged table’s files are not deleted when you `DROP` the table. However, changes to either the registered table or the files will be reflected in both locations.
# MAGIC 
# MAGIC Managed tables require that the data for our table be stored in DBFS. Unmanaged tables only store metadata in DBFS.
# MAGIC 
# MAGIC Since Delta Lake stores schema (and partition) info in the `_delta_log` directory, we do not have to specify partition columns.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS retail")

spark.sql("CREATE TABLE retail USING DELTA LOCATION '{}'".format(delta_path))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Perform a `COUNT` query to verify the number of records.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM retail

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Metadata is displayed through `DESCRIBE DETAIL <tableName>`.
# MAGIC 
# MAGIC As long as we have some data in place already for a Delta Lake table, we can infer the schema.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE DETAIL retail

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Add and Remove Partitions
# MAGIC 
# MAGIC Delta Lake automatically tracks the set of partitions present in a table and updates the list as data is added or removed. As a result, there is no need to run `ALTER TABLE [ADD|DROP] PARTITION` or `MSCK`.
# MAGIC 
# MAGIC In this section, we'll load a small amount of new data and show how easy it is to append this to our existing Delta table.
# MAGIC 
# MAGIC We'll start by loading new consumer product data.

# COMMAND ----------

new_retail = (spark.read.format("csv")
  .option("header", "true")
  .schema(retail_schema)
  .load(retail_data_mini))

display(new_retail)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Do a simple count of the number of new items to be added to production data.

# COMMAND ----------

new_retail.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Adding to our existing Delta Lake is as easy as modifying our write statement and specifying the `append` mode. 
# MAGIC 
# MAGIC Here we save to our previously created Delta Lake.

# COMMAND ----------

(new_retail.write.format("delta")
  .partitionBy("Country")
  .mode("append")
  .save(delta_path))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### REFRESH TABLE
# MAGIC 
# MAGIC Delta tables always return the most up-to-date information, so there is no need to manually call `REFRESH TABLE` after changes.
# MAGIC 
# MAGIC Let's perform a `COUNT` query to verify the number of records.

# COMMAND ----------

display(spark.sql("SELECT COUNT(*) FROM delta.`{}`".format(delta_path)))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The changes to our files have been immediately reflected in the table that we've registered.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM retail

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Load a Single Partition
# MAGIC 
# MAGIC If we are interested in a single partition, we can specify it using a `WHERE` clause.

# COMMAND ----------

display(spark.sql("SELECT * FROM delta.`{}` WHERE Country = 'United Kingdom'".format(delta_path)))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We can also do this for the table that we've registered.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM retail
# MAGIC WHERE Country = 'United Kingdom'

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Delta Logo Tiny](https://files.training.databricks.com/courses/hadoop-migration/delta-lake-tiny-logo.png) Convert to Delta Lake on Databricks
# MAGIC 
# MAGIC Let's now read-in our retail data and store it as Parquet.

# COMMAND ----------

(retail.write.format("parquet")
  .mode("overwrite")
  .partitionBy("Country")
  .save(parquet_path))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We'll create a Dataframe and view the data.

# COMMAND ----------

retail_parquet = (spark.read.format("parquet")
                    .load(parquet_path))

display(retail_parquet)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We'll now convert the existing Parquet data to Delta in-place. This command lists all the files in the directory, creates a Delta Lake transaction log that tracks these files, and automatically infers the data schema by reading the footers of all Parquet files. The conversion process collects statistics to improve query performance on the converted Delta data. If we provide a table name, the metastore is also updated to reflect that the table is now a Delta table.

# COMMAND ----------

spark.sql("CONVERT TO DELTA parquet.`{}` PARTITIONED BY (Country string)".format(parquet_path))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's run a query on the directory of data.

# COMMAND ----------

display(spark.sql("SELECT * FROM delta.`{}` LIMIT 5".format(parquet_path)))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Delta Logo Tiny](https://files.training.databricks.com/courses/hadoop-migration/delta-lake-tiny-logo.png) Delta Transaction Log
# MAGIC 
# MAGIC _ _delta_log_ contains files that are used by Delta. It is strongly recommended not to alter/delete/add anything to it manually. Only use Delta (SparkSQL or Dataframe) APIs with Delta tables.

# COMMAND ----------

display(dbutils.fs.ls(parquet_path + "/_delta_log"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Delta Logo Tiny](https://files.training.databricks.com/courses/hadoop-migration/delta-lake-tiny-logo.png) Delta Metadata
# MAGIC 
# MAGIC Since we already have data backing `retail` in place, the table in the Hive metastore automatically inherits the schema, partitioning, and table properties of the existing data.
# MAGIC 
# MAGIC Note that we only store table name, path, and database information in the Hive metastore. The actual schema is stored in the `_delta_log` directory.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Delta Logo Tiny](https://files.training.databricks.com/courses/hadoop-migration/delta-lake-tiny-logo.png) Time Travel
# MAGIC 
# MAGIC Because Delta Lake is version controlled, we have the option to query past versions of the data. Let's look at the history of our current Delta table.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY retail

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Querying an older version is as easy as adding `VERSION AS OF <desired_version>`.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM retail
# MAGIC VERSION AS OF 1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Using a single file storage system, we now have access to every version of our historical data, ensuring that our data analysts will be able to replicate their reports (and compare aggregate changes over time) and our data scientists will be able to replicate their experiments.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Check Difference Between Versions
# MAGIC 
# MAGIC We want to find the quantity from Sweden that was added to our `retail` table.
# MAGIC 
# MAGIC Our original table will be version `0`. Let's write an SQL query to see how much quantity we originally had from Sweden.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT SUM(Quantity) AS Original_Quanity
# MAGIC FROM retail
# MAGIC VERSION AS OF 0
# MAGIC WHERE Country = 'Sweden'

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We can get the difference, which represents our new entries.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT SUM(Quantity) - (
# MAGIC   SELECT SUM(Quantity)
# MAGIC   FROM retail
# MAGIC   VERSION AS OF 0
# MAGIC   WHERE Country = 'Sweden') AS New_Quantity
# MAGIC FROM retail
# MAGIC WHERE Country = 'Sweden'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC In this lesson we have discussed several important Delta Lake features using examples.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC [Databricks Documentation - Delta Lake](https://docs.databricks.com/delta/index.html)
# MAGIC 
# MAGIC [Databricks Documentation - Delta Lake - Optimizations](https://docs.databricks.com/delta/optimizations/index.html)
# MAGIC 
# MAGIC [Databricks Knowledgebase - Delta Lake](https://kb.databricks.com/delta/index.html)
# MAGIC 
# MAGIC [Faster SQL Queries on Delta Lake with Dynamic File Pruning](https://databricks.com/blog/2020/04/30/faster-sql-queries-on-delta-lake-with-dynamic-file-pruning.html)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>