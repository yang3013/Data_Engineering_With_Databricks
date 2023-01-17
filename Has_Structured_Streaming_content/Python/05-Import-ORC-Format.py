# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Importing ORC Formatted Hive Tables into Spark SQL
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Import an ORC Formatted Hive table into SparkSQL

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/>
# MAGIC **THIS NOTEBOOK DEPENDS UPON THE PREVIOUS NOTEBOOK**

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC Both Hive and SparkSQL tables allow the data to be stored in many formats. 
# MAGIC 
# MAGIC The default format for Hive is a text file with one record per line, columns delimited by ```ctrlA``` characters.
# MAGIC 
# MAGIC Another common format is ORC, a column-based format often used with TEZ execution engine. 
# MAGIC 
# MAGIC Spark can read data stored in ORC files. 
# MAGIC 
# MAGIC Spark supports a vectorized Reader for ORC files, and it is enabled by default in the Databricks Runtime. 
# MAGIC 
# MAGIC [Vectorized Reader Documentation](https://spark.apache.org/docs/latest/sql-data-sources-orc.html)
# MAGIC 
# MAGIC In this notebook you would create a table definition and specify ORC format for storage, and then copy the Hive data file into the directory for the table. 
# MAGIC 
# MAGIC This is just one of many paths available when moving tables from Hive to SparkSQL.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Storage Format Recommendations
# MAGIC 
# MAGIC Although this exercise creates a table with the storage using ORC, we recommend using Delta or Parquet format. 
# MAGIC 
# MAGIC The purpose of showing the use of ORC is to demonstrate that Databricks supports many formats. 

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC 
# MAGIC ### In this notebook you will complete the following steps: 
# MAGIC 0. Create a table definition in SparkSQL
# MAGIC 0. Copy the data file exported from Hive into the Spark table location
# MAGIC 
# MAGIC You will use the HiveDataDump directory that was imported in the previous notebook.
# MAGIC 
# MAGIC Note that the steps are slightly different then the process used with the Parquet data; instead of passing the data through a Dataframe, we move the data files directly.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Steps in this Notebook
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/courses/hadoop-migration/Import_ORC_Format.png" width=70%/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Review the data 
# MAGIC 
# MAGIC The Hadoop data has been exported into a gzipped tar archive with the following structure:
# MAGIC 
# MAGIC <pre>
# MAGIC HiveDataDump/
# MAGIC  /hive
# MAGIC    /create_table_statement.sql
# MAGIC    /datadir
# MAGIC      /data-files
# MAGIC  /orc
# MAGIC    /create_table_statement.sql
# MAGIC    /datadir
# MAGIC      /data-files
# MAGIC  /parquet
# MAGIC    /create_table_statement.sql
# MAGIC    /datadir
# MAGIC      /data-files
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## View the extracted data

# COMMAND ----------

dataDumpPath = userhome + '/hiveData' # Path to import Hadoop Data Dump from Hive

print(dataDumpPath)

# COMMAND ----------

display(dbutils.fs.ls(dataDumpPath + "/HiveDataDump"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step One
# MAGIC 
# MAGIC ## Create a SparkSQL table by editing the create table statement exported from Hive

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### View the data directory
# MAGIC 
# MAGIC The ```orc``` directory contains:
# MAGIC * The SQL create table statement as recorded from the Hive Statement, ```SHOW CREATE TABLE```
# MAGIC * A copy of the data directory

# COMMAND ----------

display(dbutils.fs.ls(dataDumpPath + "/HiveDataDump/orc"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### View the create table statement for the ORC Formatted Hive table
# MAGIC 
# MAGIC The output of show create table is stored in the data dump. 
# MAGIC 
# MAGIC You should see the following lines that confirm that table is stored in ORC format:
# MAGIC 
# MAGIC ```
# MAGIC ROW FORMAT SERDE 
# MAGIC   'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
# MAGIC STORED AS INPUTFORMAT 
# MAGIC   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
# MAGIC OUTPUTFORMAT 
# MAGIC   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
# MAGIC ```
# MAGIC 
# MAGIC Once again, calling displayHTML and wrapping the text in ```<pre></pre>``` html tags will make the file more readable in the notebook.

# COMMAND ----------

displayHTML("<pre>" + dbutils.fs.head(userhome +"/hiveData/HiveDataDump/orc/create_table_statement.sql") + "</pre>")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Create a SparkSQL table
# MAGIC 
# MAGIC By copying the DDL for the table definition in Hive, we can recreate the table in Spark. 
# MAGIC 
# MAGIC One difference between creating Spark tables and creating Hive tables is the syntax for defining the data format. 
# MAGIC 
# MAGIC For an ORC table in Hive, the following options would create an ORC formatted table: 
# MAGIC 
# MAGIC #### Hive Style 
# MAGIC * CREATE TABLE ... STORED AS ORC
# MAGIC * ALTER TABLE ... [PARTITION partition_spec] SET FILEFORMAT ORC
# MAGIC * SET hive.default.fileformat=Orc
# MAGIC 
# MAGIC #### SparkSQL style
# MAGIC * CREATE TABLE ... USING ORC
# MAGIC 
# MAGIC The SQL statement in the next cell takes the Hive create table statement and replaces the Hive syntax for defining storage with the Spark syntax.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- Drop the table before creating it
# MAGIC -- This allows re-running this notebook without producing errors
# MAGIC 
# MAGIC DROP TABLE IF EXISTS world_population_orc_format;
# MAGIC 
# MAGIC CREATE TABLE `world_population_orc_format`(
# MAGIC   `country_name` string, 
# MAGIC   `country_code` string, 
# MAGIC   `y1960` bigint, 
# MAGIC   `y1970` bigint, 
# MAGIC   `y1980` bigint, 
# MAGIC   `y1990` bigint, 
# MAGIC   `y2000` bigint, 
# MAGIC   `y2010` bigint)
# MAGIC USING ORC;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Can you use the create table statement as is? 
# MAGIC 
# MAGIC You could, but there is a slight difference.
# MAGIC 
# MAGIC Below is an example. The table definition has been shortened to save space.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS world_population_orc_format2;
# MAGIC 
# MAGIC CREATE TABLE `world_population_orc_format2`(
# MAGIC   `country_name` string, 
# MAGIC   `country_code` string, 
# MAGIC   `indicator_name` string, 
# MAGIC   `indicator_code` string)
# MAGIC ROW FORMAT SERDE
# MAGIC   'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
# MAGIC STORED AS INPUTFORMAT
# MAGIC   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
# MAGIC OUTPUTFORMAT 
# MAGIC   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### What's the difference? 
# MAGIC 
# MAGIC The table that uses the Hive format exactly may not perform as well as a table that specifies "USING ORC". 
# MAGIC 
# MAGIC The Hive syntax will use the Hive serde's while the ORC format will use Spark or Databricks optimized Serde's.
# MAGIC 
# MAGIC To see if a table is using Hive or Spark/Databricks serdes run ```DESCRIBE FORMATTED your_table``` the Provider field will be Hive, if Hive serdes are used.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE FORMATTED world_population_orc_format;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE FORMATTED world_population_orc_format2;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- The world_population_orc_format2 table was for demo purposes only
# MAGIC -- so we remove it
# MAGIC 
# MAGIC DROP TABLE world_population_orc_format2;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Verify the table format
# MAGIC 
# MAGIC A number of SQL statements can show the format of the table. 
# MAGIC 
# MAGIC * DESCRIBE EXTENDED
# MAGIC * SHOW CREATE TABLE
# MAGIC * DESCRIBE FORMATTED
# MAGIC * and others
# MAGIC 
# MAGIC [Documentation](https://docs.databricks.com/spark/latest/spark-sql/language-manual/describe-table.html).
# MAGIC 
# MAGIC Run DESCRIBE EXTENDED world_population_orc_format.
# MAGIC 
# MAGIC Scroll down to verify that the serde defined for the table is ```org.apache.hadoop.hive.ql.io.orc.OrcSerde```.
# MAGIC 
# MAGIC Also verify that the provider is ```ORC```.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE EXTENDED world_population_orc_format;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step Two
# MAGIC 
# MAGIC ## Copy the data file into the table's data directory
# MAGIC 
# MAGIC The table definition is correct, all that remains is to get the data into the table.
# MAGIC 
# MAGIC For an ORC-formatted table, and for most formats with the exception of Delta, you can just copy data files into the data directory.
# MAGIC 
# MAGIC If you are changing the storage format, you could create a Dataframe from the source, and use saveAsTable to write to the destination in the new format. 
# MAGIC 
# MAGIC The same operation could be done in SQL by creating table1 and then create table2 as SELECT * FROM table1.
# MAGIC 
# MAGIC The path to the data directory for a table is available through ```DESCRIBE EXTENDED```.
# MAGIC 
# MAGIC Scroll down in the output from the cell above to find the value for Location.

# COMMAND ----------

databaseName = spark.conf.get("com.databricks.training.spark.databaseName") # this value was added to spark conf by classroom setup notebook
sourcePath = userhome + "/hiveData/HiveDataDump/orc/datadir/000000_0"
destinationPath = "dbfs:/user/hive/warehouse/" + databaseName + ".db/world_population_orc_format"

dbutils.fs.cp(sourcePath, destinationPath)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Verify the data copy was successful
# MAGIC 
# MAGIC Read the table using an SQL statement to verify.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM world_population_orc_format ORDER BY y2010 DESC LIMIT 1;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Congratulations 
# MAGIC 
# MAGIC You Have:
# MAGIC * Created a SparkSQL table using a create table statement
# MAGIC * Added data to the table by copying the original Hive data over to the Spark table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC A SparkSQL table is very similar to a Hive table.
# MAGIC 
# MAGIC The table requires a location for the data directory and a schema.
# MAGIC 
# MAGIC This information is stored in a Metastore service just like Hive.
# MAGIC 
# MAGIC You can use the same create table statement in SparkSQL that you would use in Hive, the only exception being the formatting options use a different syntax. In this case we left the formatting options blank and the defaults of lazySimpleSerde where what we needed. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC [Documentation on Describe Statements](https://docs.databricks.com/spark/latest/spark-sql/language-manual/describe-table.html)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>