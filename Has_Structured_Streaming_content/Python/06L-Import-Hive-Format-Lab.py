# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Importing Hive Formatted Hive tables into SparkSQL Lab
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Import a Hive Formatted Hive table into SparkSQL

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Introduction
# MAGIC 
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
# MAGIC In this notebook you would create a table definition, and then copy the Hive data file into the directory for the table. 
# MAGIC 
# MAGIC This is just one of many paths available when moving tables from Hive to SparkSQL.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ##Overview
# MAGIC 
# MAGIC ### In this notebook you will complete the following steps: 
# MAGIC 1. Create a table definition in SparkSQL
# MAGIC 2. Copy the data file exported from Hive into the Spark table Location
# MAGIC 
# MAGIC You will use the HiveDataDump directory that was imported in the previous notebook.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Diagram of Steps Performed in this Lab
# MAGIC 
# MAGIC <img src="https://s3-us-west-2.amazonaws.com/files.training.databricks.com/courses/hadoop-migration/Import_Hive_Format_Lab.png" width=70%/>

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
# MAGIC 
# MAGIC The data and create table statement as exported from Hive is in the ```hive``` directory.

# COMMAND ----------

display(dbutils.fs.ls(userhome + "/hiveData/HiveDataDump"))

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

# COMMAND ----------

display(dbutils.fs.ls(userhome + "/hiveData/HiveDataDump"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### View the create table statement for the Hive Formatted Hive table
# MAGIC 
# MAGIC The output of show create table is stored in the data dump. 
# MAGIC 
# MAGIC You should see the following lines that confirm that table is stored in Hive text format:
# MAGIC 
# MAGIC ```
# MAGIC ROW FORMAT SERDE 
# MAGIC   'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
# MAGIC STORED AS INPUTFORMAT 
# MAGIC   'org.apache.hadoop.mapred.TextInputFormat' 
# MAGIC OUTPUTFORMAT 
# MAGIC   'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
# MAGIC ```
# MAGIC 
# MAGIC Once again, calling displayHTML and wrapping the text in ```<pre></pre>``` html tags will make the file more readable in the notebook.

# COMMAND ----------

displayHTML("<pre>" + dbutils.fs.head(userhome +"/hiveData/HiveDataDump/hive/create_table_statement.sql") + "</pre>")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Differences between Hive Create Table and Spark Create Table
# MAGIC 
# MAGIC In order to create this table in Spark You would remove the Location information: 
# MAGIC 
# MAGIC ```
# MAGIC LOCATION
# MAGIC   'hdfs://namenode:8020/user/hive/warehouse/world_population_hive_format'
# MAGIC ```  
# MAGIC 
# MAGIC #### Alternative
# MAGIC 
# MAGIC Simply add ``` USING HIVE ```
# MAGIC 
# MAGIC #### Spark syntax would be
# MAGIC 
# MAGIC ``` "USING HIVE" ```
# MAGIC 
# MAGIC #### Even simpler Spark syntax
# MAGIC 
# MAGIC Hive format is the default, if you just create the table and define the columns you will be fine. 
# MAGIC 
# MAGIC #### More verbose Spark syntax would be
# MAGIC 
# MAGIC ```
# MAGIC ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
# MAGIC WITH SERDEPROPERTIES (
# MAGIC   'serialization.format' = '1'
# MAGIC )
# MAGIC STORED AS
# MAGIC   INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
# MAGIC   OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Create a table
# MAGIC 
# MAGIC By copying the DDL for the table definition in Hive, we can recreate the table in Spark. 
# MAGIC 
# MAGIC The only difference is that SparkSQL can use a different syntax for the Serde, Input Format and Output Format. Also, the Location and TBLPROPERTIES are not relevent.
# MAGIC 
# MAGIC Here is the create table statement with those options removed.
# MAGIC 
# MAGIC Note that creating a table without a ```Location``` specified will create a Managed table. Creating an external table is recommended. For the purposes of simplifying the labs and demos this course uses the default location.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC 
# MAGIC -- Add your SQL statement here
# MAGIC -- Dropping and creating the table in the same cell 
# MAGIC -- allows the cell to be rerun 
# MAGIC -- if you have to edit your create table statement
# MAGIC 
# MAGIC DROP TABLE IF EXISTS world_population_hive_format;
# MAGIC 
# MAGIC -- Your Create table statement here
# MAGIC -- CREATE TABLE `world_population_hive_format`( ... 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Verify the table format
# MAGIC 
# MAGIC A number of SQL statements can show the format of the table. 
# MAGIC 
# MAGIC * DESCRIBE EXTENDED
# MAGIC * SHOW CREATE TABLE
# MAGIC * and others
# MAGIC 
# MAGIC Run DESCRIBE EXTENDED world_population_hive_format.
# MAGIC 
# MAGIC Scroll down to verify that the serde defined for the table, the default serde as we did not specify, is LazySimpleSerde.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC 
# MAGIC Your SQL Here

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step Two
# MAGIC 
# MAGIC ## Copy the data file into the table's data directory
# MAGIC 
# MAGIC The table definition is correct, all that remains is to get the data into the table. 
# MAGIC 
# MAGIC For a Hive format table, and for most formats with the exception of Delta, you can just copy data files into the data directory.
# MAGIC 
# MAGIC The path to the data directory for a table is available through ```DESCRIBE EXTENDED```. 
# MAGIC 
# MAGIC Scroll down in the output from the cell above to find the value for Location.

# COMMAND ----------

## Some Variable have been set for you
## databaseName is set by classroom setup

databaseName = spark.conf.get("com.databricks.training.spark.databaseName")

## SourcePath is the path to the data exported from hive
sourcePath = userhome + "/hiveData/HiveDataDump/hive/datadir/000000_0"

## destinationPath is the Destination of the table in spark SQL
destinationPath = "dbfs:/user/hive/warehouse/" + databaseName + ".db/world_population_hive_format"

print("sourcePath is: " + sourcePath + " \n destinationPath is: " + destinationPath)

# COMMAND ----------

# TODO

# Copy the data from sourcePath to destinationPath using a dbutils.fs.cp command

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Verify the data copy was successful
# MAGIC 
# MAGIC Read the table using an SQL statement to verify.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Congratulations 
# MAGIC 
# MAGIC You have:
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
# MAGIC [Hive to Spark Migration Presentation](https://databricks.com/session/experiences-migrating-hive-workload-to-sparksql)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>