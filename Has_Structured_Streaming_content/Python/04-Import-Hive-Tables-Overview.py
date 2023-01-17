# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Importing Hive Tables into Spark SQL
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Import a Hive table into SparkSQL
# MAGIC * Understand the various methods of meeting the needed requirements

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Introduction
# MAGIC 
# MAGIC Your company has decided to move from an on-prem Hadoop architecture to a cloud architecture with Databricks as the compute engine.
# MAGIC 
# MAGIC The good news is that SparkSQL implementation is very similar to a Hive implementation.
# MAGIC 
# MAGIC Your Hadoop devops team have exported the data from three Hive tables.
# MAGIC 
# MAGIC Your task is to import those tables into  Azure Databricks.
# MAGIC 
# MAGIC This Notebook will focus on importing a Hive Parquet table into a SparkSQL Delta Table.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC Both Hive and SparkSQL tables allow the data to be stored in many formats.
# MAGIC 
# MAGIC The tables exported from Hive will be in:
# MAGIC 0. Hive Format (text ctrlA delimited)
# MAGIC 0. Parquet Format
# MAGIC 0. ORC Format
# MAGIC 
# MAGIC In this notebook you will import the Parquet table into a Delta table. 

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
# MAGIC 
# MAGIC 0. Import the HiveDataDump archive into DBFS (Databricks File System)
# MAGIC 0. Unzip and uncompress the archive
# MAGIC 0. Create a Dataframe from the Parquet data files exported from Hive
# MAGIC 0. Save that Dataframe as a SparkSQL table using Delta storage

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC #### Diagram of steps in process
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/courses/hadoop-migration/Import_Hive_Tables_Overview.png" width=70%/>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step One
# MAGIC 
# MAGIC ## Import the archive
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
# MAGIC 
# MAGIC The files in this case are small enough that we can pull them in using the driver node of our cluster.
# MAGIC 
# MAGIC If the files were larger, we could put them in  Azure Blob Storage and access the content from our notebooks, either directly or by mounting the container into DBFS.
# MAGIC 
# MAGIC **Note**: You can use %sh cells for the operations of downloading and unzipping the archive.
# MAGIC 
# MAGIC For this class, Python tools are used because it is easier to pass variables like "userhome" to Python commands.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Set some variables

# COMMAND ----------

dataDumpPath = userhome + '/hiveData' # Path to import Hadoop Data Dump from Hive

print(dataDumpPath)

# COMMAND ----------

# WORKING
import urllib.request

## REMOVE FILE IF PREVIOUSLY IMPORTED
dbutils.fs.rm(dataDumpPath, True)

############
# This is the Python equivalent of:
# wget https://files.training.databricks.com/courses/hadoop-migration/HiveDataDump.tar.gz
############

print('Beginning file download with urllib2 ...')

dbutils.fs.mkdirs(dataDumpPath)                                    # Create the Directory 
file_to_save = dataDumpPath + '/HiveDataDump.tar.gz'               # Name the file we save to
file_to_save = file_to_save.replace('dbfs:', '/dbfs')              # To reference paths directly from Python, use /dbfs/....

url = 'https://files.training.databricks.com/courses/hadoop-migration/HiveDataDump.tar.gz' # File to download
urllib.request.urlretrieve(url, file_to_save)                                              # Execute the download

print("Download Complete")
print("File Saved to: " + file_to_save)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Verify Download was Succesful

# COMMAND ----------

display(dbutils.fs.ls(dataDumpPath))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step Two
# MAGIC 
# MAGIC ## Extract the tar.gz file
# MAGIC 
# MAGIC One method to extract the archive would be to use ```%sh``` commands, a simple tar -xzvf would work.
# MAGIC 
# MAGIC This notebook uses the Python equivalent in order to pass ```userhome``` variables.

# COMMAND ----------

import tarfile

######################
# This is the python equivalent of:
# tar -xzvf HiveDataDump.tar.gz
#######################

tf = tarfile.open(file_to_save)

tf.extractall(path=dataDumpPath.replace("dbfs:","/dbfs")) # When writing directly from Python, the path for dbfs: is /dbfs

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Verify the data has been extracted

# COMMAND ----------

display(dbutils.fs.ls(dataDumpPath))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Read the README
# MAGIC 
# MAGIC The README is a text file with newlines. One way to display that in a notebook is to wrap it in ```<pre></pre>``` tags and call displayHTML.

# COMMAND ----------

displayHTML("<pre>" + dbutils.fs.head(userhome + "/hiveData/HiveDataDump/README.txt") + "</pre>")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step Three
# MAGIC 
# MAGIC ## Create a Dataframe from the Parquet data files exported from Hive
# MAGIC 
# MAGIC One way to import the data from Hive into SparkSQL is to create a Dataframe from the data files.
# MAGIC 
# MAGIC One benefit of this method is that you can verify the content as a Dataframe before saving it as a table.
# MAGIC 
# MAGIC This method is particularly effective with data formats such as Parquet that have an embedded schema as that prevents schema errors.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### View the data directory
# MAGIC 
# MAGIC The Parquet table in Hive was exported into ```/HiveDataDump/parquet```.
# MAGIC 
# MAGIC The create table statement was captured into ```create_table_statement.sql``` and the data directory was saved to ```HiveDataDump/parquet/datadir```.

# COMMAND ----------

display(dbutils.fs.ls(dataDumpPath + "/HiveDataDump/parquet"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### View the create table statement for the Parquet table
# MAGIC 
# MAGIC The output of show create table is stored in the data dump. 
# MAGIC 
# MAGIC Verify that the data is stored as Parquet.
# MAGIC 
# MAGIC Row Format Serde, Input Format, Output Format should all be some version of Parquet.
# MAGIC 
# MAGIC Once again, calling displayHTML and wrapping the text in ```<pre></pre>``` html tags will make the file more readable in the notebook.

# COMMAND ----------

displayHTML("<pre>" + dbutils.fs.head(userhome +"/hiveData/HiveDataDump/parquet/create_table_statement.sql") + "</pre>")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Create a Dataframe from the data file for the Parquet table
# MAGIC 
# MAGIC The complete contents of the Hive data directory are part of the data dump. 
# MAGIC 
# MAGIC Create a Dataframe from ```.../hiveData/HiveDataDump/parquet/datadir```.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Verify the path
# MAGIC 
# MAGIC The data directory typically contains many files, in this case all the data is stored in one file, ```000000_0```.

# COMMAND ----------

display(dbutils.fs.ls(dataDumpPath + "/HiveDataDump/parquet/datadir"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Set up a variable to point to the Parquet directory

# COMMAND ----------

parquetPath = dataDumpPath + "/HiveDataDump/parquet/datadir"
print(parquetPath)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create a Dataframe by reading the Parquet data directory

# COMMAND ----------

df = spark.read.format("parquet").load(parquetPath)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Verify the schema, count the columns
# MAGIC 
# MAGIC There should be 8 columns, verify by running `df.columns` which returns a list, and then getting the length of that list with `len`.

# COMMAND ----------

len(df.columns) == 8

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Create a temp view to verify the data is as expected
# MAGIC 
# MAGIC Remember a temp view in Spark is a table definition that points to a Dataframe definition.
# MAGIC 
# MAGIC It is **NOT** the same operation as CREATE TABLE AS SELECT; it is a way to enable SQL against the Dataframe.
# MAGIC 
# MAGIC Also note that if you materialize many intermediate tables in a Hive workflow, you may need to re-architect in Databricks in order to save on the cost of cloud storage reads and writes.
# MAGIC 
# MAGIC Some tools in Databricks that would be useful would be Temporary Views, and perhaps caching temp views if they are reused.

# COMMAND ----------

df.createOrReplaceTempView("testParquetRead")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Run a describe statement to view the schema

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE testParquetRead;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Run a select statement to verify columns have data
# MAGIC 
# MAGIC A common error when importing is a misconfiguration that leads to null values.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- Table has population by year, this query orders the table by column y2017 Desc and displays one row
# MAGIC -- The table has per country, per region and other aggregations of population including global
# MAGIC -- The row with country_name "World" should display
# MAGIC 
# MAGIC SELECT * FROM testParquetRead ORDER BY y2010 DESC LIMIT 1;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step Four
# MAGIC 
# MAGIC ## Save the Dataframe as a SparkSQL table using Delta storage
# MAGIC 
# MAGIC A Spark Dataframe can be saved as a SQL table.
# MAGIC 
# MAGIC Like Hive, a table will be created by default in whatever database you are currently working in.
# MAGIC 
# MAGIC For this class the classroom setup has created a database for you and switched to use that database. 
# MAGIC 
# MAGIC This is the database to save the table to.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why Delta?
# MAGIC 
# MAGIC Delta provides many advanced features, for an overview see the [Documentation](https://docs.databricks.com/delta/delta-intro.html).

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Verify that you are using the correct database
# MAGIC 
# MAGIC You should see username_db.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT current_database();

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Save the Dataframe as a SQL table using Delta format
# MAGIC 
# MAGIC This will write the data into a Delta Formatted directory. 
# MAGIC 
# MAGIC The location will be a directory created in the path specified for the location of the database. 
# MAGIC 
# MAGIC In this case ```dbfs:/user/hive/warehouse/<YOUR DATABASE NAME>/world_population_delta```

# COMMAND ----------

df.write.saveAsTable("world_population_delta", format="delta", mode="overwrite", partitionBy=None)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Run a ```DESCRIBE EXTENDED``` statement to verify

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE EXTENDED world_population_delta;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Run a ```SHOW TABLES``` statement to verify
# MAGIC 
# MAGIC You should see the world_population_delta table in the current database. 
# MAGIC 
# MAGIC You should also see the testparquetread temp view, temp views are local to the notebook that created them and do not have a database. 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Run a select statement to verify

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM world_population_delta ORDER BY y2010 DESC LIMIT 1;

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
# MAGIC By leveraging the fact that Parquet also stores the schema, you can create a Dataframe from Hive Parquet files, and save that Dataframe as a SparkSQL table using any format you choose. In this case we chose a Delta format so we can take advantage of the features of a Delta table.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Congratulations 
# MAGIC 
# MAGIC You Have:
# MAGIC * Created a Dataframe from Parquet files exported from Hive
# MAGIC * Saved that Dataframe as a SQL table using the Delta format

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC [Spark SQL supported and unsupported features list](https://docs.databricks.com/spark/latest/spark-sql/compatibility/hive.html)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>