# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Hive, Spark, and Impala Architectures
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Understand the differences in execution between the platforms
# MAGIC * Understand the similarities in execution between the platforms
# MAGIC * Know how to extract metadata from SparkSQL

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Introduction
# MAGIC 
# MAGIC The main assumption for attendees of this class is the following:
# MAGIC 
# MAGIC > Your company has decided to move from an on-prem Hadoop architecture to a cloud architecture with Databricks as the compute engine.
# MAGIC 
# MAGIC The good news is that SparkSQL implementation is very similar to a Hive implementation and somewhat similar to an Impala implementation.
# MAGIC 
# MAGIC This notebook will describe the operations of each of these SQL engines to prepare you to migrate tables from one format to another. 

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
# MAGIC ## Part One: Databricks SparkSQL Architecture

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ![Spark SQL Architecture](https://files.training.databricks.com/courses/hadoop-migration/Spark_arch.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Metastore Definition
# MAGIC 
# MAGIC #### Metastore (aka Catalog)
# MAGIC 
# MAGIC The Hive metastore is a database in a Relational Database (such as MySQL) that stores all the databases and tables that can be queried by Spark.
# MAGIC 
# MAGIC Spark clusters run a Hive metastore client, which connects directly to this database to interpret which databases and tables can be queried by Spark.
# MAGIC 
# MAGIC New databases and tables can also be created (or modified) via the Hive metastore client, which writes to the Relational Database.
# MAGIC 
# MAGIC See the Hive design [docs](http://hive.apache.org/) for more details on this standard open source component.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Role of the Hive Metastore
# MAGIC 
# MAGIC #### Hive Metastore functionality
# MAGIC * Maps Database Name to a default location
# MAGIC * Maps Tables to:
# MAGIC   * Database
# MAGIC   * Location
# MAGIC   * Format
# MAGIC   * Schema
# MAGIC   
# MAGIC #### Additional functionality of Metastore, not discussed in this class
# MAGIC * Permissions for Table and Database level ACLs
# MAGIC * Statistics for Cost Based Optimizer (CBO)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Metastore Options
# MAGIC 
# MAGIC The options for metastores are: 
# MAGIC   * Databricks Hive Metastore 
# MAGIC   * External Hive Metastore

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### SparkSQL Tables
# MAGIC 
# MAGIC When a table is defined in a workspace, the meta-information about the table is stored in the Metastore Service.
# MAGIC 
# MAGIC This is just like the Hive Metastore, and consists of a Relational Database that stores table information.
# MAGIC 
# MAGIC In particular, the Metastore stores:
# MAGIC * Table Name
# MAGIC * Database or Schema that contains the table
# MAGIC * Location of the data files
# MAGIC * Format of the table 
# MAGIC   * How the data is stored and how to serialize/deserialize

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Managed vs External Tables
# MAGIC 
# MAGIC SparkSQL uses the same methods as Hive with regards to Managed and External tables:
# MAGIC * External table
# MAGIC   * Dropping Table does not delete the data directory
# MAGIC * Managed Table
# MAGIC   * Dropping table deletes the data directory
# MAGIC   
# MAGIC **Note**: The terms "managed" and "unmanaged" are used in the Databricks documentation. Think of this as whether or not the files are managed by the Hive metastore.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Examples
# MAGIC 
# MAGIC Here is a list of some queries and how they are executed.
# MAGIC 
# MAGIC Note that all of these operations require a running cluster.
# MAGIC 
# MAGIC | Query | Execution Path |
# MAGIC | ----- | -------------- |
# MAGIC | SHOW TABLES | Metastore only operation, the driver queries the metastore and returns the result |
# MAGIC | SELECT COUNT(\*) FROM table | Spark Job is triggered, Metastore is queried for input path, and data format |
# MAGIC | CREATE table AS SELECT * FROM other_table | Spark Job is triggered, metastore is both read from and written to |
# MAGIC | ALTER table RENAME ... | Metastore only operation |

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### The Dataframe API
# MAGIC 
# MAGIC #### SparkSQL and Dataframes
# MAGIC 
# MAGIC Starting in Spark 2.0, Spark has supported the Dataframe or Structured API.
# MAGIC 
# MAGIC In the Dataframe API, SQL-like operations are supported programmatically in Scala, Python or R.
# MAGIC 
# MAGIC Data is represented as a collection of Rows, containing columns with a specified datatype.
# MAGIC 
# MAGIC Options for manipulating Dataframes include:
# MAGIC * SQL
# MAGIC * PySpark
# MAGIC * Scala
# MAGIC * R
# MAGIC 
# MAGIC Your data manipulation in Spark can be done in SQL, but the same transformations are available using Java/Scala/Python/R. 
# MAGIC 
# MAGIC All Dataframe operations regardless of the language are interpreted by the Catalyst Optimizer and the instructions are compiled into RDD operations.
# MAGIC 
# MAGIC Everything you do (more or less) in the Dataframe API uses the same underlying Catalyst execution engine.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Part Two: Hive Architecture

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ![Spark SQL Architecture](https://files.training.databricks.com/courses/hadoop-migration/hive_arch_final.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Similarities between Hive and SparkSQL
# MAGIC 
# MAGIC * Both use metastore service
# MAGIC * Both support ODBC/JDBC and command line (notebooks for Databricks) interface
# MAGIC * Both translate a SQL query into a collection of distributed processes based upon information such as data location and format from the metastore

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Differences between Hive and SparkSQL
# MAGIC 
# MAGIC * Hive executes in Map/Reduce over Yarn
# MAGIC * Spark executes in Spark
# MAGIC * Hadoop leverages data locality, Spark does not
# MAGIC * Spark clusters are ephemeral, Hadoop clusters are fixed

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Part Three: Impala Architecture

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ![Spark SQL Architecture](https://files.training.databricks.com/courses/hadoop-migration/impala_arch.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Similarities between SparkSQL and Impala
# MAGIC 
# MAGIC * Both support JDBC/ODBC and command line input
# MAGIC * Both rely on a metastore service
# MAGIC * Both convert an SQL statement into a distributed process

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Differences between SparkSQL and Impala
# MAGIC 
# MAGIC * Impala depends upon long running Impala daemons running on Datanodes, Spark uses ephemeral clusters
# MAGIC * Impala caches and optimizes queries using memory and HBase on worker nodes, spark caches and optimizes in a different manner

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Part Four: Exploring your SparkSQL environment
# MAGIC 
# MAGIC A number of informational queries are available in Databricks, many of these have direct Hive equivalents. These queries can be used to answer questions like:
# MAGIC * What database am I using?
# MAGIC * Where is the table stored?
# MAGIC * What is the table Schema?
# MAGIC 
# MAGIC The queries will be introduced here, and you will use them in the following lab.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Get Database information
# MAGIC 
# MAGIC A database is a container for tables, a database will have a default location, this is the path where data is stored for any managed\* table created in that database.
# MAGIC 
# MAGIC The default database is ```default```
# MAGIC 
# MAGIC To use a database besides ```default```, you can issue a ``` USE database_name``` statement.
# MAGIC 
# MAGIC | Information Needed | Query |
# MAGIC | ------------------ | ----- |
# MAGIC | What database am I using? | SELECT current_database(); |
# MAGIC | How do I switch databases? | USE database_name; |
# MAGIC | Where is this table located? | DESCRIBE EXTENDED table_name |
# MAGIC | What functions are available? | SHOW FUNCTIONS |
# MAGIC | How do I query across databases? | SELECT * FROM database1.table JOIN database2.table2 |
# MAGIC | How do I see all databases? | SHOW databases |
# MAGIC 
# MAGIC You may need to refer to the above table when doing the lab.
# MAGIC 
# MAGIC \* Managed and External/Unmanaged tables will be discussed later in the course. For now, the default is managed, and data is stored in the directory specified for the database.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Show current database
# MAGIC 
# MAGIC The cell below will show what database you are currently using. 
# MAGIC 
# MAGIC Note that it is **NOT** the default database; the classroom setup scripts created a database and issued a `USE database_name` statement.
# MAGIC 
# MAGIC If you see "default" as the current database, you failed to run the classroom setup cell at the beginning of this notebook.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT current_database();

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Python version
# MAGIC 
# MAGIC Most SQL statements have a direct Python or Scala equivalent, this makes it easy to wrap programming logic around the output of an SQL statement. For example, a for loop that lists the metadata for all tables in a database, or loops over a collection of tables looking for a particular column name.

# COMMAND ----------

display(spark.sql("SELECT current_database()"))

# COMMAND ----------

print(spark.catalog.currentDatabase())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### More informational queries are demonstrated in the lab.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Congratulations, you should now have a better understanding of the similarities and differences between SparkSQL, Hive and Impala.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>