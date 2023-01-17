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
# MAGIC ### Databricks SparkSQL architecture lab
# MAGIC 
# MAGIC In this lab, you will explore the Databricks Environment.
# MAGIC 
# MAGIC Goals: 
# MAGIC * Learn how to list tables
# MAGIC * Learn how to see tables
# MAGIC * Extract data from tables

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
# MAGIC ### Run a notebook that creates a sample table and database

# COMMAND ----------

# MAGIC %run ./Runnable/Create-Sample-DB

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step One
# MAGIC ### View Databases 
# MAGIC 
# MAGIC Use SHOW DATABASES to see all the databases.
# MAGIC 
# MAGIC You should see at least the following databases
# MAGIC * default
# MAGIC * your_user_name_db
# MAGIC * sampledb
# MAGIC 
# MAGIC If you see more than that, then perhaps you are not using our standard lab environment and are in a shared workspace. 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC 
# MAGIC YOUR SQL STATEMENT HERE

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### You can do the same in python
# MAGIC 
# MAGIC The example below lists all databases using Python instead of SQL.

# COMMAND ----------

display(spark.catalog.listDatabases());

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Another method of wrapping SQL in Python
# MAGIC 
# MAGIC ```display(spark.sql("SHOW DATABASES"))```

# COMMAND ----------

display(spark.sql("SHOW DATABASES"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step Two
# MAGIC ### Determine what Database you are currently using
# MAGIC 
# MAGIC Use SQL to determine what database you are using. 
# MAGIC 
# MAGIC This could also be done in Python with ```print(spark.catalog.currentDatabase())```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC 
# MAGIC YOUR SQL STATEMENT HERE

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step Three
# MAGIC ### List functions and use regex match to search functions
# MAGIC 
# MAGIC Note that ```current_database()``` is a FUNCTION; you can use SHOW FUNCTIONS to see what other functions are available.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC 
# MAGIC YOUR SQL STATEMENT HERE

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### More on SHOW FUNCTIONS
# MAGIC 
# MAGIC * SHOW USER FUNCTIONS
# MAGIC   * Only User Defined SparkSQL functions
# MAGIC * SHOW SYSTEM FUNCTIONS
# MAGIC   * Only system defined SparkSQL functions
# MAGIC * SHOW ALL FUNCTIONS
# MAGIC   * Both User and System
# MAGIC 
# MAGIC [Documentation Page](https://docs.databricks.com/spark/latest/spark-sql/language-manual/show-functions.html)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Regular Expression Matching for Functions
# MAGIC 
# MAGIC Note that the LIKE clause uses Java Regex pattern matching rather than SQL pattern matching. 
# MAGIC 
# MAGIC Write an SQL statement to show all functions containing the string "current".
# MAGIC 
# MAGIC If you are not familiar with Java Regular Expressions, here is the pattern to use
# MAGIC ```.?current.*```
# MAGIC 
# MAGIC #### Overview of the Regex
# MAGIC * ```.?``` Matches 0 or more of any character
# MAGIC * ```current``` matches "current"
# MAGIC * ```.*``` Matches one or more of any character
# MAGIC 
# MAGIC More information on [Show Functions](https://docs.databricks.com/spark/latest/spark-sql/language-manual/show-functions.html).
# MAGIC 
# MAGIC A nice tutorial on [Java Regex](https://www.vogella.com/tutorials/JavaRegularExpressions/article.html).

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Regex Function match
# MAGIC 
# MAGIC Add a Regex to SHOW FUNCTIONS LIKE to match functions containing the string ```current```.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC 
# MAGIC -- Add a Regex clause to this statement
# MAGIC SHOW FUNCTIONS LIKE "<YOUR REGEX HERE>";

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step Four
# MAGIC ### Switch Databases and Describe table
# MAGIC 
# MAGIC Switch to the sampledb and describe the table that you find in that database.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC 
# MAGIC Write an SQL statement to switch to the database sampledb
# MAGIC 
# MAGIC Verify with a select current database statement

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### List tables in sampledb

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC 
# MAGIC Write an SQL statement to show tables from sampledb

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### View Columns in table1
# MAGIC 
# MAGIC Write an SQL statement to view the columns in table1.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC 
# MAGIC Write a describe table statement to view the columns in table1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Write an SQL statement to get more information on table1
# MAGIC 
# MAGIC Use Describe Extended to see information such as:
# MAGIC * Table Format
# MAGIC * Table Location

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC 
# MAGIC Write a describe extended statement to find more information about the table
# MAGIC describe formatted also works
# MAGIC 
# MAGIC Your statement here

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step Five
# MAGIC ### View the contents of the data directory for table1
# MAGIC 
# MAGIC The location value from describe extended shows the directory that holds datafiles for the table.
# MAGIC 
# MAGIC Take the value of location from the statement above and use it to list the contents of the directory.
# MAGIC 
# MAGIC You should see a Parquet file that holds the data, and a number of files that are written as part of job record keeping.

# COMMAND ----------

# TODO

dbutils.fs.ls("<YOUR TABLE PATH>")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Congratulations you have completed the lab!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>