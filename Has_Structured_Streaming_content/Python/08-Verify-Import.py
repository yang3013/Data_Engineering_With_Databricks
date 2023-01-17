# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #Verify Import
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Verify the data import did not introduce errors

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Introduction
# MAGIC 
# MAGIC Your data has been migrated over to cloud storage. 
# MAGIC 
# MAGIC The Metadata or table definitions have been created. 
# MAGIC 
# MAGIC The goal of this notebook is to validate that the import was accurate. In particular: 
# MAGIC * Correct number of rows and columns
# MAGIC * Correct datatypes
# MAGIC * Correct values, no unexpected NULL fields

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC A number of queries have been executed using Hive on the original data from the Hadoop cluster.
# MAGIC 
# MAGIC The output of those queries has been stored in a text file. 
# MAGIC 
# MAGIC You will run the same queries on the data in SparkSQL and verify math output.

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
# MAGIC ### Verify the tables are present
# MAGIC 
# MAGIC The classroom-setup script switches your focus to a database that reflects your username. 
# MAGIC 
# MAGIC A show tables SQL statement will verify that our data is present. You should see:
# MAGIC * world_population_delta
# MAGIC * world_population_hive_format
# MAGIC * world_population_orc_format

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## What to test for when migrating data
# MAGIC 
# MAGIC 0. Record Counts across matching tables in the two environments
# MAGIC 0. Column Average Value - For numeric columns using averages or other metrics to validate that there is no numeric value or precision-based anomaly introduced during the migration process
# MAGIC 0. Null/Empty Counts - For any column type check the total counts of null values to ensure it matches; pay special care that empty values and nulls are treated differently and counted separately
# MAGIC 0. String Value Match - Use string encoding or other techniques to validate that text data match across the two systems
# MAGIC 0. Legacy Ingest Rules - Apply all the legacy ingest rules to the migrated data set; for example, any column value formatting has to be verified

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Rules Engine
# MAGIC 
# MAGIC There is a GitHub project to validate Dataframes against a defined set of rules. 
# MAGIC 
# MAGIC This may be useful to automate a set of tests. 
# MAGIC 
# MAGIC Here is the [GitHub Link](https://github.com/databrickslabs/dataframe-rules-engine)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Count Rows
# MAGIC 
# MAGIC There should be 264 rows in each table.
# MAGIC 
# MAGIC In this notebook, verify the row count.
# MAGIC 
# MAGIC In the lab, you will verify a random query against results generated from Hive.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT (SELECT COUNT(*) FROM world_population_hive_format) == 264;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Next steps
# MAGIC 
# MAGIC Next there is a lab to verify additional queries. 
# MAGIC 
# MAGIC In the data dump, there is a JSON file with queries and the results returned from Hive.
# MAGIC 
# MAGIC In the lab, you will get a random query to verify that SparkSQL returns the same results.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC [Rules Engine](https://github.com/databrickslabs/dataframe-rules-engine)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>