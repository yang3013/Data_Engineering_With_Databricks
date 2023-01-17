# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # SQL Endpoint
# MAGIC 
# MAGIC You can connect Business Intelligence (BI) tools to Databricks clusters to query data in tables.
# MAGIC 
# MAGIC Every Databricks cluster runs a JDBC/ODBC server on the driver node. 
# MAGIC 
# MAGIC This notebook covers general installation and configuration instructions for most BI tools. 
# MAGIC 
# MAGIC For tool-specific connection instructions, see [Business intelligence tools](https://docs.microsoft.com/en-us/azure/databricks/integrations/bi/).

# COMMAND ----------

# MAGIC %md
# MAGIC # SQL Endpoint
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Understand how to configure a connection from an SQL tool to the cluster using JDBC

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC Hive Impala and Spark all support remote connections using SQL-based tools. 
# MAGIC 
# MAGIC For Spark, the process involves connecting to the driver node of a cluster running your Databricks workspace.
# MAGIC 
# MAGIC This notebook provides an overview of that process.
# MAGIC 
# MAGIC The documentation is available [here](https://docs.microsoft.com/en-us/azure/databricks/integrations/bi/jdbc-odbc-bi).
# MAGIC 
# MAGIC Connecting a remote application to Hive and then using the same tool to connect to SparkSQL would allow for automated testing and verification of the migration.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Setting up SQL Workbench/J to connect to Spark
# MAGIC 
# MAGIC This notebook demonstrates SQL Workbench/J configuration because SQL Workbench/J is free and easy to use. 
# MAGIC 
# MAGIC Any tool that can connect to an SQL data source using JDBC/ODBC could be used. 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Download SQL Workbench/J
# MAGIC 
# MAGIC SQL Workbench/J can be downloaded from [this site](http://www.sql-workbench.eu/).
# MAGIC 
# MAGIC As a Java application, you are basically downloading a Jar file and some scripts that call Java and execute classes in the Jar. This makes for a nice and portable application.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Configure a connection to your workspace
# MAGIC 
# MAGIC To connect to your cluster you will need:
# MAGIC * SIMBA JDBC driver
# MAGIC * Connection string
# MAGIC * Connection token

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Download the JDBC driver from Databricks
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/courses/hadoop-migration/1_jdbc_download_workbenchj.png" width=70%/>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Add the Spark Driver
# MAGIC 0. Open SQL Workbench/J
# MAGIC 0. Choose **File**, and then choose **Connect window**
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/courses/hadoop-migration/2_connect_window_workbenchJ.png" width=40%/>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Create a new connection profile
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/courses/hadoop-migration/3_select_group_workbenchj.png" width=70%/>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## In the new profile box, type a name for the profile
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/courses/hadoop-migration/4_add_profile_workbenchJ.png" width=30%/>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Choose Manage Drivers
# MAGIC 0. In the Driver box, select the driver you just added
# MAGIC 0. Add the URL for the connection
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/courses/hadoop-migration/5_configure_profile_workbenchj.png" width=100%/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Get the JDBC Connection String for the Cluster
# MAGIC 
# MAGIC 0. Navigate to the **Cluster** page in your Databricks workspace
# MAGIC 0. Select your **Cluster**
# MAGIC 0. Click **Advanced Options**
# MAGIC 0. Select **JDBC/ODBC**
# MAGIC 0. Copy the **JDBC URL**

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Edit the JDBC URL to use a token
# MAGIC 0. Click top right on the **Person Icon**
# MAGIC 0. Select **User Settings**
# MAGIC 0. **Generate New Token**
# MAGIC 0. Replace ```<personal-access-token>``` with your token
# MAGIC 0. Use that URL in Workbench/J

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC * [SQL Databases using JDBC](https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/sql-databases)
# MAGIC * [JDBC Notebook Example](https://docs.databricks.com/_static/notebooks/data-import/jdbc.html)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>