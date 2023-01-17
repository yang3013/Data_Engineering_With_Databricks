# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Metadata Architecture and Configuration
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Understand the details of Metadata Migration
# MAGIC * Understand the configuration and use of a self-managed Metastore

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Metadata Overview
# MAGIC 
# MAGIC Referring to the SparkSQL Metadata diagram, you will note that the driver of the Spark cluster connects to the Metastore.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ![Spark SQL Architecture](https://files.training.databricks.com/courses/hadoop-migration/Spark_arch.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Standard Configuration
# MAGIC 
# MAGIC In a default configuration of a Databricks workspace, each workspace will be provided with a Metastore by Databricks.
# MAGIC 
# MAGIC It is possible to configure your clusters, or all the clusters in a workspace to connect to a self-managed external Metastore.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ![Metastore](https://files.training.databricks.com/courses/hadoop-migration/Azure-DB-Blog-Image.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### In a standard setup of a Databricks Workspace
# MAGIC 
# MAGIC * The Workspace is assigned a Metastore
# MAGIC * The Metastore is managed by Databricks
# MAGIC * Each cluster that starts in the workspace connects to the Metastore
# MAGIC 
# MAGIC All requests that involve a table cause:
# MAGIC * The driver of the cluster to query the Metastore
# MAGIC * Information from the Metastore such as table location and schema are used to configure any Spark job that is triggered involving the tables

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Using your own Metastore
# MAGIC 
# MAGIC If you configure a workspace to use a self-managed Metastore
# MAGIC * Your organization will be responsible for maintaining the Metastore
# MAGIC * The Metastore will run on your cloud infrastructure

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Remote Metastore Setup
# MAGIC 
# MAGIC 0. Set up VM running MariaDB
# MAGIC   0. ```sudo apt update```
# MAGIC   0. ```sudo apt install mariadb-server mariadb-client```
# MAGIC   0. ```sudo mysql_secure_installation```
# MAGIC   0. ```sudo service mariadb start```
# MAGIC 0. Configure the Database Instance  
# MAGIC   0. ```sudo mysql -u root```
# MAGIC   0. Create user with remote privileges
# MAGIC     * ```CREATE USER metastoreUser@'%' identified by 'userpass';```
# MAGIC   0. Create database for Hive Metastore to use
# MAGIC     * ```create database metastoreDB;```
# MAGIC   0. Grant metastoreUser privileges on metastoreDB  
# MAGIC     * ```GRANT ALL on metastoreDB.* to metastoreUser'%';```
# MAGIC   0. Configure to Listen on all Network Interfaces
# MAGIC     * Edit Config ```bind-address = 0.0.0.0```
# MAGIC   0. Restart
# MAGIC     * ```sudo service mysqld restart```
# MAGIC 0. Allow Port 3306 through Firewall
# MAGIC   * ```sudo iptables -A INPUT -i eth0 -p tcp -m tcp --dport 3306 -j ACCEPT```
# MAGIC 0. Initialize the Schema for the metastoreDB
# MAGIC   * Hive Binary Builds provide an initialization script
# MAGIC     0. ```wget http://mirrors.ocf.berkeley.edu/apache/hive/hive-3.1.2/apache-hive-3.1.2-bin.tar.gz```
# MAGIC     0. ```mysql -u metastoreUser -p metastoreDB < scripts/metastore/upgrade/mysql/hive-schema-3.1.0.mysql.sql```

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Cluster Config to use Remote Metastore
# MAGIC 
# MAGIC Follow the [Documentation](https://docs.databricks.com/data/metastores/external-hive-metastore.html).
# MAGIC 
# MAGIC Configuration parameter examples.
# MAGIC ```
# MAGIC # JDBC connect string for a JDBC Metastore
# MAGIC javax.jdo.option.ConnectionURL jdbc:mysql://<metastore-host>:<metastore-port>/<metastore-db>
# MAGIC 
# MAGIC # Username to use against Metastore database
# MAGIC javax.jdo.option.ConnectionUserName <mysql-username>
# MAGIC 
# MAGIC # Password to use against Metastore database
# MAGIC javax.jdo.option.ConnectionPassword <mysql-password>
# MAGIC 
# MAGIC # Driver class name for a JDBC Metastore (Runtime 3.4 and later)
# MAGIC javax.jdo.option.ConnectionDriverName org.mariadb.jdbc.Driver
# MAGIC 
# MAGIC # Driver class name for a JDBC Metastore (prior to Runtime 3.4)
# MAGIC # javax.jdo.option.ConnectionDriverName com.mysql.jdbc.Driver
# MAGIC 
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Configure all clusters
# MAGIC   
# MAGIC Typically, all clusters in the workspace use the same Metastore; use an init script to configure all clusters to use the Metastore configuration after you test on an initial cluster.
# MAGIC 
# MAGIC [Cluster Init Script Documentation](https://docs.databricks.com/clusters/init-scripts.html#init-scripts).

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Self-Managed Metastore Security considerations
# MAGIC 
# MAGIC 
# MAGIC #### Use Virtual Private Clouds
# MAGIC 
# MAGIC Databricks clusters run inside a Virtual Private Cloud (VPC).
# MAGIC 
# MAGIC We recommend that you set up the external Hive Metastore inside a new VPC and then peer these two VPCs to make clusters connect to the Hive Metastore using a private IP address.
# MAGIC 
# MAGIC See the [Documentation](https://docs.databricks.com/data/metastores/external-hive-metastore.html).

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Migration of Metadata
# MAGIC 
# MAGIC This may differ depending on the organization or the individual. 
# MAGIC 
# MAGIC Some tables or databases may be the responsibility of devops. 
# MAGIC 
# MAGIC Individuals or teams may have shared tables or databases. 
# MAGIC 
# MAGIC Individuals or teams may have permissions to create and manage their own tables. 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Scripted Migration
# MAGIC 
# MAGIC It may be possible to automate or script some or all of the Metadata migration process.
# MAGIC 
# MAGIC Once you have a Databricks managed, or self-managed Metastore, you could script the migration.
# MAGIC 
# MAGIC This would involve:
# MAGIC * Mapping on-prem HDFS locations to Cloud locations for storage
# MAGIC * Generating an export of the Metadata tables from on-prem
# MAGIC * Modifying that export with paths to new data locations
# MAGIC * Importing the modified export into the cloud based Metastore

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Topics to Consider
# MAGIC 
# MAGIC 1. Cloud Storage vs Physical Storage
# MAGIC 
# MAGIC   Creating intermediary tables using statements like 
# MAGIC   ```CREATE TABLE AS SELECT ...```
# MAGIC 
# MAGIC   This is not a significant cost in Hive/HDFS; in the cloud it involves read/write to cloud storage
# MAGIC   
# MAGIC   A more Spark-friendly approach would be to use ```CREATE OR REPLACE TEMPORARY VIEW``` rather than ```CREATE TABLE AS SELECT ...```
# MAGIC   
# MAGIC   The temporary view can be cached if it is likely to be reused
# MAGIC   
# MAGIC 2. Hive UDFs
# MAGIC 
# MAGIC   Although Hive UDFs can often be imported and used as is, they are not optimized by Spark's Catalyst execution engine, and they may prevent the use of Tungsten Record Format which is an optimized method to avoid the cost of serializing/deserializing data
# MAGIC   
# MAGIC 3. Issues with Date Format for Hive Parquet Data
# MAGIC 
# MAGIC   If there have been issues with date formats when using Hive DDL for Parquet tables, the solution would be to use Spark DDL for the tables
# MAGIC 
# MAGIC 4. Broadcast Joins
# MAGIC 
# MAGIC   Spark supports Broadcast Joins, sometimes called MapJoins in Hive
# MAGIC   
# MAGIC   Configure a Broadcast Threshold setting to automatically use Broadcast Join, or use a hint
# MAGIC   
# MAGIC    ```SELECT /*+ BROADCAST(A) */ A.a1, A.a2, B.b1, B.b2 FROM A JOIN B```
# MAGIC 5. Skewed Data
# MAGIC   
# MAGIC   Data Skew is an optimization challenge for both Spark and Hadoop; check long running jobs to see if Skew is the issue and rewrite/re-organize as needed
# MAGIC   
# MAGIC   [Skew Documentation](https://docs.databricks.com/delta/join-performance/skew-join.html)
# MAGIC    
# MAGIC 6. Pandas/Vectorized UDFs
# MAGIC 
# MAGIC   When writing or using a Python UDF, Pandas or Vectorized UDFs can increase performance
# MAGIC 
# MAGIC   [Documentation](https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html)
# MAGIC   
# MAGIC 7. If possible, write UDFs in Scala
# MAGIC 
# MAGIC   Python UDFs invoke the overhead of having to instantiate a row and ship the row to a Python interpreter; this involves quite a bit of overhead and a Scala UDF will run faster
# MAGIC  
# MAGIC 8. Table Partitions
# MAGIC 
# MAGIC   SparkSQL supports partitioning of tables, in both Spark and Map/Reduce, a large number of partitions can have a detrimental impact on performance
# MAGIC   
# MAGIC   Optimal: Less than 2,000 partitions
# MAGIC   
# MAGIC   Avoid: More than 10,000 partitions
# MAGIC   
# MAGIC   Avoid: Small File Problem
# MAGIC   
# MAGIC 9. Table Statistics
# MAGIC 
# MAGIC   Similar to Hive, SparkSQL supports the collection of table statistics in the Metastore with an Analyze Table statement 
# MAGIC   
# MAGIC   Delta Tables also maintain internal statistics on both the table level, and per Parquet file
# MAGIC   
# MAGIC   Delta table operations still benefit from collecting statistics with Analyze Table statement 
# MAGIC   
# MAGIC   Collecting Statistics enables SparkSQL to use a [Cost based Optimizer](https://docs.databricks.com/spark/latest/spark-sql/cbo.html#cost-based-optimizer)
# MAGIC   
# MAGIC 10. Partition Discovery
# MAGIC 
# MAGIC   You may need to run `MSCK REPAIR TABLE` which will redefine the partitioning information in the Metastore based on the filesystem structure in the tables Data Directory, [Documentation](https://docs.databricks.com/data/tables.html#create-partitioned-tables)
# MAGIC   
# MAGIC 11. Hive UDFs
# MAGIC 
# MAGIC   Hive UDFs may be a bottleneck - Spark may not be able to optimize them well, potentially rewrite
# MAGIC   
# MAGIC   Hive Python UDFs (stdin/stdout magic) have a completely different syntax than Spark UDFs, so they will need to be rewritten
# MAGIC   
# MAGIC 12. Impala Timestamps
# MAGIC 
# MAGIC    Impala does not store or interpret timestamps using the local timezone; instead, it always uses UTC, which  behaves differently than SparkSQL
# MAGIC 
# MAGIC 13. Impala Complex Datatypes
# MAGIC  
# MAGIC  Complex data types in Impala are queried as separate tables; in SparkSQL nested data can be accessed using dot notation, ```column.nested_value```
# MAGIC    
# MAGIC    See, [Impala Documentation](https://docs.cloudera.com/documentation/enterprise/5-5-x/topics/impala_array.html#array)
# MAGIC    
# MAGIC 14. Impala Insert into vs SparkSQL
# MAGIC 
# MAGIC   Insert into select doesn’t require column names to match in Impala, in Spark/Delta, it does; in Impala as long as the order and schema is a match, it will work

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## List of Hive Functions not supported by SparkSQL
# MAGIC 
# MAGIC * compute_stats
# MAGIC * context_ngrams
# MAGIC * create_union
# MAGIC * current_user
# MAGIC * ewah_bitmap 
# MAGIC * ewah_bitmap_and
# MAGIC * ewah_bitmap_empty
# MAGIC * ewah_bitmap_or
# MAGIC * field
# MAGIC * in_file
# MAGIC * index
# MAGIC * matchpath 
# MAGIC * ngrams 
# MAGIC * noop
# MAGIC * noopstreaming 
# MAGIC * noopwithmap
# MAGIC * noopwithmapstreaming
# MAGIC * parse_url_tuple
# MAGIC * reflect2 
# MAGIC * windowingtablefunction

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC You should now:
# MAGIC * Understand how to setup a self-managed Metastore
# MAGIC * Understand how Databricks can manage the Metastore for you
# MAGIC * Understand that for large migrations it may be worthwhile to invest the time to write a script to automate the process

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC [Setup a Remote Metastore](https://docs.databricks.com/data/metastores/external-hive-metastore.html)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>