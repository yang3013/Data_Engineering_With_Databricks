# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC ### Sample Database and Table Creator
# MAGIC 
# MAGIC This notebook creates a database called sampledb. 
# MAGIC 
# MAGIC Also creates a table sampledb.table1 and drops some data in there
# MAGIC 
# MAGIC ### Purpose
# MAGIC 
# MAGIC * To have some sample data for a Lab

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS sampledb;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS sampledb.table1 as SELECT 1 id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from sampledb.table1 ;

# COMMAND ----------

displayHTML("All done!")
