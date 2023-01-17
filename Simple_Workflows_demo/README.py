# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #SIMPLE MULTI TASK JOB DEMO

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Introduction
# MAGIC 
# MAGIC Databricks has had a jobs feature that would allow you to schedule a notebook to be run on a schedule, or continuously
# MAGIC 
# MAGIC Recently we added the ability run a workflow, or a series of tasks that can be configured to run on differently configured clusters and to specify dependencies, such as series or parallel execution
# MAGIC 
# MAGIC This is an attempt to demo that functionality as simply as possible

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # THE NOTEBOOKS
# MAGIC 
# MAGIC Notebook 01,02, and 03 are simple notebooks that display the output of a simple SQL statement
# MAGIC 
# MAGIC # Please complete the following
# MAGIC 
# MAGIC Using workflows build a job that executes notebook 01 as task1, when task1 completes execute notebooks 02 and 03 in parallel
# MAGIC 
# MAGIC Use an existing cluster for faster execution
# MAGIC 
# MAGIC # Extra credit
# MAGIC 
# MAGIC Add a cell to Notebook 01 with a statement that will fail such as ```Select * from no_such_table```
# MAGIC 
# MAGIC Rerun the Job to see the failure process

# COMMAND ----------

