# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Simple task

# COMMAND ----------

myval = dbutils.jobs.taskValues.get("task1", "key",debugValue=111)

# COMMAND ----------

print(myval)

# COMMAND ----------

# MAGIC %md
# MAGIC # Forking logic based on taskValue from previous task
# MAGIC 
# MAGIC You can exit a notebook and stop later cells from executing with
# MAGIC 
# MAGIC ```dbutils.notebook.exit()```

# COMMAND ----------

if(myval > 1000):
    dbutils.notebook.exit("bye")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Print the value retrieved from taskValues
# MAGIC 
# MAGIC Note that this command will NOT run if myval is > 1000, either as passed from taskValues, or if the optional section in the task1 notebook was executed

# COMMAND ----------

print(myval)