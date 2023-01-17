# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Notebook 03
# MAGIC 
# MAGIC The usual path to this notebook would be if they entered a value that can not be cast to an integer in notebook 01

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Notebook Logic
# MAGIC 
# MAGIC Notebook should get passed a string
# MAGIC 
# MAGIC This notebook will return the length of the string

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Get the input value, it always starts as a sting. 

# COMMAND ----------

inputValue = dbutils.widgets.get("arg1")

# COMMAND ----------

print(inputValue)

# COMMAND ----------

inputLength = str(len(inputValue))

# COMMAND ----------

returnVal =   "Notebook 3 Succeeded " + inputValue +  " has " + inputLength + " characters "

# COMMAND ----------

dbutils.notebook.exit(returnVal)