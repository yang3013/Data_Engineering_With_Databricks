# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Chaining a series of notebooks together using widgets and exit
# MAGIC 
# MAGIC * This example shows taking input to a notebook using a widget. 
# MAGIC * Using the input value with some test logic to run one of two other notebooks
# MAGIC * Using notebook(exit)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## notebook API
# MAGIC 
# MAGIC The methods available in the dbutils.notebook API to build notebook workflows are: run and exit. Both parameters and return values must be strings.
# MAGIC 
# MAGIC run(path: String,  timeout_seconds: int, arguments: Map): String
# MAGIC Run a notebook and return its exit value. The method starts an ephemeral job that runs immediately.
# MAGIC 
# MAGIC The timeout_seconds parameter controls the timeout of the run (0 means no timeout): the call to run throws an exception if it doesn’t finish within the specified time. If Databricks is down for more than 10 minutes, the notebook run fails regardless of timeout_seconds.
# MAGIC 
# MAGIC The arguments parameter sets widget values of the target notebook. Specifically, if the notebook you are running has a widget named A, and you pass a key-value pair ("A": "B") as part of the arguments parameter to the run() call, then retrieving the value of widget A will return "B". You can find the instructions for creating and working with widgets in the Widgets topic.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # widgets
# MAGIC 
# MAGIC Widgets are a way to allow the user or a run operation to input content variables into the notebook.
# MAGIC 
# MAGIC 
# MAGIC For More info dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("Integer", "your entry", "Enter Integer here")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Notebook Logic
# MAGIC 
# MAGIC If we get a string that can not be cast to an integer go to Notebook 2
# MAGIC 
# MAGIC If we get a string that can be cast as an integer and is even Go to Notebook 3
# MAGIC 
# MAGIC If we get a string that can be cast as an integer and is odd go to notebook 4

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Get the value of the widget

# COMMAND ----------

input = dbutils.widgets.get("Integer")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Use the value of the Widget to determine which notebook to run
# MAGIC 
# MAGIC If the value is an int, run notebook 2
# MAGIC If the value is not an int, run notebook 3
# MAGIC 
# MAGIC Note the logic is actually bad here, the try will return an exception if the notebook run fails, and then go to the except !!!

# COMMAND ----------

try:
  # Is it an Integer?
  int(dbutils.widgets.get("Integer"))
  returnValue = dbutils.notebook.run("02", 60 , {"arg1": input})
except:
  # It is Not an Integer
  returnValue = dbutils.notebook.run("03", 60 , {"arg1": input})
  

# COMMAND ----------

print(returnValue)

# COMMAND ----------

