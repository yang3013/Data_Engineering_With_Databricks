# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Notebook 02
# MAGIC 
# MAGIC The usual path to this notebook would be if they entered an integer in notebook 01

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Notebook Logic
# MAGIC 
# MAGIC Notebook should get passed an integer, 
# MAGIC 
# MAGIC If so return odd or even
# MAGIC 
# MAGIC This notebook is designed to be called with an input value of arg1 set to int

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Retrieve inputValue and cast to int, 
# MAGIC 
# MAGIC All input values are strings initially
# MAGIC 
# MAGIC Note there should be some logic here to deal with inconsistent input  here but whatever

# COMMAND ----------

inputValue = int(dbutils.widgets.get("arg1"))

# COMMAND ----------

print(inputValue)

# COMMAND ----------

if (inputValue % 2) == 0:
  returnVal =   "Notebook 2 Succeeded, Integer " + str(inputValue) +  " is even"
else:
  returnVal =   "Notebook 2 Succeeded, Integer " + str(inputValue) + " is odd" 

#returnVal = "Hey"

# COMMAND ----------

try:
  dbutils.fs.head("dbfs:/ints")
except:
  print("File not there yet")
  

# COMMAND ----------

# to cleanup
#dbutils.fs.rm("dbfs:/ints")

# COMMAND ----------

# MAGIC %md
# MAGIC def userDefFunction (arg1, arg2, arg3 ...):
# MAGIC     program statement1
# MAGIC     program statement3
# MAGIC     program statement3
# MAGIC     ....
# MAGIC    return;

# COMMAND ----------

dbutils.notebook.exit(returnVal)

# COMMAND ----------

#dbutils.fs.ls("dbfs:/")