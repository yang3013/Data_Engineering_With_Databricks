# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### A collection of useful stuff found while doing other things

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC To Check for the presence of a file

# COMMAND ----------

filename = "dbfs:/ints"



def fileExists (arg1):
  try:
    dbutils.fs.head(filename,1)
  except:
    return False;
  else:
    return True;
    
  
if(fileExists(filename)):
  print("Yes it exists");

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Some OS level working directory stuff

# COMMAND ----------

import os
 
dirpath = os.getcwd()
dirpath = "/dbfs"
print("current directory is : " + dirpath)
foldername = os.path.basename(dirpath)
print("Directory name is : " + foldername)