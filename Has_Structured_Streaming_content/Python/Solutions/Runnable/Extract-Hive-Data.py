# Databricks notebook source

def getTags() -> dict: 
  return sc._jvm.scala.collection.JavaConversions.mapAsJavaMap(
    dbutils.entry_point.getDbutils().notebook().getContext().tags()
  )
# Get a single tag's value
def getTag(tagName: str, defaultValue: str = None) -> str:
  values = getTags()[tagName]
  try:
    if len(values) > 0:
      return values
  except:
    return defaultValue

# COMMAND ----------

import re

name = getTag("user")
databaseName = re.sub("[^a-zA-Z0-9]", "_", name) + "_db"
userhome = f"dbfs:/user/{name}"
spark.conf.set("com.databricks.training.spark.databaseName", databaseName)

dbutils.fs.rm(userhome + "/hiveData/", True)

# COMMAND ----------

import urllib.request
import tarfile

path_to_save = userhome + '/hiveData/'
dbutils.fs.mkdirs(path_to_save)
file_to_save = path_to_save + '/HiveDataDump.tar.gz'
file_to_save = file_to_save.replace('dbfs:', '/dbfs')
url = 'https://github.com/tomthetrainerDB/hadoop_migration/raw/master/HiveDataDump.tar.gz'

urllib.request.urlretrieve(url, file_to_save) 

tf = tarfile.open(file_to_save)
tf.extractall(path=path_to_save.replace("dbfs:","/dbfs"))

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({"databaseName": databaseName, "userhome": userhome}))

# COMMAND ----------

