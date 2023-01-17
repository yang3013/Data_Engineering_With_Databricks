# Databricks notebook source

dbutils.widgets.text("databaseName", "default")
dbutils.widgets.text("userhome", "student")
databaseName = dbutils.widgets.get("databaseName")
userhome = dbutils.widgets.get("userhome")
print(databaseName, userhome)

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {databaseName}")
spark.sql(f"USE {databaseName}")
spark.sql("DROP TABLE IF EXISTS world_population_delta")

# COMMAND ----------

parquetPath = userhome + "/hiveData/HiveDataDump/parquet/datadir"
df = spark.read.format("parquet").load(parquetPath)
df.write.saveAsTable("world_population_delta", format="delta", mode="overwrite", partitionBy=None)

# COMMAND ----------

count = spark.read.table("world_population_delta").count()

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({"count": count}))
