# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Simple cdf demo

# COMMAND ----------

spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE cdf_demo 
# MAGIC (id INT, name STRING, value DOUBLE); 
# MAGIC 
# MAGIC INSERT INTO cdf_demo VALUES (1, "Yve", 1.0);
# MAGIC INSERT INTO cdf_demo VALUES (2, "Omar", 2.5);
# MAGIC INSERT INTO cdf_demo VALUES (3, "Elia", 3.3);

# COMMAND ----------

cdc_df = (spark.readStream
               .format("delta")
               .option("readChangeData", True)
               .option("startingVersion", 0)
               .table("cdf_demo"))

# COMMAND ----------

display(cdc_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC update cdf_demo set name = "tom" where id =1;

# COMMAND ----------

