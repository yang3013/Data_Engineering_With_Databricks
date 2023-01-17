# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Variables in SQL cells
# MAGIC 
# MAGIC This is a quick demo notebook to share some quick examples of passing and retrieving variables in sql. 
# MAGIC 
# MAGIC Note that not every possible use of these features is demonstrated, just enough to show you what is possible

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Widgets
# MAGIC 
# MAGIC Widgets are a way to take user input to a notebook through the web interface, or passed as input values when run as a job.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Set a widget in python

# COMMAND ----------

dbutils.widgets.text("input", "user_supplied_input")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Retrieve a widget in SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select getArgument("input") as column1;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Setting and retrieving variables 
# MAGIC 
# MAGIC In addition to retrieving the values of widgets you can set and retrieve variables in SQL. 
# MAGIC 
# MAGIC Much of this is similar to functionality provided by hive and the hive documentation is a useful resource.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Namespaces
# MAGIC 
# MAGIC There are three namespaces for variables – hiveconf, system, and env. (Custom variables can also be created in a separate namespace with the define or hivevar option in Hive 0.8.0 and later releases.)
# MAGIC 
# MAGIC The hiveconf variables are set as normal:
# MAGIC 
# MAGIC set x=myvalue
# MAGIC However they are retrieved using:
# MAGIC 
# MAGIC ${hiveconf:x}

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Setting and retrieving variable in hiveconf namespace

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC set x = "tom";
# MAGIC 
# MAGIC select ${hiveconf:x};

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Create table using the variable
# MAGIC 
# MAGIC We assign the column the name "name", but insert the value of the var

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create table example_table as select ${hiveconf:x} name ;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Verify

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from example_table;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Add another variable to the hiveconf space

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC set y = "evan";
# MAGIC 
# MAGIC select ${hiveconf:y};

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Insert the value of that var into the table

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC insert into example_table select ${hiveconf:y}

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Use the variable in a filter

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from example_table where name = ${hiveconf:y};

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Retrieving and Setting Spark Configuration in SQL

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Using ```set``` to retrieve values

# COMMAND ----------

# MAGIC %sql
# MAGIC -- retrieve all settings
# MAGIC set;

# COMMAND ----------

# MAGIC %sql
# MAGIC --retrieve a specific setting
# MAGIC 
# MAGIC set spark.sql.shuffle.partitions;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- alternative way to select a value
# MAGIC select ${hiveconf:spark.sql.shuffle.partitions} as column1; 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- change a setting
# MAGIC set spark.sql.shuffle.partitions = 8;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Using a custom namespace to pass values from sql to python/scala

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- set in sql
# MAGIC 
# MAGIC set databricks.training.sample_var = "tom";

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retrieve in Python

# COMMAND ----------

myvar = spark.conf.get("databricks.training.sample_var")
print(myvar)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Retrieve in Scala

# COMMAND ----------

# MAGIC %scala
# MAGIC println(spark.conf.get("databricks.training.sample_var"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### More Docs
# MAGIC 
# MAGIC https://cwiki.apache.org/confluence/display/Hive/LanguageManual+VariableSubstitution