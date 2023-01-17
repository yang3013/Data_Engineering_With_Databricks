# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Performance of User Defined Functions
# MAGIC 
# MAGIC In the previous notebook we showed that it is possible to import Hive UDFs unchanged and use them in Spark. In this notebook we'll talk about when you should consider refactoring Hive UDFs during the migration.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Describe the benefits of refactoring UDFs to native Spark
# MAGIC * Explore the performance of custom logic
# MAGIC * Compare the limitations of UDFs for Spark
# MAGIC * Recommend performant solutions that limit development costs

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Do I need a UDF?
# MAGIC 
# MAGIC Before registering a UDF, you should consider whether or not this code is truly necessary. When migrating from Hive, UDFs will already be defined in the JVM; as such, in some cases you **may decide to sacrifice a small amount of performance to minimize development time for refactoring**. If your UDF proves to present a significant bottleneck in execution, refactoring to native Spark is the only option to significantly speed up execution.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Python UDFs have significant performance challenges due to data serialization. Scala and Java UDFs are more performant because Spark runs in the JVM. If you are migrating Python code and need to adapt UDFs, explore [Pandas UDFs](https://databricks.com/blog/2020/05/20/new-pandas-udfs-and-python-type-hints-in-the-upcoming-release-of-apache-spark-3-0.html).
# MAGIC 
# MAGIC ### Challenges
# MAGIC * Catalyst optimizer treats UDF as a black box
# MAGIC * Can not use optimized data transfer format tungsten
# MAGIC * UDFs can lead to poor performance
# MAGIC 
# MAGIC ### Solutions
# MAGIC * Use built in functions when possible
# MAGIC * Java or Scala UDFs perform better than Python UDFs functions
# MAGIC * See [vectorized UDFs using arrow](https://docs.databricks.com/spark/latest/spark-sql/udf-python-pandas.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Our User Defined Function
# MAGIC 
# MAGIC The UDF we're importing defines some logic for handling temperatures that have been recorded in either Farenheit or Celcius. It accepts two arguments: `unit` and `temperature`. If the unit is the string `F`, it converts the temperature to Celcius. Otherwise, it returns the temperature passed (which should already be in Celcius).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Some Data
# MAGIC 
# MAGIC Here we'll cache 20 million records, half of which are Celcius. We'll also create a clean directory to write to.

# COMMAND ----------

import numpy as np
import pandas as pd

basePath = f"{userhome}/udf_test/"
dbutils.fs.rm(basePath, True)

Fdf = pd.DataFrame(np.random.normal(55, 25, 10000000), columns=["temp"])
Fdf["unit"] = "F"

Cdf = pd.DataFrame(np.random.normal(10, 10, 10000000), columns=["temp"])
Cdf["unit"] = "C"

df = spark.createDataFrame(pd.concat([Fdf, Cdf]).sample(frac=1))

df.cache().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hive UDFs
# MAGIC 
# MAGIC As we saw in the last notebook, it's very simple to register create a temporary function using a Hive UDF packaged in a JAR.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TEMPORARY FUNCTION IF EXISTS FtoC;
# MAGIC 
# MAGIC CREATE TEMPORARY FUNCTION FtoC AS "com.databricks.training.examples.FtoCUDF" USING JAR "https://files.training.databricks.com/courses/hadoop-migration/CombinedUDF-1.0-SNAPSHOT.jar";

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC This function is registered in Spark SQL. We can use it in our DataFrame with the `.selectExpr` method.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> The `toDF` method allows us to rename all columns in the order they appear; because we have a single column, we'll pass a single string value.

# COMMAND ----------

udf_query = df.selectExpr("FtoC(unit, float(temp))").toDF("c_temp")

# COMMAND ----------

# MAGIC %md
# MAGIC To force all of our data to process, we'll write out to parquet.

# COMMAND ----------

(udf_query.write
.format("parquet")
.mode("overwrite")
.save(f"{basePath}/hive"))

# COMMAND ----------

# MAGIC %md
# MAGIC Because Hive UDFs are already in the JVM, we may be satisfied with our performance. Because these JARs are fully compiled, we don't have transparency into our logic from the Databricks environment. Additionally, Java code cannot be run directly in Databricks notebooks, so building and maintaining these functions will be a separate process from how Databricks code is developed and versioned.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python UDF
# MAGIC 
# MAGIC Python is one of the de-facto languages for many data personas. As such, it's likely that you'll encounter custom Python logic that needs to be adapted to Spark. You may find yourself on a project in which much of the Spark logic has been implemented using Python UDFs. While this limits dev time during a POC using small data, these functions can be difficult to scale.
# MAGIC 
# MAGIC Let's look at our logic as a Python UDF.

# COMMAND ----------

from pyspark.sql.functions import udf, col
from pyspark.sql.types import DoubleType

def convertFtoCpy(unitCol, tempCol):
  if unitCol == "F":
      return (tempCol - 32) * (5/9)
  else:
      return tempCol
    
FtoCpy = udf(convertFtoCpy, returnType=DoubleType())

# COMMAND ----------

# MAGIC %md
# MAGIC While recent changes to Python allow type information to be provided, it remains optional (and largely absent from much Python code). The `udf` function from the `pyspark.sql.functions` module allows you to use arbitrary Python code within your Spark DataFrame operations.

# COMMAND ----------

py_query = df.select(FtoCpy(col("unit"), col("temp"))).toDF("c_temp")

# COMMAND ----------

# MAGIC %md
# MAGIC Note that as we execute this code, our string comparison and math operations will actually happen within the Python kernel (not the JVM). As such, we'll pay a cost for data serialization/de-serialization as we move between the Python kernel and the JVM.

# COMMAND ----------

(py_query.write
.format("parquet")
.mode("overwrite")
.save(f"{basePath}/python"))

# COMMAND ----------

# MAGIC %md
# MAGIC All other things being equal, Python UDFs are usually the _slowest_ way to apply custom logic, especially as the size of your data scales.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pandas UDF
# MAGIC 
# MAGIC Pandas UDFs will generally provide a significant performance boost relative to vanilla Python. While Pandas UDFs have been supported for several years in Spark, [the 3.0 release will introduce new functionality](https://databricks.com/blog/2020/05/20/new-pandas-udfs-and-python-type-hints-in-the-upcoming-release-of-apache-spark-3-0.html) that will dramatically improve performance and simplify testing and maintenance.
# MAGIC 
# MAGIC At present, Pandas UDFs are designed for either aggregation on a DataFrame or vectorized operations that return a column. We'll need to adjust our logic to ensure that all of our operations would work if each argument were a Pandas Series.

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, PandasUDFType

@pandas_udf("float", PandasUDFType.SCALAR)
def FtoCpandas(unitCol, tempCol):
    return ((tempCol * (unitCol == "F" ) - 32) * (5/9)) + (tempCol * (unitCol != "F"))

# COMMAND ----------

# MAGIC %md
# MAGIC Once registered, Pandas UDFs are used the same way as Python UDFs.

# COMMAND ----------

pandas_query = df.select(FtoCpandas(col("unit"), col("temp"))).toDF("c_temp")

# COMMAND ----------

# MAGIC %md
# MAGIC Because Pandas UDFs use Arrow, serialization costs between the JVM and Python are drastically reduced, allowing for more efficient execution.

# COMMAND ----------

(pandas_query.write
.format("parquet")
.mode("overwrite")
.save(f"{basePath}/pandas"))

# COMMAND ----------

# MAGIC %md
# MAGIC Note that Arrow will only grab 10k records per batch by default in order to not hog the JVM. Overriding this default may improve performance.

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 2500000)

# COMMAND ----------

(pandas_query.write
.format("parquet")
.mode("overwrite")
.save(f"{basePath}/pandas_big"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scala UDFs
# MAGIC 
# MAGIC Regardless of the language you choose to work in, refactoring UDFs to Scala can be beneficial when you need truly custom logic. Because Scala executes in the JVM, there are no serilization costs. While Scala UDFs are still a black box, they are type-aware.

# COMMAND ----------

# MAGIC %scala
# MAGIC def convertFtoCscala(unitCol:String, tempCol:Float):Float = {
# MAGIC   if (unitCol == "F") 
# MAGIC     (tempCol - 32.0f) * (5.0f/9.0f)
# MAGIC   else
# MAGIC     tempCol
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC Using `spark.udf.register`, this Scala function will be registered in Spark SQL, making it available from any language.

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.udf.register("FtoCscala", convertFtoCscala _)

# COMMAND ----------

# MAGIC %md
# MAGIC From pyspark, we'll use `selectExpr`, just like with the Hive UDF we loaded from a JAR.

# COMMAND ----------

scala_query = df.selectExpr("FtoCscala(unit, float(temp))").toDF("c_temp")

# COMMAND ----------

# MAGIC %md
# MAGIC Generally, Scala UDFs should perform about the same as Hive UDFs. The main benefit to rewriting during migration would be the ability to version this code side-by-side with your Spark code.

# COMMAND ----------

(scala_query.write
.format("parquet")
.mode("overwrite")
.save(f"{basePath}/scala"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark SQL
# MAGIC 
# MAGIC Sometimes it might be easiest to define custom logic in SQL, and many customers will have huge amounts of legacy SQL code that they seek to migrate. While extremely long SQL queries can challenge the Catalyst optimizer during planning, it is generally safe to execute most logic as SQL and see optimized execution.
# MAGIC 
# MAGIC Here, we'll define our logic toward again using the `selectExpr` method.

# COMMAND ----------

FtoCsql = """
  CASE
    WHEN unit = "F" THEN (temp - 32) * (5/9)
    ELSE temp
  END
"""

# COMMAND ----------

# MAGIC %md
# MAGIC Defining SQL logic as strings makes it easy to use from the DataFrames API.

# COMMAND ----------

sql_query = df.selectExpr(FtoCsql).toDF("c_temp")

# COMMAND ----------

# MAGIC %md
# MAGIC Spark SQL should give us the maximally optimized execution.

# COMMAND ----------

(sql_query.write
.format("parquet")
.mode("overwrite")
.save(f"{basePath}/sql"))

# COMMAND ----------

# MAGIC %md
# MAGIC The main downside to using SQL is that it can be difficult to define unit tests for this logic.

# COMMAND ----------

# MAGIC %md
# MAGIC ### DataFrames and Python Glue
# MAGIC 
# MAGIC Note that there is absolutely no penalty associated with wrapping DataFrame operations within custom Python functions. All of the logic in the following cell will be executed as native Spark.

# COMMAND ----------

def convertFtoC(unitCol, tempCol):
  from pyspark.sql.functions import when
  return when(unitCol == "F", (tempCol - 32) * (5/9)).otherwise(tempCol)

# COMMAND ----------

# MAGIC %md
# MAGIC This design approach allows custom logic and repeated patterns to be reused throughout your projects. By refactoring DataFrames code into methods, testing and versioning code will become much easier.

# COMMAND ----------

df_query = df.select(convertFtoC(col("unit"), col("temp"))).toDF("c_temp")

# COMMAND ----------

# MAGIC %md
# MAGIC While this function is used the same way as a UDF, don't be fooled: all of our logic compiles and executed within the JVM. As such, we see the same optimization present in Spark SQL.

# COMMAND ----------

(df_query.write
.format("parquet")
.mode("overwrite")
.save(f"{basePath}/dataframe"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Review
# MAGIC 
# MAGIC | Type | Catalyst Optimized | Execution Environment |
# MAGIC | --- | --- | --- |
# MAGIC | Hive UDF | No | JVM |
# MAGIC | Python UDF | No | Python |
# MAGIC | Pandas UDF | No | Python (Arrow) |
# MAGIC | Scala UDF | No | JVM |
# MAGIC | Spark SQL | Yes | JVM |
# MAGIC | Spark DataFrame | Yes | JVM |
# MAGIC 
# MAGIC In terms of performance: 
# MAGIC * Built in functions will be fastest because of the Catalyst optimizer
# MAGIC * Code that executes in the JVM (Scala, Java, Hive UDFs) will be faster than Python UDFs
# MAGIC * Pandas UDFs use Arrow to reduce serialization costs associated with Python UDFs
# MAGIC * Python UDFs should generally be avoided, but Python can be used as glue code without any degradation in performance.
# MAGIC 
# MAGIC ## So Should I Refactor?
# MAGIC 
# MAGIC **Maybe**. If you are satisfied with the performance of your migrated Hive UDFs, refactoring this codebase should not take top priority. While Spark functions can be combined to execute most logic, there will be cases where UDFs may be necessary to fill in gaps. Hive UDFs have the benefit of already being in the JVM, and as such should be approximately as performant as Scala UDFs. 
# MAGIC 
# MAGIC That being said, migrating Hive UDFs written in either Java or Scala over to Scala UDFs should be a quick job, and may simplify testing and maintenance of code.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> It is possible to write Scala code that extends Spark’s native functions library. Doing so will provide an additional performance boost over Scala UDFs.
# MAGIC 
# MAGIC Ultimately, **the decision to refactor should be driven by cost and performance**. Over the next several lessons, you will learn how to identify and troubleshoot performance issues. While UDFs *can* be a performance bottleneck, there are many other areas that might take higher priority in getting a migration up to scale.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC * [New Builtin and Higher Order Functions](https://databricks.com/blog/2018/11/16/introducing-new-built-in-functions-and-higher-order-functions-for-complex-data-types-in-apache-spark.html)
# MAGIC * [Hive UDFs](https://kb.databricks.com/data/hive-udf.html)
# MAGIC * [Pandas UDF for PySpark](https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html)
# MAGIC * [Spark Hive UDF](https://github.com/bmc/spark-hive-udf)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>