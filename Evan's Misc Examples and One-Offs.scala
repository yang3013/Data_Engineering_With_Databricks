// Databricks notebook source
// MAGIC %md
// MAGIC # Evan's Misc Spark Examples and One-Offs
// MAGIC 
// MAGIC + Where can I find Azure Databricks&copy; guide to best practices?
// MAGIC + How do I get the full path to the current notebook?
// MAGIC + Notebook Context Tags
// MAGIC + How do I determine my user name programatically?
// MAGIC + How do I find my cluster's "ID"? Not the name given to my cluster.
// MAGIC + How can I programmatically retrieve the number of executors, workers, cores per worker, and total cores in the cluster? 
// MAGIC   - Is there a simpler way of programmatically retrieving the number of workers? (Yes)
// MAGIC + Properties of `spark.conf`
// MAGIC   - How do I display all the `spark.conf` properties?
// MAGIC     * How do I display just **Databricks-specific** `spark.conf` properties.
// MAGIC   - Is there a difference between `spark.conf` and `spark.sparkContext.getConf` ? Yes and No
// MAGIC   - How do I check if a configuration property exists?
// MAGIC   - How do I check if a configuration property is modifiable?
// MAGIC + Properties of `SQLConf`
// MAGIC + Versioning
// MAGIC   - What version of Spark is my cluster using?
// MAGIC   - What version of the Databricks Runtime Environment is my cluster using?
// MAGIC   - What version of Scala is my cluster running?
// MAGIC   - What version of Python is my cluster running?
// MAGIC   - What version of Java is my cluster running?
// MAGIC   - What is the Java Runtime version of my cluster?
// MAGIC + How do I create a custom `StorageLevel` for persistence (caching)?
// MAGIC + What is the syntax to `join` on two column comparisons?
// MAGIC   - Using `&&`
// MAGIC   - Using chained `.where(..)` calls
// MAGIC + How do I prevent the catalyst optimizer from inserting an `isnotnull(xxx)`.
// MAGIC + How do I create a second `SparkSession` instance?
// MAGIC + Global temporary views and their reserved namespace: `global_temp`
// MAGIC + Improving speed of listing tables using SQL instead of using the `Catalog`
// MAGIC + Limitations of `Seq.toDF(..)`
// MAGIC + `Row` objects and `==`
// MAGIC + `StructType` (schema) objects and `==`
// MAGIC + Setting the schema of a DataFrame with a simple String
// MAGIC + How do I turn a column of type array of strings into a column of single strings made up of the array elements? `functions.concat_ws`
// MAGIC + How can I programmatically get the size of my DataFrame in RAM? 
// MAGIC + Dynamically creating columns holding metadata
// MAGIC   - How do I include a column with the file name from which a row data was derived? `functions.input_file_name()`
// MAGIC   - How do I include a column with the Spark partition id for a row's data `functions.spark_partition_id()`
// MAGIC   - How do I include a column with a unique row id?  `functions.monotonically_increasing_id()`
// MAGIC + What are the configuration options for a Delta readers and writers?
// MAGIC   - Example of a `replaceWhere` option in a Delta table.
// MAGIC + How do I use Koalas in my Databricks notebook?
// MAGIC + How do I share values between Python and Scala in the same notebook?
// MAGIC + How can I share values between two different notebooks without using `%run`

// COMMAND ----------

// MAGIC %run "./Includes Clone V5.0.2/Dataset-Mounts"

// COMMAND ----------

// MAGIC %md
// MAGIC ### Where can I find Azure Databricks&copy; guide to best practices?
// MAGIC 
// MAGIC Azure Databricks best practices can be found on GitHub here [AzureDatabricksBestPractices](https://github.com/Azure/AzureDatabricksBestPractices/blob/master/toc.md)

// COMMAND ----------

// MAGIC %md
// MAGIC ### <a name="anchor1"></a>Retrieving the full path to the current notebook

// COMMAND ----------

// MAGIC %scala
// MAGIC println("Path to this notebook: %s".format(dbutils.notebook.getContext.notebookPath.get))

// COMMAND ----------

// MAGIC %python
// MAGIC print("Path to this notebook is: {}".format(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Notebook context tags collection
// MAGIC 
// MAGIC Notebook context tags in Scala are pretty straightforward.

// COMMAND ----------

// MAGIC %scala
// MAGIC val contextTagsMap = dbutils.notebook.getContext.tags
// MAGIC val fstr = "Key: %-20s Value: %s"
// MAGIC for(ctxTagMapEntryTuple <- contextTagsMap){
// MAGIC   println(fstr.format(ctxTagMapEntryTuple._1,ctxTagMapEntryTuple._2))
// MAGIC }
// MAGIC println("="*70)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC #### However, context tags in Python end up being a Scala object
// MAGIC 
// MAGIC <pre style="width:85%; color:black; background-color:#f2f2f2; 
// MAGIC            font-weight:normal; font-family:Courier New; font-size:16px;
// MAGIC             margin-left:15px; ">
// MAGIC %python
// MAGIC contextTags = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags()
// MAGIC </pre>
// MAGIC 
// MAGIC It therefore can't be iterated through with a typical Python `for` loop.

// COMMAND ----------

// MAGIC %python
// MAGIC contextTags = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags()
// MAGIC print(type(contextTags)) # produces <class 'py4j.java_gateway.JavaObject'>
// MAGIC # for element in contextTags: # <<< does not compute
// MAGIC #   print(element)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC To get around this limitation, we simply convert the Scala Map to a Python `dict`. 

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC def toPythonDict(scalaMap):
// MAGIC   keys = scalaMap.keys().iterator()
// MAGIC   pythonDict = {}
// MAGIC   while(keys.hasNex
// MAGIC     key = keys.next()
// MAGIC     pythonDict[key] = scalaMap.get(key).get()
// MAGIC   return pythonDict
// MAGIC 
// MAGIC def getNotebookContextTagsAsDict():
// MAGIC   aScalaMap = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags()
// MAGIC   return toPythonDict(aScalaMap)
// MAGIC 
// MAGIC contextTagsDict = getNotebookContextTagsAsDict()
// MAGIC fstr = fstr = "Key: {:20s} Value: {}"
// MAGIC for k in contextTagsDict:
// MAGIC   print(fstr.format(k,contextTagsDict[k]))

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### <a id="anchor1"></a>What is my user name?
// MAGIC 
// MAGIC There are **too many** ways to derive the username of a Databricks notebook. ;) 
// MAGIC 
// MAGIC <div style="color:red">
// MAGIC   You will get errors using CE when trying to use context tags. </br>
// MAGIC   java.lang.IllegalArgumentException: requirement failed: To enable notebook workflows, please upgrade your Databricks subscription.
// MAGIC   </div>
// MAGIC   

// COMMAND ----------

// MAGIC %scala
// MAGIC val fstr = "%d The username of this notebook is %s"
// MAGIC println(fstr.format(1,dbutils.notebook.getContext.tags("user")))
// MAGIC // OR
// MAGIC println(fstr.format(2,dbutils.notebook.getContext.tags.apply("user")))
// MAGIC // OR
// MAGIC println(fstr.format(3,dbutils.notebook.getContext.tags.get("user").get))
// MAGIC var username = com.databricks.logging.AttributionContext.current.tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER,"unknown")
// MAGIC println(fstr.format(4,username))
// MAGIC // OR 
// MAGIC username = com.databricks.logging.AttributionContext.current.tags.get(com.databricks.logging.BaseTagDefinitions.TAG_USER).get
// MAGIC println(fstr.format(5,username))
// MAGIC println("="*70)

// COMMAND ----------

// MAGIC %python
// MAGIC fstr = "{:d} The username of this notebook is {:s}"
// MAGIC username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply("user")
// MAGIC print(fstr.format(1, username))
// MAGIC username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("user").get()
// MAGIC print(fstr.format(2, username))

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ###What is my cluster's **ID** - Not the name given to my cluster?
// MAGIC 
// MAGIC <div style="color:red">
// MAGIC   You will get errors using CE when trying to use context tags. </div>
// MAGIC   <div style="margin-left: 35px; 
// MAGIC                width:85%; 
// MAGIC                word-wrap: break-word; 
// MAGIC                color:red; 
// MAGIC                background-color:white; 
// MAGIC                font-family:Courier New; 
// MAGIC                font-size:16px">
// MAGIC   java.lang.IllegalArgumentException: requirement failed: To enable notebook workflows, please upgrade your Databricks subscription.
// MAGIC   </div>

// COMMAND ----------

// MAGIC %scala
// MAGIC println("The cluster ID is %s".format(dbutils.notebook.getContext.tags("clusterId")))

// COMMAND ----------

// MAGIC %python
// MAGIC print("The cluster ID is {:s}".format(dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("clusterId").get()))

// COMMAND ----------

// MAGIC %md
// MAGIC ###What is my application id?

// COMMAND ----------

// MAGIC %scala
// MAGIC val fmtstr = "The application ID is %s"
// MAGIC println(fmtstr.format(spark.sparkContext.applicationId))
// MAGIC println(fmtstr.format(spark.sessionState.conf.getAllConfs.get("spark.app.id").get))

// COMMAND ----------

// MAGIC %python
// MAGIC print("The application ID is {:s}".format(spark.sparkContext.applicationId))

// COMMAND ----------

// MAGIC %md
// MAGIC ### How do I determine the Spark version of the cluster my notebook is currently attached to?

// COMMAND ----------

// same syntax in both scala and python
print(spark.version)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Determining the Databricks runtime version of the cluster
// MAGIC 
// MAGIC  <div style="color:red"> You will get errors using Community Edition when trying to use context tags.</div>
// MAGIC   
// MAGIC   <div style="margin-left: 35px; 
// MAGIC                width:85%; 
// MAGIC                word-wrap: break-word; 
// MAGIC                color:red; 
// MAGIC                background-color:white; 
// MAGIC                font-family:Courier New; 
// MAGIC                font-size:16px">
// MAGIC   java.lang.IllegalArgumentException: requirement failed: To enable notebook workflows, please upgrade your Databricks subscription.</div>
// MAGIC   

// COMMAND ----------

// MAGIC %scala
// MAGIC val fstr = "%d The Databricks runtime version is %s"
// MAGIC var dbrVersion = dbutils.notebook.getContext.tags("sparkVersion")
// MAGIC println(fstr.format(1, dbrVersion))
// MAGIC // OR
// MAGIC dbrVersion = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
// MAGIC println(fstr.format(2, dbrVersion))
// MAGIC println("="*70)

// COMMAND ----------

// MAGIC %python
// MAGIC fstr = "{:d} The Databricks runtime version is {:s}"
// MAGIC dbrVersion = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("sparkVersion").get()
// MAGIC print(fstr.format(1, dbrVersion))
// MAGIC # OR
// MAGIC dbrVersion = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
// MAGIC print(fstr.format(2, dbrVersion))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Getting the number of executors, workers, cores per worker and total cores in the cluster

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC // First, use the spark context to retrieve the executor info. This is an array of SparkExecutoInfo objects
// MAGIC val executorInfos = spark.sparkContext.statusTracker.getExecutorInfos
// MAGIC // The length of this array is the executor count
// MAGIC val executorCount = executorInfos.length
// MAGIC // However, one of these executors is for the driver so subtract 1
// MAGIC val workerCount = executorCount - 1
// MAGIC // Now we need to know the number of available processors per worker
// MAGIC val processorsPerWorker = java.lang.Runtime.getRuntime.availableProcessors
// MAGIC // Multiply the two and we have our core count
// MAGIC val coreCount = workerCount * processorsPerWorker
// MAGIC println("The number of cores in our cluster is: %d".format(coreCount))
// MAGIC println("spark.sparkContext.defaultParallelism is: %d".format(spark.sparkContext.defaultParallelism))
// MAGIC println("Executor count is %d".format(executorCount))
// MAGIC println("Worker count is %d".format(workerCount))
// MAGIC println("Processors per worker %d".format(processorsPerWorker))
// MAGIC println("="*70)

// COMMAND ----------

// MAGIC %python
// MAGIC jvm = sc._gateway.jvm
// MAGIC 
// MAGIC # First, use the spark context to retrieve the executor info. This is an array of SparkExecutoInfo objects
// MAGIC executorInfos = sc._jsc.sc().statusTracker().getExecutorInfos()
// MAGIC # The length of this array is the executor count
// MAGIC executorCount = len(executorInfos)
// MAGIC # However, one of these executors is for the driver so subtract 1
// MAGIC workerCount = executorCount - 1
// MAGIC # Now we need to know the number of available processors per worker
// MAGIC processorsPerWorker = jvm.java.lang.Runtime.getRuntime().availableProcessors()
// MAGIC # Multiply the two and we have our core count
// MAGIC coreCount = workerCount * processorsPerWorker
// MAGIC print("The number of cores in our cluster is: {:d}".format(coreCount))
// MAGIC print("spark.sparkContext.defaultParallelism is: {:d}".format(spark.sparkContext.defaultParallelism))
// MAGIC print("Executor count is {:d}".format(executorCount))
// MAGIC print("Worker count is {:d}".format(workerCount))
// MAGIC print("Processors per worker {:d}".format(processorsPerWorker))

// COMMAND ----------

// MAGIC %md
// MAGIC ### A simpler property to determine the number of workers in the cluster?

// COMMAND ----------

// MAGIC %scala
// MAGIC val workerCountProperty = "spark.databricks.clusterUsageTags.clusterWorkers" 
// MAGIC // or
// MAGIC //val workerCountProperty = "spark.databricks.clusterUsageTags.clusterTargetWorkers 
// MAGIC println("The number of workers in the current cluster is: %s".format(spark.conf.get(workerCountProperty)))
// MAGIC println("="*70)

// COMMAND ----------

// MAGIC %python
// MAGIC workerCountProperty = "spark.databricks.clusterUsageTags.clusterWorkers"
// MAGIC # or
// MAGIC # workerCountProperty = "spark.databricks.clusterUsageTags.clusterTargetWorkers"
// MAGIC valueStr = spark.conf.get(workerCountProperty)
// MAGIC valueInteger = int(valueStr)
// MAGIC print("The number of workers in the current cluster is: {:d}".format(valueInteger))

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Programmatically determining the cluster name

// COMMAND ----------

// python or scala - same syntax
print(spark.conf.get("spark.databricks.clusterUsageTags.clusterName"))

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Programmatically determining the basic cluster information
// MAGIC SparkConf has a property that summarizes basic cluster information in a simple JSON-formatted string. Don't let the name `clusterAllTags` fool you. There is much more information in the `clusterUsageTag` properties than this string summarizes. 
// MAGIC 
// MAGIC   <div style="margin-left: 35px; 
// MAGIC                width:85%; 
// MAGIC                word-wrap: break-word; 
// MAGIC                color:red; 
// MAGIC                background-color:white;                
// MAGIC                font-size:16px">
// MAGIC   The information for Community Edition will be limited to the name of the one worker node. For a real cluster, the info will include the vendor, the creator of the cluster, the cluster name and cluster ID. 
// MAGIC   </div>

// COMMAND ----------

// MAGIC %scala
// MAGIC val clusterTagsJson = spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags")
// MAGIC val clusterTagsDF = spark.read.json(spark.sparkContext.parallelize(List(clusterTagsJson)).toDS)
// MAGIC clusterTagsDF.show(truncate=false)
// MAGIC println("="*70)

// COMMAND ----------

// MAGIC %python
// MAGIC clusterTagsJson = spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags")
// MAGIC clusterTagsDF = spark.read.json(spark.sparkContext.parallelize([clusterTagsJson]))
// MAGIC clusterTagsDF.show(truncate=False)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Displaying all `spark.conf` properties

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC display(spark.conf.getAll.toSeq.toDF("property_name","property_value").sort("property_name"))

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC display(spark.createDataFrame(spark.sparkContext._conf.getAll(),["property_name","property_value"]).sort("property_name"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Display just Databricks-specific `spark.conf` properties.

// COMMAND ----------

// MAGIC %scala
// MAGIC display(spark.conf.getAll.toSeq.filter(_._1.contains("databricks")).toDF("property_name","property_value").sort("property_name"))

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import col
// MAGIC display((
// MAGIC   spark.createDataFrame(spark.sparkContext._conf.getAll(),
// MAGIC                         ["property_name","property_value"])
// MAGIC        .filter(col("property_name").contains("databricks"))
// MAGIC        .sort("property_name")
// MAGIC ))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Is there a difference between `spark.conf` and `spark.sparkContext.getConf` ?
// MAGIC 
// MAGIC Yes and no.  First, lets show that they are not two references to the same object.

// COMMAND ----------

// MAGIC %scala
// MAGIC val same = spark.conf == spark.sparkContext.getConf // compiler warning, produces false
// MAGIC println("Same? %s".format(same))
// MAGIC println("="*70)

// COMMAND ----------

// MAGIC %python
// MAGIC same = spark.conf == spark.sparkContext.getConf()
// MAGIC print("Same? {}".format(same))

// COMMAND ----------

// MAGIC %md
// MAGIC Notice above that `==` produced false. The objects aren't even instances of the same class.

// COMMAND ----------

// MAGIC %scala
// MAGIC println(spark.conf.getClass)
// MAGIC println(spark.sparkContext.getConf.getClass)
// MAGIC println("="*70)

// COMMAND ----------

// MAGIC %python
// MAGIC print(type(spark.conf))
// MAGIC print(type(spark.sparkContext.getConf()))

// COMMAND ----------

// MAGIC %md
// MAGIC However, they start with the same properties in both.  Let's look at this in Scala first, then Python.

// COMMAND ----------

val sparkConf = spark.conf
val sparkConfDF = sparkConf.getAll.toSeq.toDF("property_name_sparkConf","property_value_sparkConf")
                                        .sort($"property_name_sparkConf".asc)
val sparkConfcount = sparkConfDF.count


val scConf = sc.getConf
val scConfDF = scConf.getAll.toSeq.toDF("property_name_scConf","property_value_scConf")
                                  .sort($"property_name_scConf".asc)
val scConfcount = scConfDF.count

val joinedDF = sparkConfDF.join(scConfDF, $"property_name_sparkConf" === $"property_name_scConf" , "fullouter")
                          .withColumn("same_name_and_value", (($"property_value_sparkConf"===$"property_value_scConf") 
                                      && ($"property_name_sparkConf" === $"property_name_scConf")))
val joinedCount = joinedDF.count.toInt
val fstr = "%-10s has %4d properties\n%-10s has %4d properties\n%-10s has %4d rows"
println(fstr.format("spark.conf",sparkConfcount,"sc.getConf",scConfcount,"joined DF",joinedCount))
joinedDF.show(joinedCount,20)
//display(joinedDF)
println("="*70)

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import col
// MAGIC sparkConf = spark.conf
// MAGIC scConf = sc.getConf()
// MAGIC 
// MAGIC sparkConfDF = (spark.createDataFrame(scConf.getAll()).toDF("property_name_sparkConf","property_value_sparkConf")
// MAGIC                                                      .sort(col("property_name_sparkConf").asc()))
// MAGIC sparkConfcount = sparkConfDF.count()
// MAGIC scConfDF = (spark.createDataFrame(scConf.getAll()).toDF("property_name_scConf","property_value_scConf")
// MAGIC                                                   .sort(col("property_name_scConf").asc()))
// MAGIC scConfcount = int(sparkConfDF.count())
// MAGIC joinedDF = (sparkConfDF.join(scConfDF, col("property_name_sparkConf") == col("property_name_scConf") , "fullouter")
// MAGIC             .withColumn("same_name_and_value", ((col("property_value_sparkConf") == col("property_value_scConf"))
// MAGIC                                       & (col("property_name_sparkConf") == col("property_name_scConf")))) )
// MAGIC joinedCount = int(joinedDF.count())
// MAGIC fstr = "{:14s} has {:4d} properties\n{:14s} has {:4d} properties\n{:14s} has {:4d} rows"
// MAGIC print(fstr.format("spark.conf",sparkConfcount,"sc.getConf()",scConfcount,"joined DF",joinedCount))
// MAGIC joinedDF.show(joinedCount,20)
// MAGIC #display(joinedDF)
// MAGIC print("="*50)

// COMMAND ----------

// MAGIC %md
// MAGIC A quick scan of the joined table above and we can see that these property objects have the same initial set properties.

// COMMAND ----------

// MAGIC %scala
// MAGIC display(joinedDF)

// COMMAND ----------

// MAGIC %python
// MAGIC display(joinedDF)

// COMMAND ----------

// MAGIC %md
// MAGIC #### What happens if I place a property into `spark.conf` object only. Is it seen in the `spark.sparkContext.getConf()` object?
// MAGIC 
// MAGIC The answer is no. See the code below that demonstrates this. 

// COMMAND ----------

// MAGIC %scala
// MAGIC val testPropertyName = "abc"
// MAGIC val testPropertyValue = "123"
// MAGIC spark.conf.set(testPropertyName,testPropertyValue)
// MAGIC val retrieved = spark.conf.get(testPropertyName)
// MAGIC println("Value for property '%s' set in spark.conf: %s".format(testPropertyName,retrieved))
// MAGIC val exists = spark.sparkContext.getConf.contains(testPropertyName)
// MAGIC println("Does property '%s' exist in spark.sparkContext.getConf? %s".format(testPropertyName,exists))
// MAGIC println("="*70)

// COMMAND ----------

// MAGIC %python
// MAGIC testPropertyName = "abc"
// MAGIC testPropertyValue = "123"
// MAGIC spark.conf.set(testPropertyName,testPropertyValue)
// MAGIC retrieved = spark.conf.get(testPropertyName)
// MAGIC print("Value for property '{}' set in spark.conf: {}".format(testPropertyName,retrieved))
// MAGIC exists = spark.sparkContext.getConf().contains(testPropertyName)
// MAGIC print("Does property '{}' exist in spark.sparkContext.getConf? {}".format(testPropertyName, exists))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Checking if a particular Spark configuration property exists
// MAGIC 
// MAGIC Remember from above that there are two places spark configuration properties could exist.
// MAGIC + `spark.conf`
// MAGIC + `spark.sparkContext.getConf()` a.k.a. `sc.getConf()`
// MAGIC 
// MAGIC The two objects have different classes/types and thus different APIs.
// MAGIC 
// MAGIC |Language  |Object                        |Class/Type                          |
// MAGIC |----------|------                        |-------------------------           |
// MAGIC |Scala     |`spark.conf`                  |`org.apache.spark.sql.RuntimeConfig`|
// MAGIC |Scala     |`spark.sparkContext.getConf`  |`org.apache.spark.SparkConf`        |
// MAGIC |Python    |`spark.conf`                  |`pyspark.sql.conf.RuntimeConfig`    |
// MAGIC |Python    |`spark.sparkContext.getConf()`|`pyspark.conf.SparkConf`            |
// MAGIC 
// MAGIC 
// MAGIC Some notable differences with the APIs for the two different object classes/types.
// MAGIC 
// MAGIC **Scala**
// MAGIC 
// MAGIC |`spark.conf` [org.apache.spark.sql.RuntimeConfig](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.RuntimeConfig)    |`sc.getConf` [org.apache.spark.SparkConf](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.SparkConf)                                  |
// MAGIC |---------------------------------------            |------------------------------------                                     |
// MAGIC |limited set of fuctions                            | much larger set of functions                                            |
// MAGIC |no type-specific `getXXX` functions                |`getInt("abc",7)`, `getDouble("abc",7.0)`,<br/>`getBoolean("abc",false)` etc  |
// MAGIC |has `isModifiable` function                        |N/A                                                                      |
// MAGIC |N/A                                                |has `contains` function                                                  |
// MAGIC |has an `unset` function                            |has a `remove` function                                                  |  
// MAGIC |has only one `set` function                        |has Spark-specific `set` fuctions like `setAppName`<br/> and `setMaster`       |
// MAGIC |N/A                                                |has a `setJars` function that distributes jars to the cluster            |
// MAGIC 
// MAGIC 
// MAGIC **Python**
// MAGIC 
// MAGIC |`spark.conf` [pyspark.sql.conf.RuntimeConfig](https://spark.apache.org/docs/2.4.3/api/python/pyspark.sql.html#pyspark.sql.SparkSession.conf)    |`sc.getConf` [pyspark.conf.SparkConf](https://spark.apache.org/docs/2.4.3/api/python/pyspark.html?highlight=sparkconf#pyspark.SparkConf)                                  |
// MAGIC |---------------------------------------            |------------------------------------                                     |
// MAGIC |limited set of functions                           |slightly larger set of functions |
// MAGIC |has `isModifiable` function                        |N/A                                                                      |
// MAGIC |N/A                                                |has `contains` function                                                  |
// MAGIC |has an `unset` function                            |has neither an `unset` nor a `remove` function                           |  
// MAGIC |N/A                                                |has a `toDebugString()` function
// MAGIC 
// MAGIC 
// MAGIC Let's demonstrate checking in  `spark.conf` first.

// COMMAND ----------

// MAGIC %scala
// MAGIC var examplePropertyName1 = "xyz.abc"
// MAGIC var doesItExist1 = spark.conf.getOption(examplePropertyName1) != None
// MAGIC var fstr = "Does property '%s' exist in spark.conf? %s"
// MAGIC println(fstr.format(examplePropertyName1, doesItExist1))
// MAGIC val examplePropertyName2 = "spark.databricks.preemption.enabled"
// MAGIC val doesItExist2 = spark.conf.getOption(examplePropertyName2) != None
// MAGIC println(fstr.format(examplePropertyName2,doesItExist2))
// MAGIC spark.conf.unset(examplePropertyName1)
// MAGIC println("="*70)

// COMMAND ----------

// MAGIC %python
// MAGIC examplePropertyName1 = "xyz.abc"
// MAGIC doesItExist1 = spark.conf.get(examplePropertyName1, None) != None
// MAGIC fstr = "Does property '{}' exist in spark.conf? {}"
// MAGIC print(fstr.format(examplePropertyName1, str(doesItExist1)))
// MAGIC examplePropertyName2 = "spark.databricks.preemption.enabled"
// MAGIC doesItExist2 = spark.conf.get(examplePropertyName2,None) != None
// MAGIC print(fstr.format(examplePropertyName2,str(doesItExist2)))
// MAGIC spark.conf.unset(examplePropertyName1)

// COMMAND ----------

// MAGIC %md
// MAGIC Now we will demonstrate checking in `spark.sparkContext.getConf`

// COMMAND ----------

// MAGIC %scala
// MAGIC var examplePropertyName1 = "xyz.abc"
// MAGIC var doesItExist1 = spark.sparkContext.getConf.getOption(examplePropertyName1) != None
// MAGIC fstr = "Does property '%s' exist in spark.sparkContext.getConf? %s"
// MAGIC println(fstr.format(examplePropertyName1,doesItExist1))
// MAGIC val examplePropertyName2 = "spark.databricks.preemption.enabled"
// MAGIC val doesItExist2 = spark.sparkContext.getConf.getOption(examplePropertyName2) != None
// MAGIC println(fstr.format(examplePropertyName2,doesItExist2))
// MAGIC spark.sparkContext.getConf.remove(examplePropertyName1)
// MAGIC println("="*70)

// COMMAND ----------

// MAGIC %python
// MAGIC examplePropertyName1 = "xyz.abc"
// MAGIC doesItExist1 = spark.sparkContext.getConf().get(examplePropertyName1, None) != None
// MAGIC fstr = "Does property '{}' exist in spark.sparkContext.getConf? {}"
// MAGIC print(fstr.format(examplePropertyName1,str(doesItExist1)))
// MAGIC examplePropertyName2 = "spark.databricks.preemption.enabled"
// MAGIC doesItExist2 = spark.sparkContext.getConf().get(examplePropertyName2,None) != None
// MAGIC print(fstr.format(examplePropertyName2, str(doesItExist2)))
// MAGIC spark.sparkContext.getConf().set(examplePropertyName1,None)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Checking if a particular Spark configuration property is modifiable

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC // spark.conf has a function isModifiable - this function is not found in spark.sparkContext.getConf
// MAGIC val examplePropertyName1 = "spark.databricks.preemption.enabled"
// MAGIC println("Is property '%s' modfiable? %s".format(examplePropertyName1, spark.conf.isModifiable(examplePropertyName1)))
// MAGIC 
// MAGIC println("="*50)

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC # spark.conf has a function isModifiable - this function is not found in spark.sparkContext.getConf()
// MAGIC examplePropertyName = "spark.databricks.preemption.enabled"
// MAGIC print("Is property '{}'' modfiable? {}".format(examplePropertyName, spark.conf.isModifiable(examplePropertyName)))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Properties in `SQLConf`
// MAGIC 
// MAGIC The SQL Engine of your Spark environment has a configuration object separate from `spark.conf` and `spark.sparkContext.getConf()`. The class of this config object is `org.apache.spark.sql.internal.SQLConf`.  You can get access to this config object and get values as well as set some values. 

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC | Property Name 	| Property Value 	|
// MAGIC |---------------	|----------------	|
// MAGIC |eventLog.rolloverIntervalSeconds  	|3600  	|
// MAGIC |spark.akka.frameSize  	|256  	|
// MAGIC |spark.app.id  	|app-20190718113302-0000  	|
// MAGIC |spark.app.name  	|Databricks Shell  	|
// MAGIC |spark.cleaner.referenceTracking.blocking  	|false  	|
// MAGIC |spark.databricks.acl.client  	|com.databricks.spark.sql.acl.client.SparkSqlAclClient  	|
// MAGIC |spark.databricks.acl.provider  	|com.databricks.sql.acl.ReflectionBackedAclProvider  	|
// MAGIC |spark.databricks.cloudProvider  	|AWS  	|
// MAGIC |spark.databricks.clusterSource  	|UI  	|
// MAGIC |spark.databricks.clusterUsageTags.autoTerminationMinutes  	|120  	|
// MAGIC |spark.databricks.clusterUsageTags.clusterAllTags  	|[{"key":"Vendor","value":"Databricks"},{"key":"Creator","value":"evan.troyka@dat...*[too long to display]*  	|
// MAGIC |spark.databricks.clusterUsageTags.clusterAvailability  	|SPOT_WITH_FALLBACK  	|
// MAGIC |spark.databricks.clusterUsageTags.clusterCreator  	|Webapp  	|
// MAGIC |spark.databricks.clusterUsageTags.clusterEbsVolumeCount  	|3  	|
// MAGIC |spark.databricks.clusterUsageTags.clusterEbsVolumeSize  	|100  	|
// MAGIC |spark.databricks.clusterUsageTags.clusterEbsVolumeType  	|GENERAL_PURPOSE_SSD  	|
// MAGIC |spark.databricks.clusterUsageTags.clusterFirstOnDemand  	|3  	|
// MAGIC |spark.databricks.clusterUsageTags.clusterGeneration  	|62  	|
// MAGIC |spark.databricks.clusterUsageTags.clusterId  	|0514-115341-brad4  	|
// MAGIC |spark.databricks.clusterUsageTags.clusterLastActivityTime  	|1563403620176  	|
// MAGIC |spark.databricks.clusterUsageTags.clusterLogDeliveryEnabled  	|true  	|
// MAGIC |spark.databricks.clusterUsageTags.clusterLogDestination  	|dbfs:/user/evan.troyka@databricks.com/cluster-logs  	|
// MAGIC |spark.databricks.clusterUsageTags.clusterMetastoreAccessType  	|RDS_DIRECT  	|
// MAGIC |spark.databricks.clusterUsageTags.clusterName  	|Evan prep and test  	|
// MAGIC |spark.databricks.clusterUsageTags.clusterNoDriverDaemon  	|false  	|
// MAGIC |spark.databricks.clusterUsageTags.clusterNodeType  	|m4.xlarge  	|
// MAGIC |spark.databricks.clusterUsageTags.clusterNumSshKeys  	|0  	|
// MAGIC |spark.databricks.clusterUsageTags.clusterOwnerOrgId  	|0  	|
// MAGIC |spark.databricks.clusterUsageTags.clusterOwnerUserId  	|100144  	|
// MAGIC |spark.databricks.clusterUsageTags.clusterPinned  	|false  	|
// MAGIC |spark.databricks.clusterUsageTags.clusterPythonVersion  	|3  	|
// MAGIC |spark.databricks.clusterUsageTags.clusterResourceClass  	|default  	|
// MAGIC |spark.databricks.clusterUsageTags.clusterScalingType  	|fixed_size  	|
// MAGIC |spark.databricks.clusterUsageTags.clusterSku  	|STANDARD_SKU  	|
// MAGIC |spark.databricks.clusterUsageTags.clusterSpotBidPricePercent  	|100  	|
// MAGIC |spark.databricks.clusterUsageTags.clusterState  	|Pending  	|
// MAGIC |spark.databricks.clusterUsageTags.clusterStateMessage  	|Starting Spark  	|
// MAGIC |spark.databricks.clusterUsageTags.clusterTargetWorkers  	|2  	|
// MAGIC |spark.databricks.clusterUsageTags.clusterWorkers  	|2  	|
// MAGIC |spark.databricks.clusterUsageTags.containerType  	|LXC  	|
// MAGIC |spark.databricks.clusterUsageTags.containerZoneId  	|us-west-1c  	|
// MAGIC |spark.databricks.clusterUsageTags.driverContainerId  	|a9a27e82019e4bc5aff589cea7a81ddb  	|
// MAGIC |spark.databricks.clusterUsageTags.driverContainerPrivateIp  	|10.229.248.17  	|
// MAGIC |spark.databricks.clusterUsageTags.driverInstanceId  	|i-06e039c42b2e69502  	|
// MAGIC |spark.databricks.clusterUsageTags.driverInstancePrivateIp  	|10.229.234.228  	|
// MAGIC |spark.databricks.clusterUsageTags.driverNodeType  	|m4.xlarge  	|
// MAGIC |spark.databricks.clusterUsageTags.driverPublicDns  	|ec2-52-53-249-107.us-west-1.compute.amazonaws.com  	|
// MAGIC |spark.databricks.clusterUsageTags.enableCredentialPassthrough  	|false  	|
// MAGIC |spark.databricks.clusterUsageTags.enableDfAcls  	|false  	|
// MAGIC |spark.databricks.clusterUsageTags.enableElasticDisk  	|false  	|
// MAGIC |spark.databricks.clusterUsageTags.enableJdbcAutoStart  	|true  	|
// MAGIC |spark.databricks.clusterUsageTags.enableJobsAutostart  	|true  	|
// MAGIC |spark.databricks.clusterUsageTags.enableSqlAclsOnly  	|false  	|
// MAGIC |spark.databricks.clusterUsageTags.numPerClusterInitScriptsV2  	|0  	|
// MAGIC |spark.databricks.clusterUsageTags.sparkVersion  	|5.4.x-scala2.11  	|
// MAGIC |spark.databricks.clusterUsageTags.userProvidedRemoteVolumeCount  	|3  	|
// MAGIC |spark.databricks.clusterUsageTags.userProvidedRemoteVolumeSizeGb  	|100  	|
// MAGIC |spark.databricks.clusterUsageTags.userProvidedRemoteVolumeType  	|ebs_volume_type: GENERAL_PURPOSE_SSD
// MAGIC   	|
// MAGIC |spark.databricks.clusterUsageTags.workerEnvironmentId  	|default-worker-env  	|
// MAGIC |spark.databricks.credential.redactor  	|com.databricks.logging.secrets.CredentialRedactorProxyImpl  	|
// MAGIC |spark.databricks.delta.logStore.crossCloud.fatal  	|true  	|
// MAGIC |spark.databricks.delta.multiClusterWrites.enabled  	|true  	|
// MAGIC |spark.databricks.driverNodeTypeId  	|m4.xlarge  	|
// MAGIC |spark.databricks.eventLog.dir  	|eventlogs  	|
// MAGIC |spark.databricks.io.directoryCommit.enableLogicalDelete  	|false  	|
// MAGIC |spark.databricks.overrideDefaultCommitProtocol  	|org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol  	|
// MAGIC |spark.databricks.passthrough.adls.gen2.tokenProviderClassName  	|com.databricks.backend.daemon.data.client.adl.AdlGen2CredentialContextTokenProvider  	|
// MAGIC |spark.databricks.passthrough.adls.tokenProviderClassName  	|com.databricks.backend.daemon.data.client.adl.AdlCredentialContextTokenProvider  	|
// MAGIC |spark.databricks.passthrough.s3a.tokenProviderClassName  	|com.databricks.backend.daemon.driver.aws.AwsCredentialContextTokenProvider  	|
// MAGIC |spark.databricks.preemption.enabled  	|true  	|
// MAGIC |spark.databricks.redactor  	|com.databricks.spark.util.DatabricksSparkLogRedactorProxy  	|
// MAGIC |spark.databricks.service.server.enabled  	|true  	|
// MAGIC |spark.databricks.session.share  	|false  	|
// MAGIC |spark.databricks.sparkContextId  	|1024761533536797187  	|
// MAGIC |spark.databricks.tahoe.logStore.aws.class  	|com.databricks.tahoe.store.MultiClusterLogStore  	|
// MAGIC |spark.databricks.tahoe.logStore.azure.class  	|com.databricks.tahoe.store.AzureLogStore  	|
// MAGIC |spark.databricks.tahoe.logStore.class  	|com.databricks.tahoe.store.DelegatingLogStore  	|
// MAGIC |spark.databricks.workerNodeTypeId  	|m4.xlarge  	|
// MAGIC |spark.driver.allowMultipleContexts  	|false  	|
// MAGIC |spark.driver.host  	|ip-10-229-248-17.us-west-1.compute.internal  	|
// MAGIC |spark.driver.maxResultSize  	|4g  	|
// MAGIC |spark.driver.port  	|44977  	|
// MAGIC |spark.driver.tempDirectory  	|/local_disk0/tmp  	|
// MAGIC |spark.eventLog.enabled  	|false  	|
// MAGIC |spark.executor.extraClassPath  	|/databricks/spark/dbconf/log4j/executor:/databricks/spark/dbconf/jets3t/:/databr...*[too long to display]*  	|
// MAGIC |spark.executor.extraJavaOptions  	|-Djava.io.tmpdir=/local_disk0/tmp -XX:ReservedCodeCacheSize=256m -XX:+UseCodeCac...*[too long to display]*  	|
// MAGIC |spark.executor.id  	|driver  	|
// MAGIC |spark.executor.memory  	|8873m  	|
// MAGIC |spark.executor.tempDirectory  	|/local_disk0/tmp  	|
// MAGIC |spark.extraListeners  	|com.databricks.backend.daemon.driver.DBCEventLoggingListener  	|
// MAGIC |spark.files.fetchFailure.unRegisterOutputOnHost  	|true  	|
// MAGIC |spark.files.overwrite  	|true  	|
// MAGIC |spark.files.useFetchCache  	|false  	|
// MAGIC |spark.hadoop.databricks.dbfs.client.version  	|v2  	|
// MAGIC |spark.hadoop.databricks.s3commit.client.sslTrustAll  	|false  	|
// MAGIC |spark.hadoop.fs.abfs.impl  	|shaded.databricks.v20180920_b33d810.org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem  	|
// MAGIC |spark.hadoop.fs.abfs.impl.disable.cache  	|true  	|
// MAGIC |spark.hadoop.fs.abfss.impl  	|shaded.databricks.v20180920_b33d810.org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem  	|
// MAGIC |spark.hadoop.fs.abfss.impl.disable.cache  	|true  	|
// MAGIC |spark.hadoop.fs.adl.impl  	|com.databricks.adl.AdlFileSystem  	|
// MAGIC |spark.hadoop.fs.adl.impl.disable.cache  	|true  	|
// MAGIC |spark.hadoop.fs.azure.skip.metrics  	|true  	|
// MAGIC |spark.hadoop.fs.s3.impl  	|com.databricks.s3a.S3AFileSystem  	|
// MAGIC |spark.hadoop.fs.s3a.connection.maximum  	|200  	|
// MAGIC |spark.hadoop.fs.s3a.fast.upload  	|true  	|
// MAGIC |spark.hadoop.fs.s3a.fast.upload.default  	|true  	|
// MAGIC |spark.hadoop.fs.s3a.impl  	|com.databricks.s3a.S3AFileSystem  	|
// MAGIC |spark.hadoop.fs.s3a.multipart.size  	|10485760  	|
// MAGIC |spark.hadoop.fs.s3a.multipart.threshold  	|104857600  	|
// MAGIC |spark.hadoop.fs.s3a.threads.max  	|136  	|
// MAGIC |spark.hadoop.fs.s3n.impl  	|com.databricks.s3a.S3AFileSystem  	|
// MAGIC |spark.hadoop.fs.wasb.impl  	|shaded.databricks.org.apache.hadoop.fs.azure.NativeAzureFileSystem  	|
// MAGIC |spark.hadoop.fs.wasb.impl.disable.cache  	|true  	|
// MAGIC |spark.hadoop.fs.wasbs.impl  	|shaded.databricks.org.apache.hadoop.fs.azure.NativeAzureFileSystem  	|
// MAGIC |spark.hadoop.fs.wasbs.impl.disable.cache  	|true  	|
// MAGIC |spark.hadoop.hive.server2.enable.doAs  	|false  	|
// MAGIC |spark.hadoop.hive.server2.idle.operation.timeout  	|7200000  	|
// MAGIC |spark.hadoop.hive.server2.idle.session.timeout  	|900000  	|
// MAGIC |spark.hadoop.hive.server2.keystore.password  	|gb1gQqZ9ZIHS  	|
// MAGIC |spark.hadoop.hive.server2.keystore.path  	|/databricks/keys/jetty-ssl-driver-keystore.jks  	|
// MAGIC |spark.hadoop.hive.server2.session.check.interval  	|60000  	|
// MAGIC |spark.hadoop.hive.server2.thrift.http.cookie.auth.enabled  	|false  	|
// MAGIC |spark.hadoop.hive.server2.thrift.http.port  	|10000  	|
// MAGIC |spark.hadoop.hive.server2.transport.mode  	|http  	|
// MAGIC |spark.hadoop.hive.server2.use.SSL  	|true  	|
// MAGIC |spark.hadoop.hive.warehouse.subdir.inherit.perms  	|false  	|
// MAGIC |spark.hadoop.mapred.output.committer.class  	|com.databricks.backend.daemon.data.client.DirectOutputCommitter  	|
// MAGIC |spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version  	|2  	|
// MAGIC |spark.hadoop.parquet.memory.pool.ratio  	|0.5  	|
// MAGIC |spark.hadoop.spark.sql.parquet.output.committer.class  	|org.apache.spark.sql.parquet.DirectParquetOutputCommitter  	|
// MAGIC |spark.hadoop.spark.sql.sources.outputCommitterClass  	|com.databricks.backend.daemon.data.client.MapReduceDirectOutputCommitter  	|
// MAGIC |spark.hadoop.spark.thriftserver.customHeadersToProperties  	|X-Databricks-User-Token:spark.databricks.token,X-Databricks-Api-Url:spark.databr...*[too long to display]*  	|
// MAGIC |spark.home  	|/databricks/spark  	|
// MAGIC |spark.logConf  	|true  	|
// MAGIC |spark.master  	|spark://10.229.248.17:7077  	|
// MAGIC |spark.metrics.conf  	|/databricks/spark/conf/metrics.properties  	|
// MAGIC |spark.r.backendConnectionTimeout  	|604800  	|
// MAGIC |spark.r.numRBackendThreads  	|1  	|
// MAGIC |spark.r.sql.derby.temp.dir  	|/tmp/RtmpEcPc2Q  	|
// MAGIC |spark.rdd.compress  	|true  	|
// MAGIC |spark.repl.class.outputDir  	|/local_disk0/tmp/repl/spark-1024761533536797187-dd00efa4-aff6-4991-a54e-9290c8c988c0  	|
// MAGIC |spark.repl.class.uri  	|spark://ip-10-229-248-17.us-west-1.compute.internal:44977/classes  	|
// MAGIC |spark.rpc.message.maxSize  	|256  	|
// MAGIC |spark.scheduler.listenerbus.eventqueue.capacity  	|20000  	|
// MAGIC |spark.scheduler.mode  	|FAIR  	|
// MAGIC |spark.serializer.objectStreamReset  	|100  	|
// MAGIC |spark.shuffle.manager  	|SORT  	|
// MAGIC |spark.shuffle.memoryFraction  	|0.2  	|
// MAGIC |spark.shuffle.reduceLocality.enabled  	|false  	|
// MAGIC |spark.shuffle.service.enabled  	|true  	|
// MAGIC |spark.shuffle.service.port  	|4048  	|
// MAGIC |spark.sparkr.use.daemon  	|false  	|
// MAGIC |spark.speculation  	|false  	|
// MAGIC |spark.speculation.multiplier  	|3  	|
// MAGIC |spark.speculation.quantile  	|0.9  	|
// MAGIC |spark.sql.allowMultipleContexts  	|false  	|
// MAGIC |spark.sql.catalogImplementation  	|hive  	|
// MAGIC |spark.sql.cbo.enabled  	|true  	|
// MAGIC |spark.sql.cbo.joinReorder.enabled  	|true  	|
// MAGIC |spark.sql.hive.convertCTAS  	|true  	|
// MAGIC |spark.sql.hive.convertMetastoreParquet  	|true  	|
// MAGIC |spark.sql.hive.metastore.jars  	|/databricks/hive/*  	|
// MAGIC |spark.sql.hive.metastore.sharedPrefixes  	|org.mariadb.jdbc,com.mysql.jdbc,org.postgresql,com.microsoft.sqlserver,microsoft...*[too long to display]*  	|
// MAGIC |spark.sql.hive.metastore.version  	|0.13.0  	|
// MAGIC |spark.sql.parquet.cacheMetadata  	|true  	|
// MAGIC |spark.sql.parquet.compression.codec  	|snappy  	|
// MAGIC |spark.sql.sources.commitProtocolClass  	|com.databricks.sql.transaction.directory.DirectoryAtomicCommitProtocol  	|
// MAGIC |spark.sql.streaming.checkpointFileManagerClass  	|com.databricks.spark.sql.streaming.DatabricksCheckpointFileManager  	|
// MAGIC |spark.sql.ui.retainedExecutions  	|100  	|
// MAGIC |spark.sql.warehouse.dir  	|/user/hive/warehouse  	|
// MAGIC |spark.storage.blockManagerTimeoutIntervalMs  	|300000  	|
// MAGIC |spark.storage.memoryFraction  	|0.5  	|
// MAGIC |spark.streaming.driver.writeAheadLog.allowBatching  	|true  	|
// MAGIC |spark.streaming.driver.writeAheadLog.closeFileAfterWrite  	|true  	|
// MAGIC |spark.task.reaper.enabled  	|true  	|
// MAGIC |spark.task.reaper.killTimeout  	|60s  	|
// MAGIC |spark.ui.port  	|42216  	|
// MAGIC |spark.worker.cleanup.enabled  	|false  	|

// COMMAND ----------

// MAGIC %scala
// MAGIC // evan WORK IN PROGRESS
// MAGIC val sqlConf = spark.sessionState.conf
// MAGIC val allEntriesMap = sqlConf.getAllConfs
// MAGIC //println(all.getClass.getName)
// MAGIC //allEntriesMap.foreach(println)
// MAGIC val keys = allEntriesMap.keys.toList.sortWith(_ < _)
// MAGIC //println(keys.getClass.getName)
// MAGIC //keys.foreach(println)
// MAGIC println("\n")
// MAGIC println("| Property Name 	| Property Value 	|")
// MAGIC println("|---------------	|----------------	|")
// MAGIC def pline(k:String):Unit = {
// MAGIC   var value = allEntriesMap.get(k).get
// MAGIC   if(value.length>150){
// MAGIC     value = value.substring(0,80)+"...*[too long to display]*"
// MAGIC   }
// MAGIC   println("|"+k+"  	|"+value+"  	|")
// MAGIC }
// MAGIC keys.foreach(key => pline(key))
// MAGIC println("\n"+("*****"*15))

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### What version of Scala is my cluster running?

// COMMAND ----------

// MAGIC %scala
// MAGIC val sv = dbutils.notebook.getContext.tags("sparkVersion")
// MAGIC val scalaVersion = sv.split("-")(1).replaceAll("scala","")
// MAGIC println("The Scala version: %s".format(scalaVersion))
// MAGIC println("="*70)

// COMMAND ----------

// MAGIC %python
// MAGIC scalaVersion = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("sparkVersion").get().split("-")[1].replace("scala","")
// MAGIC print("The Scala version: {}".format(scalaVersion))

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### What version of Python is my cluster running?

// COMMAND ----------

// MAGIC %scala
// MAGIC val pyVer = spark.conf.get("spark.databricks.clusterUsageTags.clusterPythonVersion")
// MAGIC println("Python version: %s".format(pyVer))
// MAGIC println("="*70)

// COMMAND ----------

// MAGIC %python
// MAGIC pyMajorVer = spark.conf.get("spark.databricks.clusterUsageTags.clusterPythonVersion")
// MAGIC print("Python major version: {}".format(pyMajorVer))
// MAGIC 
// MAGIC # Or more specifically
// MAGIC import platform
// MAGIC pyFullVer =  platform.python_version()
// MAGIC print("Python full  version: {}".format(pyFullVer))

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### What version of Java is my cluster running?

// COMMAND ----------

// MAGIC %scala
// MAGIC import scala.collection.JavaConverters._
// MAGIC val jVer = java.lang.System.getProperties().asScala.get("java.version").get
// MAGIC val jrunVer = java.lang.System.getProperties().asScala.get("java.runtime.version").get
// MAGIC println("Java version: %s".format(jVer))
// MAGIC println("Java runtime version: %s".format(jrunVer))
// MAGIC println("="*70)

// COMMAND ----------

// MAGIC %python
// MAGIC jVer =  sc._jvm.java.lang.System.getProperties().get("java.version")
// MAGIC jrunVer = sc._jvm.java.lang.System.getProperties().get("java.runtime.version")
// MAGIC print("Java version: {}".format(jVer))
// MAGIC print("Java runtime version: {}".format(jrunVer))

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ###Create a custom StorageLevel with 3X replication instead of 2X, memory and disk, and serialize if memory.

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.storage.StorageLevel
// MAGIC val MY_MEMORY_AND_DISK_SER_3 = 
// MAGIC   StorageLevel.apply(useDisk=true, 
// MAGIC                      useMemory=true,
// MAGIC                      useOffHeap=false,
// MAGIC                      deserialized=false,
// MAGIC                      replication=3)
// MAGIC println("MY_MEMORY_AND_DISK_SER_3: %s".format(MY_MEMORY_AND_DISK_SER_3))
// MAGIC println("="*70)

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark import StorageLevel
// MAGIC MY_MEMORY_AND_DISK_SER_3 = (StorageLevel(useDisk=True, 
// MAGIC                                          useMemory=True, 
// MAGIC                                          useOffHeap=False, 
// MAGIC                                          deserialized=False, 
// MAGIC                                          replication=3))
// MAGIC print("MY_MEMORY_AND_DISK_SER_3: {}".format(MY_MEMORY_AND_DISK_SER_3))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Joining on 2 columns using `&&` or `&` and chaining `.where` calls.

// COMMAND ----------

// MAGIC %md 
// MAGIC  First, we'll need a couple of DataFrames for demonstation purposes. 

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC val custsDF = Seq((1,"John", "Doe",   "Main", "Akron",     "OH",  false,  1000.00),
// MAGIC                   (2,"Bob",  "Bates", "Birch","Boise",     "ID",   true, 11000.00),
// MAGIC                   (3,"Carol","Carson","Oak",  "Chicago",   "IL",  false,  1200.00),
// MAGIC                   (4,"Ted",  "Talk",  "Elm",  "Trenton",   "NJ",   true,  1300.00),
// MAGIC                   (5,"Alice","Able",  "West", "Washington","DC",  false,  1400.00)
// MAGIC                 ).toDF("cust_id", "firstName", "lastName", "street", "city", "state", "active", "credit_limit")
// MAGIC                     .withColumn("credit_limit",$"credit_limit".cast("decimal(10,2)"))
// MAGIC 
// MAGIC val ordersDF = Seq(("1", 5,  "OH",  6.5, false, true,  500.50),
// MAGIC                    ("2", 4,  "IN",  5.0, true,  false, 100.00),
// MAGIC                    ("3", 3,  "HI",  9.0, false, false,  44.25),
// MAGIC                    ("4", 2,  "WY",  7.5, true,  true,    9.55),
// MAGIC                    ("5", 1,  "FL",  6.5, false, true,    1.70),
// MAGIC                    ("6", 4,  "NJ",  6.5, false, true,  223.98),
// MAGIC                    ("7", 4,  "OH",  6.5, false, true,  102.00),
// MAGIC                    ("8", 3,  "IL",  6.5, false, true,   31.22),
// MAGIC                    ("9", 5,  "MO",  6.5, false, true,  670.66)
// MAGIC                 ).toDF("order_id", "cust_id", "supplier_state", "sales_tax_rate", "filled", "open", "order_total")
// MAGIC                  .withColumn("order_total",$"order_total".cast("decimal(10,2)"))
// MAGIC 
// MAGIC //custsDF.printSchema
// MAGIC //ordersDF.printSchema
// MAGIC //println("="*70)

// COMMAND ----------

// MAGIC %python 
// MAGIC 
// MAGIC from pyspark.sql.functions import col
// MAGIC custsDF = ( 
// MAGIC   spark.createDataFrame([(1,"John",  "Doe",    "Main",  "Akron",      "OH", False, 1000.00), 
// MAGIC                          (2,"Bob",   "Bates",  "Birch", "Boise",      "ID", True , 11000.00), 
// MAGIC                          (3,"Carol", "Carson", "Oak",   "Chicago",    "IL", False, 1200.00), 
// MAGIC                          (4,"Ted",   "Talk",   "Elm",   "Trenton",    "NJ", True , 1300.00), 
// MAGIC                          (5,"Alice", "Able",   "West",  "Washington", "DC", False, 1400.00)] )
// MAGIC     .toDF("cust_id", "firstName", "lastName", "street", "city", "state", "active", "credit_limit")
// MAGIC     .withColumn("credit_limit",col("credit_limit").cast("decimal(10,2)"))
// MAGIC )
// MAGIC 
// MAGIC ordersDF =  ( 
// MAGIC    spark.createDataFrame([("1", 5,  "OH",  6.5, False, True,  500.50), 
// MAGIC                           ("2", 4,  "IN",  5.0, True , False, 100.00), 
// MAGIC                           ("3", 3,  "HI",  9.0, False, False,  44.25), 
// MAGIC                           ("4", 2,  "WY",  7.5, True,  True,    9.55), 
// MAGIC                           ("5", 1,  "FL",  6.5, False, True,    1.70), 
// MAGIC                           ("6", 4,  "NJ",  6.5, False, True,  223.98), 
// MAGIC                           ("7", 4,  "OH",  6.5, False, True,  102.00), 
// MAGIC                           ("8", 3,  "IL",  6.5, False, True,   31.22), 
// MAGIC                           ("9", 5,  "MO",  6.5, False, True,  670.66)] )
// MAGIC                 .toDF("order_id", "cust_id", "supplier_state", "sales_tax_rate", "filled", "open", "order_total") 
// MAGIC                 .withColumn("order_total",col("order_total").cast("decimal(10,2)"))
// MAGIC )
// MAGIC #custsDF.printSchema()
// MAGIC #ordersDF.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Joining on 2 columns using `&&` or `&`
// MAGIC 
// MAGIC Let's join on the customer state and supplier state as well as the customer ID. 

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC val joinMultiCols = custsDF.join(ordersDF, ($"state"===$"supplier_state") && (custsDF("cust_id")===ordersDF("cust_id")), "inner")
// MAGIC                            .drop(ordersDF("cust_id"))
// MAGIC                            .drop("street","city","state","open")
// MAGIC joinMultiCols.show

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC joinMultiCols = (custsDF.join(ordersDF, (custsDF.state == ordersDF.supplier_state) & (custsDF.cust_id == ordersDF.cust_id), "inner") 
// MAGIC                         .drop(ordersDF.cust_id)
// MAGIC                         .drop("street","city","state","open"))
// MAGIC joinMultiCols.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Joining on 2 columns chaining `.where` calls.
// MAGIC 
// MAGIC Again, let's join on the customer state and supplier state as well as the customer ID. 

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC val joinMultiCols = custsDF.join(ordersDF).where($"state" === $"supplier_state")
// MAGIC                                           .where(custsDF("cust_id") === ordersDF("cust_id"))
// MAGIC                                           .drop(ordersDF("cust_id"))
// MAGIC                                           .drop("street","city","state","open")
// MAGIC joinMultiCols.show

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC joinMultiCols = (custsDF.join(ordersDF).where(custsDF.state == ordersDF.supplier_state)
// MAGIC                                        .where(custsDF.cust_id == ordersDF.cust_id)
// MAGIC                                        .drop(ordersDF.cust_id)
// MAGIC                                        .drop("street","city","state","open") )
// MAGIC joinMultiCols.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### How do I access the `SQLConf`  object?
// MAGIC 
// MAGIC The `SQLConf` object has a function named `getAllDefinedConfs`. It returns a `Seq[Tuple3]` object. Each tuple has a property name, property value, and a descripiton. You can access the object via the `spark.sessionState` object.
// MAGIC 
// MAGIC See: https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala

// COMMAND ----------

// MAGIC %scala
// MAGIC val sqlConf = spark.sessionState.conf
// MAGIC println("sqlConf: "+sqlConf)
// MAGIC println("class of sqlConf: "+sqlConf.getClass.getName)
// MAGIC val confs = sqlConf.getAllDefinedConfs
// MAGIC confs.foreach(tup  => println("prop:" +tup._1+"\n\tvalue: "+tup._2+"\n\tdescription: "+tup._3))
// MAGIC println("="*70)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### How do I enable/disable CBO (Cost Based Optimization) in Spark?
// MAGIC 
// MAGIC CBO is disabled by default in vanilla Spark but is enabled by default in the Databricks Runtime (DBR).  You can set it with a property `spark.sql.cbo.enabled`. The value of this property and other CBO-related properties can be found in the `SQLConf` object. 
// MAGIC 
// MAGIC <pre style="width:65%; color:black; background-color:#f2f2f2; 
// MAGIC            font-weight:normal; font-family:Courier New; font-size:16px;
// MAGIC             margin-left:15px; ">
// MAGIC             
// MAGIC val cboEnabledBoolean = spark.sessionState.conf.cboEnabled
// MAGIC val joineReorderEnabled = spark.sessionState.conf.joinReorderEnabled
// MAGIC </pre>
// MAGIC 
// MAGIC #### or
// MAGIC 
// MAGIC <pre style="width:65%; color:black; background-color:#f2f2f2; 
// MAGIC            font-weight:normal; font-family:Courier New; font-size:16px;
// MAGIC             margin-left:15px; ">
// MAGIC spark.conf.get("spark.sql.cbo.enabled")
// MAGIC spark.conf.get("spark.sql.cbo.joinReorder.enabled")
// MAGIC </pre>
// MAGIC 
// MAGIC #### To change a property value, use `spark.conf.set(...)`
// MAGIC 
// MAGIC <pre style="width:65%; color:black; background-color:#f2f2f2; 
// MAGIC            font-weight:normal; font-family:Courier New; font-size:16px;
// MAGIC             margin-left:15px; ">
// MAGIC 
// MAGIC spark.conf.set("spark.sql.cbo.enabled", false)
// MAGIC spark.conf.set("spark.sql.cbo.joinReorder.enabled", false)
// MAGIC </pre>
// MAGIC 
// MAGIC The `SparkSession` object (`spark`) in Python does not have a function nor a field named `sessionState`. But you can fall back on the Java `SparkSession` object accessible by `spark._jsparkSession`. 

// COMMAND ----------

// MAGIC %scala
// MAGIC // You can get the value from the SQLConf
// MAGIC val sqlConf = spark.sessionState.conf
// MAGIC var cboEnabled = sqlConf.cboEnabled
// MAGIC var joinReorderEnabled = sqlConf.joinReorderEnabled
// MAGIC println("CBO is %s enabled.".format( (if(cboEnabled) "indeed" else "not")))
// MAGIC println("Join reorder is %s enabled.".format( (if(joinReorderEnabled) "indeed" else "not")))
// MAGIC 
// MAGIC // You can also get the value from the SparkConf property spark.sql.cbo.enable
// MAGIC println("The property spark.sql.cbo.enabled is set to: "+spark.conf.get("spark.sql.cbo.enabled"))
// MAGIC 
// MAGIC // You can then set this property using the SparkConf
// MAGIC 
// MAGIC spark.conf.set("spark.sql.cbo.enabled", false)
// MAGIC spark.conf.set("spark.sql.cbo.joinReorder.enabled", false)
// MAGIC 
// MAGIC println("After setting props through spark.conf "+("*"*30))
// MAGIC cboEnabled = sqlConf.cboEnabled
// MAGIC joinReorderEnabled = sqlConf.joinReorderEnabled
// MAGIC println("CBO is %s enabled.".format( (if(cboEnabled) "indeed" else "not")))
// MAGIC println("Join reorder is %s enabled.".format( (if(joinReorderEnabled) "indeed" else "not")))
// MAGIC 
// MAGIC // You can also set the property thorugh the SQLConf
// MAGIC 
// MAGIC val cboConfEntry = org.apache.spark.sql.internal.SQLConf.CBO_ENABLED
// MAGIC val joinReorderConfEntry = org.apache.spark.sql.internal.SQLConf.JOIN_REORDER_ENABLED
// MAGIC spark.conf.set(cboConfEntry.key, true)
// MAGIC spark.conf.set(joinReorderConfEntry.key, true)
// MAGIC 
// MAGIC println("After setting props through SQLConf (spark.sessionState.conf) "+("*"*30))
// MAGIC cboEnabled = sqlConf.cboEnabled
// MAGIC joineReorderEnabled = sqlConf.joinReorderEnabled
// MAGIC println("CBO is %s enabled.".format( (if(cboEnabled) "indeed" else "not")))
// MAGIC println("Join reorder is %s enabled.".format( (if(joineReorderEnabled) "indeed" else "not")))
// MAGIC 
// MAGIC // You can also get the description of these config properties programmatically
// MAGIC 
// MAGIC println("Documentation for cboEnabled config entry is\n\t" + cboConfEntry.doc)
// MAGIC println("Documentation for joinReorder config entry is\n\t" + joinReorderConfEntry.doc)
// MAGIC 
// MAGIC println("="*70)

// COMMAND ----------

// MAGIC %python
// MAGIC sqlConf = spark._jsparkSession.sessionState().conf()
// MAGIC cboEnabled = sqlConf.cboEnabled()
// MAGIC joinReorderEnabled = sqlConf.joinReorderEnabled()
// MAGIC print("CBO is {} enabled.".format( "indeed" if cboEnabled else "not"))
// MAGIC print("Join reorder is {} enabled.".format( "indeed" if joinReorderEnabled else "not"))
// MAGIC 
// MAGIC # You can also get the value from the SparkConf property spark.sql.cbo.enable
// MAGIC print("The property spark.sql.cbo.enabled is set to: ",spark.conf.get("spark.sql.cbo.enabled"))
// MAGIC 
// MAGIC # You can then set this property using the SparkConf
// MAGIC 
// MAGIC spark.conf.set("spark.sql.cbo.enabled", True)
// MAGIC spark.conf.set("spark.sql.cbo.joinReorder.enabled", True)
// MAGIC 
// MAGIC print("After setting props through spark.conf.set(...) ",("*"*30))
// MAGIC cboEnabled = sqlConf.cboEnabled()
// MAGIC joinReorderEnabled = sqlConf.joinReorderEnabled()
// MAGIC print("CBO is {} enabled.".format( "indeed" if cboEnabled else "not"))
// MAGIC print("Join reorder is {} enabled.".format( "indeed" if joinReorderEnabled else "not"))

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC #### How do I prevent the catalyst optimizer from inserting an `isnotnull(xxx)` when I want rows with nulls to be included.
// MAGIC 
// MAGIC The below code will cause the catalyst to insert...
// MAGIC <pre style="width:85%; color:black; background-color:#f2f2f2; 
// MAGIC            font-weight:normal; font-family:Courier New; font-size:16px;
// MAGIC             margin-left:15px; ">
// MAGIC == Optimized Logical Plan ==
// MAGIC Filter ((((((((isnotnull(project#10) && NOT (project#10 = en.zero)) && yada yada yada
// MAGIC </pre>
// MAGIC 
// MAGIC ...even though we might want rows with null values for project. 

// COMMAND ----------

// MAGIC %scala 
// MAGIC import org.apache.spark.sql.functions.col
// MAGIC val no_null_projects_automaticallyDF = spark.read.parquet("/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/")
// MAGIC   .filter( col("project") =!= "en.zero")
// MAGIC   .filter( col("project") =!= "en.zero.n")
// MAGIC   .filter( col("project") =!= "en.zero.s")
// MAGIC   .filter( col("project") =!= "en.zero.d")
// MAGIC   .filter( col("project") =!= "en.zero.voy")
// MAGIC   .filter( col("project") =!= "en.zero.b")
// MAGIC   .filter( col("project") =!= "en.zero.v")
// MAGIC   .filter( col("project") =!= "en.zero.q")
// MAGIC 
// MAGIC no_null_projects_automaticallyDF.explain(true)

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import col
// MAGIC no_null_projects_automaticallyDF = (spark.read.parquet("/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/")
// MAGIC   .filter( col("project") != "en.zero")
// MAGIC   .filter( col("project") != "en.zero.n")
// MAGIC   .filter( col("project") != "en.zero.s")
// MAGIC   .filter( col("project") != "en.zero.d")
// MAGIC   .filter( col("project") != "en.zero.voy")
// MAGIC   .filter( col("project") != "en.zero.b")
// MAGIC   .filter( col("project") != "en.zero.v")
// MAGIC   .filter( col("project") != "en.zero.q")
// MAGIC )
// MAGIC no_null_projects_automaticallyDF.explain(True)

// COMMAND ----------

// MAGIC %md
// MAGIC To keep the nulls you could `union` the two DataFrames together.

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.functions.col
// MAGIC val origDF = spark.read.parquet("/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/")
// MAGIC val nullProjectsDF = origDF.filter(col("project").isNull)                            
// MAGIC                             
// MAGIC val no_null_projects_automaticallyDF = origDF
// MAGIC   .filter( col("project") =!= "en.zero")
// MAGIC   .filter( col("project") =!= "en.zero.n")
// MAGIC   .filter( col("project") =!= "en.zero.s")
// MAGIC   .filter( col("project") =!= "en.zero.d")
// MAGIC   .filter( col("project") =!= "en.zero.voy")
// MAGIC   .filter( col("project") =!= "en.zero.b")
// MAGIC   .filter( col("project") =!= "en.zero.v")
// MAGIC   .filter( col("project") =!= "en.zero.q")
// MAGIC 
// MAGIC val withNullsUnionedDF = nullProjectsDF.union(no_null_projects_automaticallyDF)

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import col
// MAGIC origDF = spark.read.parquet("/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/")
// MAGIC nullProjectsDF = origDF.filter(col("project").isNull())                            
// MAGIC                             
// MAGIC no_null_projects_automaticallyDF = (origDF
// MAGIC   .filter( col("project") != "en.zero")
// MAGIC   .filter( col("project") != "en.zero.n")
// MAGIC   .filter( col("project") != "en.zero.s")
// MAGIC   .filter( col("project") != "en.zero.d")
// MAGIC   .filter( col("project") != "en.zero.voy")
// MAGIC   .filter( col("project") != "en.zero.b")
// MAGIC   .filter( col("project") != "en.zero.v")
// MAGIC   .filter( col("project") != "en.zero.q")
// MAGIC )
// MAGIC withNullsUnionedDF = nullProjectsDF.union(no_null_projects_automaticallyDF)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC Or you could modify your filter to be one expression that includes keeping null projects using the Boolean OR operator `|` (single pipe in Python and a `||` double pipe in Scala). The below cell produces an optimized plan that does **not** filter out null projects. 
// MAGIC 
// MAGIC <pre style="width:90%; color:black; background-color:#f2f2f2; 
// MAGIC            font-weight:normal; font-family:Courier New; font-size:14px;
// MAGIC             margin-left:1px; ">
// MAGIC == Optimized Logical Plan ==
// MAGIC Filter (isnull(project#42) || (((((((NOT (project#42 = en.zero) && NOT (project#42 = en.zero.n)) && NOT yada yada yada
// MAGIC </pre>

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.functions.col
// MAGIC val origDF = spark.read.parquet("/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/")
// MAGIC                                     
// MAGIC val desiredProjectsAndNullProjectsDF = origDF.filter( (col("project").isNull ||                                                 
// MAGIC                                                        ((col("project") =!= "en.zero")   && 
// MAGIC                                                         (col("project") =!= "en.zero.n") && 
// MAGIC                                                         (col("project") =!= "en.zero.s") && 
// MAGIC                                                         (col("project") =!= "en.zero.d") && 
// MAGIC                                                         (col("project") =!= "en.zero.voy") && 
// MAGIC                                                         (col("project") =!= "en.zero.b") && 
// MAGIC                                                         (col("project") =!= "en.zero.v") && 
// MAGIC                                                         (col("project") =!= "en.zero.q"))
// MAGIC                                                       ))
// MAGIC                                                        
// MAGIC desiredProjectsAndNullProjectsDF.explain(true)

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import col
// MAGIC origDF = spark.read.parquet("/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/")
// MAGIC                                     
// MAGIC desiredProjectsAndNullProjectsDF = (origDF.filter( (col("project").isNull()) |                                                 
// MAGIC                   ((col("project") != "en.zero") & 
// MAGIC                    (col("project") != "en.zero.n") & 
// MAGIC                    (col("project") != "en.zero.s") & 
// MAGIC                    (col("project") != "en.zero.d") & 
// MAGIC                    (col("project") != "en.zero.voy") & 
// MAGIC                    (col("project") != "en.zero.b") & 
// MAGIC                    (col("project") != "en.zero.v") & 
// MAGIC                    (col("project") != "en.zero.q")) ) )
// MAGIC desiredProjectsAndNullProjectsDF.explain(True)

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Create a second ("child") `SparkSession` instance within the same application.

// COMMAND ----------

// MAGIC %scala
// MAGIC val sparkChild = spark.newSession
// MAGIC sparkChild.conf.set("spark.sql.sources.default","json")
// MAGIC 
// MAGIC println("Are the two SparkSession instance the same object? " + (spark==sparkChild))
// MAGIC println("The default file format for the original SparkSession instance is: " + spark.conf.get("spark.sql.sources.default"))
// MAGIC println("The default file format for the child    SparkSession instance is: " + sparkChild.conf.get("spark.sql.sources.default"))
// MAGIC println("="*70)

// COMMAND ----------

// MAGIC %python
// MAGIC sparkChild = spark.newSession()
// MAGIC sparkChild.conf.set("spark.sql.sources.default","json")
// MAGIC 
// MAGIC print("Are the two SparkSession instance the same object? ", (spark==sparkChild))
// MAGIC print("The default file format for the original SparkSession instance is: ", spark.conf.get("spark.sql.sources.default"))
// MAGIC print("The default file format for the child    SparkSession instance is: ", sparkChild.conf.get("spark.sql.sources.default"))

// COMMAND ----------

// MAGIC %md
// MAGIC **IMPORTANT** The above two `SparkSession` instances would not share temp views unless the views were **GLOBAL** temp views.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Global Temporary Views and their reserved namespace: `global_temp`
// MAGIC 
// MAGIC Global views are put into a special, reserved namespace named "global_temp". If someone creates a global temp view and then asks the 
// MAGIC catalog for a list of tables, they **will not** see the view unless they were to specify the namespace as "global_temp". 
// MAGIC 
// MAGIC The term "global" can be misleading. A global temporary view in one application can **not** be seen in another application.  The scope of a global temp view is within one application across multiple `SparkSession` instances. Each instance of `SparkSession` within a single application (think "driver main method") can only see the temporary views that were created in association with that instance using `df.createOrReplaceTempView("abc")` or `df.createTempView("abc")`.  However, if a view is created with `df.createGlobalTempView("abc")` then a separate `SparkSession` instance would have access to that view. (FTR, it is rare that you will see mulitiple `SparkSession` objects within the same driver main.) 

// COMMAND ----------

// MAGIC %md
// MAGIC Run the following cell only once. Running it more than once will cause exceptions because the views already exist. 

// COMMAND ----------

custsDF.createTempView("evan_temp_view") 
custsDF.createGlobalTempView("evan_global_temp_view")

// COMMAND ----------

// MAGIC %md
// MAGIC Now let's list the tables (which includes views) that include the string `evan`. Note that we are not specifying on which database or namespace we are listing tables & views.

// COMMAND ----------

// MAGIC %scala 
// MAGIC spark.catalog.listTables.where("name like '%evan%'").show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC Notice above that the view `evan_global_temp_view` **did not appear** in the listing. Again,this is because all global temporary views are put into a special, 
// MAGIC reserve namespace called "global_temp". However, if they specify the reserve database when listing tables we **will** see the view. In fact, we see views that are **not** global.

// COMMAND ----------

spark.catalog.listTables("global_temp").where("name like '%evan%'").show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC Notice above that we see two views. One view is in the database (namespace) "global_temp" and the other is in the "null" database. Note that "null" as a database does not mean it is in the default database. When creating a non-global temp view, one can not specify the database. Run the next cell and note the error message.

// COMMAND ----------

// custsDF.createTempView("default.some_view_name") // <<< produces error

// COMMAND ----------

// MAGIC %md
// MAGIC When attempting to perform an SQL query over a global view, one **must** use the fully qualified name of the view even if the current database is set to "global_temp". 
// MAGIC 
// MAGIC In fact, setting the current database to "global_temp" causes an exception. Remember, "global_temp" is actually a namespace and not a database. Run both cells below and see that both cause an exception.

// COMMAND ----------

// spark.sql("select * from evan_global_temp_view limit 10").show(false) // <<< produces error

// COMMAND ----------

// spark.catalog.setCurrentDatabase("global_temp") // <<< produces error

// COMMAND ----------

// MAGIC %md
// MAGIC Using the fully-qualified name of the view will get rid of the exception when performing SQL queries over a global temporary view.

// COMMAND ----------

spark.sql("select * from global_temp.evan_global_temp_view limit 10").show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC Global temporary views do not exist outside your application. The are "global" only to the `SparkSession` objects you have in your application if you have more than one.  Run the cell that appears earlier in this notebook that demostrates how to create a second `SparkSession` instance (if you have not run it already). Then run the cells below.
// MAGIC 
// MAGIC In the scope of this notebook, we have 2 spark session objects.

// COMMAND ----------

println("Our first SparkSession object is referenced with the varable name 'spark'. \n\tIt has a default sources format of " 
        + spark.conf.get("spark.sql.sources.default"))
println("We created a child SparkSession object and named the variable 'sparkChild'. \n\tIt has a default sources format of " 
        + sparkChild.conf.get("spark.sql.sources.default"))
println("="*70)

// COMMAND ----------

// MAGIC %md
// MAGIC These two `SparkSession` instances share global temporary views but not non-global temp views. Notice the difference when we list the tables using the catalogs from the two different session objects.

// COMMAND ----------

// MAGIC %scala
// MAGIC spark.catalog.listTables.where("name like '%evan%'").show(false)
// MAGIC sparkChild.catalog.listTables.where("name like '%evan%'").show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC Above, note that listing tables & views for `sparkChild` produced no tables or views containing the string 'evan' in the name. A session can not see the non-global temp views of another session.
// MAGIC 
// MAGIC However, if we list the a tables in the "global_temp" namespace using both catalogs, we see there is common view between the two sessions.

// COMMAND ----------

     spark.catalog.listTables("global_temp").where("name like '%evan%'").show(false)
sparkChild.catalog.listTables("global_temp").where("name like '%evan%'").show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's drop the global temp view using SQL using our original `SparkSession` object. We could also use the API call `spark.catalog.dropGlobalTempView("evan_global_temp_view")`. We will list the global views again using the other `SparkSession` instance to verify the view's deletion.

// COMMAND ----------

spark.sql("drop view if exists global_temp.evan_global_temp_view")

sparkChild.catalog.listTables("global_temp").where("name like '%evan%'").show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC We still, however, have the non-global view in the `spark` instance.

// COMMAND ----------

spark.sql("show tables").where("tableName like '%evan%'").show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC In a Databricks notebook, we could use the magic command `%sql` if we prefer SQL syntax. However, notice that `\*` (star) is used for a wildcard and not `%`

// COMMAND ----------

// MAGIC %sql
// MAGIC SHOW TABLES IN default LIKE '%evan%'

// COMMAND ----------

spark.catalog.listTables("default").where("name like '%evan%'").show(false)

// COMMAND ----------

// MAGIC %sql
// MAGIC SHOW TABLES IN default LIKE '*evan*'

// COMMAND ----------

spark.catalog.listTables("default").where("name like '*evan*'").show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Using `spark.catalog.listTables.show` is slow. Try `%sql show tables` instead.
// MAGIC 
// MAGIC When retrieving the list of tables from the catalog, a lot of metadata is collected that you might not need and is not actually shown. This can make the operation slow even if there aren't a sizable number of tables. Try using `%sql show tables` or `spark.sql("show tables")` instead.  It will only retrieve the table names and whether it's temporary and thus be faster. 

// COMMAND ----------

// slow
spark.catalog.listTables("default").show(false)

// COMMAND ----------

// MAGIC %sql /* much faster than using spark.catalog.listTables("default")  */
// MAGIC show tables 

// COMMAND ----------

// MAGIC %md
// MAGIC Even programatically, using `show tables` is faster.

// COMMAND ----------

// also much faster than using spark.catalog.listTables("default")
spark.sql("show tables in default").show(1000,false)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Limitations of `Seq.toDF(..)`
// MAGIC 
// MAGIC When using `Seq.toDF`, the elements must be related.  Example: all numeric or all text, all Maps, all array of ints, all arrays of strings, all booleans, all structs objectxs of same type. 

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC val df1 = Seq("A","B","C","D").toDF("stringCol")
// MAGIC df1.printSchema
// MAGIC val df2 = Seq(1,2,3,4,5).toDF("integerCol")
// MAGIC df2.printSchema
// MAGIC val df3 = Seq(1,2,3.14,4,5.0F).toDF("doubleCol")
// MAGIC df3.printSchema
// MAGIC val df4 = Seq(1,2.0F,3,4,5).toDF("floatCol") // mixing that produces a float column
// MAGIC df4.printSchema
// MAGIC val df5 = Seq(1,2L,3.0F,4.0D).toDF("doubleCol") // mixing produces a double column
// MAGIC df5.printSchema
// MAGIC val df6 = Seq(1L,2,4,5).toDF("bigintCol")
// MAGIC df6.printSchema
// MAGIC val df7 = Seq(true,false,false,true,true,true,false).toDF("boleanCol")
// MAGIC df7.printSchema
// MAGIC val df8 = Seq( Map("AL"->"Alabama","AR"->"Arkansas"), Map("uk"->"United Kingdom","de"->"Germany") ).toDF("mapCol")
// MAGIC df8.printSchema
// MAGIC val df9 = Seq( Array(1,2,3,4,5), Array(6,7,8,9) ).toDF("arrayOfIntegersCol")
// MAGIC df9.printSchema
// MAGIC val df10 = Seq( Array("1","2","3","4","5"), Array("6","7","8","9") ).toDF("arrayOfStringsCol")
// MAGIC df10.printSchema
// MAGIC 
// MAGIC case class Widget(widgetName:String, widgetValue:Int)
// MAGIC case class Foo(wgt:Widget, str:String)
// MAGIC 
// MAGIC val f1 = Foo(Widget("Widget 1",1),"Foo f1")
// MAGIC val f2 = Foo(Widget("Widget 2",2),"Foo f2")
// MAGIC val f3 = Foo(Widget("Widget 3",3),"Foo f3")
// MAGIC 
// MAGIC val df11 = Seq( f1, f2, f3 ).toDF("Foo_wgt_col","Foo_str_col")
// MAGIC df11.printSchema
// MAGIC df11.show
// MAGIC // we can flatten the structure using dot notation
// MAGIC val df11_B = df11.select("Foo_wgt_col.widgetName","Foo_wgt_col.widgetValue","Foo_str_col")
// MAGIC df11_B.printSchema
// MAGIC df11_B.show(false)
// MAGIC 
// MAGIC val df12 = Seq( Array(Widget("Row 1 col 0",1),Widget("Row 1 col 0",2)), 
// MAGIC                 Array(Widget("Row 2 col 0",3),Widget("Row 2 col 0",4))).toDF("arrayOfStructsCol")
// MAGIC df12.printSchema
// MAGIC df12.show(false)
// MAGIC 
// MAGIC println("="*70)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC The elements of `Seq(...).toDF` must be resolvable to a known `StructField` type such as `integer`, `boolean`, `double`, etc. Mixing of types that don't resolve to a common `StructField` type will produce a common `Any` type and cause an error. 

// COMMAND ----------

// MAGIC %scala
// MAGIC // Uncomment examples below to see the compiler errors
// MAGIC 
// MAGIC //val df = Seq("A","B","C",1).toDF("stringAndIntegerColumn") // mixing strings and integers
// MAGIC //df.printSchema
// MAGIC 
// MAGIC 
// MAGIC //val df = Seq("A","B","C",1.0).toDF("stringAndDoubleColumn") // mixing strings and doubles
// MAGIC //df.printSchema
// MAGIC 
// MAGIC //val df = Seq("A","B","C",true).toDF("stringAndBooleanColumn") // mixing strings and booleans
// MAGIC //df.printSchema
// MAGIC println("="*70)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### `Seq.toDF` and `nullable`
// MAGIC The `nullable` value of StructFields in Schemas created using `toDF` vary depending on the resolved type. Strings, maps, arrays, and structs have a `nullable` value of true while integer, boolean, and double do not. **Takeaway: **Use `Seq.toDF` for testing and learning. Avoid it in production.

// COMMAND ----------

// MAGIC %scala
// MAGIC val df1 = Seq("A").toDF("stringColumn")
// MAGIC //df1.printSchema
// MAGIC println("When the column type resolves to string, nullable is: " + df1.schema.fields(0).nullable)
// MAGIC 
// MAGIC val df2 = Seq(1).toDF("integerColumn")
// MAGIC //df2.printSchema
// MAGIC println("When the column type resolves to integer, nullable is: " + df2.schema.fields(0).nullable)
// MAGIC 
// MAGIC val df3 = Seq(2.0).toDF("doubleColumn")
// MAGIC //df3.printSchema
// MAGIC println("When the column type resolves to double, nullable is: " + df3.schema.fields(0).nullable)
// MAGIC 
// MAGIC val df4 = Seq(2.0F).toDF("floatColumn")
// MAGIC //df4.printSchema
// MAGIC println("When the column type resolves to double, nullable is: " + df4.schema.fields(0).nullable)
// MAGIC 
// MAGIC val df5 = Seq(true).toDF("boleanColumn")
// MAGIC //df5.printSchema
// MAGIC println("When the column type resolves to boolean, nullable is: " + df5.schema.fields(0).nullable)
// MAGIC 
// MAGIC val df6 = Seq( Map("AL"->"Alabama","AR"->"Arkansas"), Map("uk"->"United Kingdom","de"->"Germany") ).toDF("mapColumn")
// MAGIC //df6.printSchema
// MAGIC println("When the column type resolves to Map, nullable is: " + df6.schema.fields(0).nullable)
// MAGIC 
// MAGIC val df7 = Seq( Array(1,2,3,4,5), Array(6,7,8,9) ).toDF("arrayOfIntegersColumn")
// MAGIC //df7.printSchema
// MAGIC println("When the column type resolves to Array of ints, nullable is: " + df7.schema.fields(0).nullable)
// MAGIC 
// MAGIC val df8 = Seq( Array("1","2","3","4","5"), Array("6","7","8","9") ).toDF("arrayOfStringsColumn")
// MAGIC //df8.printSchema
// MAGIC println("When the column type resolves to Array of strings, nullable is: " + df8.schema.fields(0).nullable)
// MAGIC 
// MAGIC case class Widget(widgetName:String, widgetValue:Int)
// MAGIC case class Foo(wgt:Widget)
// MAGIC val df9 = Seq( Foo(Widget("WA",1)),Foo(Widget("WB",2)) ).toDF("structColumn")
// MAGIC //df9.printSchema
// MAGIC println("When the column type resolves to struct, nullable is: " + df9.schema.fields(0).nullable)
// MAGIC println("="*30)
// MAGIC 
// MAGIC var html = "<style> table  { border:0px solid black; border-collapse:collapse }\n\t"
// MAGIC html = html + "th,td  { border:1px solid black ; }\n\t"
// MAGIC html = html + ".col1 {text-align: left}\n\t"
// MAGIC html = html + ".col2 {text-align: center}\n"
// MAGIC html = html + "</style>\n\n"
// MAGIC html = html + "<table>\n\t<tr><th >Type</th><th>nullable</th></tr>\n"
// MAGIC 
// MAGIC val dfs = List(df1,df2,df3,df4,df5,df6,df7,df8,df9)
// MAGIC 
// MAGIC for( df <- dfs) {
// MAGIC   html = html + "\t<tr><td class='col1'>"+df.schema.fields(0).dataType+"</td><td class='col2'>"+df.schema.fields(0).nullable+"</td></tr>\n"
// MAGIC }
// MAGIC 
// MAGIC html = html + "</table>\n"
// MAGIC println(html)
// MAGIC //displayHTML(html)
// MAGIC println("="*70)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Row objects and `==`
// MAGIC 
// MAGIC For `Row` objects, `equals` calls ``==``
// MAGIC 
// MAGIC Two `Row` objects are not considered equal if the order of the column names are different. The names and orders must match to be equal. 

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.Row
// MAGIC 
// MAGIC val row1 = Row.fromSeq(Seq("A","B",1,false,null,9.0))
// MAGIC println("row1: "+ row1)
// MAGIC 
// MAGIC val row2 = Row.fromSeq(Seq("B","A",9.0,false,null,1))
// MAGIC println("row2: "+ row2)
// MAGIC 
// MAGIC println("row1.equals(row2): "+(row1.equals(row2)))
// MAGIC println("row1 == row2: "+(row1 == row2))
// MAGIC println("row1 eq row2: "+(row1 eq row2))
// MAGIC println("="*70)

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql import Row
// MAGIC 
// MAGIC row1 = Row(C0="A",C1="B",C2=1,C3=False,C4=None,C5=9.0)
// MAGIC print("row1: ", row1)
// MAGIC 
// MAGIC row2 = Row(C1="B",C0="A",C2=1,C3=False,C4=None,C5=9.0)
// MAGIC print("row2: ", row2)
// MAGIC 
// MAGIC # Be wary with Pyspark. The columns are automatically sorted alphabetically!
// MAGIC 
// MAGIC print("row1 == row2: ",(row1 == row2))

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC The schema for a `Row` object is null if the `Row` object is created outside that of a DataFrame. Schemas are optional for Rows but not for DataFrames.  All DataFrames must have a schema. 

// COMMAND ----------

// MAGIC %scala
// MAGIC // evan - this needs work
// MAGIC import org.apache.spark.sql.Row
// MAGIC 
// MAGIC val rowFromSeq = Row.fromSeq(Seq("A","B","C","D"))
// MAGIC println("rowFromSeq.schema:\t"+ rowFromSeq.schema)
// MAGIC 
// MAGIC val df = Seq("A","B","C","D").toDF
// MAGIC val rowFromDF = df.first
// MAGIC 
// MAGIC println("rowFromDF.schema:\t"+ rowFromDF.schema)
// MAGIC println("df.schema:\t\t" +df.schema)
// MAGIC println("-"*10)
// MAGIC 
// MAGIC println("rowFromDF.schema.equals(df.schema):\t"+(rowFromDF.schema.equals(df.schema)))
// MAGIC println("rowFromDF.schema == df.schema:\t"+(rowFromDF.schema == df.schema))
// MAGIC println("-"*10)
// MAGIC 
// MAGIC println("rowFromSeq.equals(rowFromDF):\t"+(rowFromSeq.equals(rowFromDF)))
// MAGIC println("rowFromSeq == rowFromDF:\t"+(rowFromSeq == rowFromDF))
// MAGIC 
// MAGIC println("="*70)

// COMMAND ----------

// MAGIC %scala
// MAGIC // evan - this needs work
// MAGIC import org.apache.spark.sql.types.{StructType,StructField,StringType}
// MAGIC import org.apache.spark.sql.Row
// MAGIC 
// MAGIC val row1 = Row.fromSeq(Seq("A","B",1,false,null,9.0))
// MAGIC println("row1: "+ row1)
// MAGIC println("row1.schema: "+ row1.schema)
// MAGIC 
// MAGIC val row2 = Row.fromSeq(Seq("A","B",1,false,null,9.0))
// MAGIC println("row2: "+ row2)
// MAGIC println("row2.schema: "+ row1.schema)
// MAGIC 
// MAGIC println("row1.equals(row2): "+(row1.equals(row2)))
// MAGIC println("row1 == row2: "+(row1 == row2))
// MAGIC 
// MAGIC println("row1.isNullAt(4) "+ row1.isNullAt(4))
// MAGIC 
// MAGIC val sampleSchema1 = StructType(
// MAGIC   List(StructField("EmployeeName",StringType,false))
// MAGIC )
// MAGIC 
// MAGIC val sampleDF1 = Seq("A B C", " XYZ ", "aaaa"," 1 2 3 x","A\t\r\f\nZ","    ").toDF("EmployeeName")
// MAGIC 
// MAGIC 
// MAGIC val schema1 = sampleDF1.schema
// MAGIC println("sampleDF1.schema: "+schema1)
// MAGIC 
// MAGIC val sampleDF3 = spark.createDataFrame(spark.sparkContext.parallelize(List(Row("A B C"),  // create an RDD of Row objects
// MAGIC                                                                           Row(" XYZ "), 
// MAGIC                                                                           Row("aaaa"),
// MAGIC                                                                           Row(" 1 2 3 x"),
// MAGIC                                                                           Row("A\t\r\f\nZ"),
// MAGIC                                                                           Row("    "))),
// MAGIC                                      sampleSchema1) // provide a schema to go along with the Row objects
// MAGIC 
// MAGIC println("="*70)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##Schema objects and `==`

// COMMAND ----------

// MAGIC %scala
// MAGIC // evan - this needs work
// MAGIC import org.apache.spark.sql.types.{StructType,StructField,StringType,BooleanType,IntegerType}
// MAGIC val schema1 = StructType(
// MAGIC   List(StructField("A",StringType),
// MAGIC        StructField("B",BooleanType),
// MAGIC        StructField("C",IntegerType)
// MAGIC       )
// MAGIC )
// MAGIC 
// MAGIC val schema2 = StructType(
// MAGIC   List(StructField("B",BooleanType),
// MAGIC        StructField("A",StringType),
// MAGIC        StructField("C",IntegerType)
// MAGIC       )
// MAGIC )
// MAGIC println("The order of the fields are different in the two StructType objects.")
// MAGIC println("\tschema1: "+schema1)
// MAGIC println("\tschema2: "+schema2)
// MAGIC println("\tschema1==schema2 produces "+(schema1==schema2))
// MAGIC println("\tschema1==schema1 produces "+(schema1==schema1))
// MAGIC println("\tschema1.equals(schema2) produces "+(schema1.equals(schema2)))
// MAGIC println("\tschema1.equals(schema1) produces "+(schema1.equals(schema1)))
// MAGIC println("="*70)

// COMMAND ----------

// MAGIC %python
// MAGIC # evan - this needs work
// MAGIC from pyspark.sql.types import StructType,StructField,StringType,BooleanType,IntegerType
// MAGIC schema1 = (StructType(
// MAGIC              [StructField("A",StringType()),
// MAGIC               StructField("B",BooleanType()),
// MAGIC               StructField("C",IntegerType())  
// MAGIC              ])
// MAGIC           )
// MAGIC 
// MAGIC schema2 = (StructType(
// MAGIC              [StructField("B",BooleanType()),
// MAGIC               StructField("A",StringType()),
// MAGIC               StructField("C",IntegerType())
// MAGIC              ])
// MAGIC           )
// MAGIC 
// MAGIC print("The order of the fields are different in the two StructType objects.")
// MAGIC print("\tschema1: ",schema1)
// MAGIC print("\tschema2: ",schema2)
// MAGIC print("\tschema1==schema2 produces ",(schema1==schema2))
// MAGIC print("\tschema1==schema1 produces ",(schema1==schema1))
// MAGIC print("="*70)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC #### Setting the schema of a DataFrame with a simple String
// MAGIC 
// MAGIC From the docs:
// MAGIC 
// MAGIC `def schema(schemaString: String): DataFrameReader`
// MAGIC 
// MAGIC Specifies the schema by using the input DDL-formatted string. Some data sources (e.g. JSON) can infer the input schema automatically from data. By specifying the schema here, the underlying data source can skip the schema inference step, and thus speed up data loading.
// MAGIC 
// MAGIC `spark.read.schema("a INT, b STRING, c DOUBLE").csv("test.csv")`
// MAGIC 
// MAGIC **Since 2.3.0**

// COMMAND ----------

// MAGIC %scala
// MAGIC val fileName = "dbfs:/mnt/training/wikipedia/clickstream/2015_02_clickstream.tsv"
// MAGIC val mySchemaString = 
// MAGIC     "prev_id integer not null comment 'The old id', curr_id integer comment 'The new id', n integer not null comment 'some n value', prev_title string, curr_title string, type string"
// MAGIC 
// MAGIC val testDF = spark.read
// MAGIC         .option("header",true)
// MAGIC         .option("sep","\t")
// MAGIC         .schema(mySchemaString)
// MAGIC         .format("csv")
// MAGIC         .load(fileName)
// MAGIC 
// MAGIC testDF.printSchema()
// MAGIC 
// MAGIC val myStructTypeSchema = spark.sessionState.sqlParser.parseTableSchema(mySchemaString)
// MAGIC println(myStructTypeSchema.getClass.getName)
// MAGIC myStructTypeSchema.foreach(field => println(field + " --- COMMENT: "+field.getComment.getOrElse("no comment")))
// MAGIC println("="*80)

// COMMAND ----------

// MAGIC %python
// MAGIC fileName = "dbfs:/mnt/training/wikipedia/clickstream/2015_02_clickstream.tsv"
// MAGIC mySchemaString = "prev_id integer not null comment 'The old id', curr_id integer comment 'The new id', n integer not null comment 'some n value', prev_title string, curr_title string, type string"
// MAGIC 
// MAGIC testDF = (spark.read
// MAGIC         .option("header",True)
// MAGIC         .option("sep","\t")
// MAGIC         .schema(mySchemaString)
// MAGIC         .format("csv")
// MAGIC         .load(fileName))
// MAGIC         
// MAGIC testDF.printSchema()

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### How do I remove all spaces and special characters from column names before writing a DataFrame?
// MAGIC 
// MAGIC When reading source data, the names of columns derived from inferring the schema could contain non-standard charaters such as spaces. When attempting to write a DataFrame in Parquet format, if any column names contain invalid characters, an exception will occur.
// MAGIC <pre style="width:80%; color:red; background-color:#f2f2f2; 
// MAGIC            font-weight:bold; font-family:Courier New; font-size:16px; 
// MAGIC             margin-left:15px; ">
// MAGIC      org.apache.spark.sql.AnalysisException: Attribute name "Case Number" contains invalid 
// MAGIC      character(s) among " ,;{}()\n\t=". Please use alias to rename it.;
// MAGIC </pre>
// MAGIC 
// MAGIC The 'Attribute name "Case Number"'  above refers to the column named "Case Number". To correct this, one could explicitly rename each column using `alias`, `name`, or `as` (Scala only). However, it is possible to rename each column dynamically rather than explicitly. To demonstrate this, we will first read CSV data containing headers that don't adhere to column name restrictions. We will print the schema, rename the columns dynamically, and the print the schema for the new DataFrame.
// MAGIC 
// MAGIC **Notes about valid and invalid characters for columns in Spark vs. a relational database**
// MAGIC + In Spark SQL `,;{}()\n\t=` are the **only invalid characters** for a column name. 
// MAGIC   - space, comma, left brace, right brace, left parenthesis, right parenthesis, new line, and tab
// MAGIC + In a relational database, typically the only valid characters for a column name are alpha (`a` through `z` upper and lower), numerals (`0` to `9`), and underscores (`_`). 
// MAGIC + In Spark SQL a column name **can start with a number**. Typically, in a relational database, this is not allowed.
// MAGIC + In Spark SQL a column name **can contain** tildes (`~`), backticks (``` ``), and even escape characters such as carriage return (`\r`), backspace (`\b`), or form feed (`\f`). Typically, in a relational database, this is not allowed.
// MAGIC + Also allowed for column names in Spark SQL but typically not allowed in a relational database are special characters such as dashes (`-`), slash (`/`), backslash (`\`), double quote marks (`"`), single quote marks (`'`) and even arithmetic & bitwise operators (`+`,`*`,`^`,`%`).
// MAGIC + Spark SQL has no restriction on using standard SQL keywords/reserve words such as `SELECT`,`FROM`,`IN`, `TABLE`,`ORDER`, etc as column nmaes. A relational database would typically not allow these as column names. 
// MAGIC 
// MAGIC <div style="color:blue; font-weight:bold; font-family:Tahoma; font-size:16px; margin-left:30px">
// MAGIC NOTE: When writing a DataFrame as CSV files or as JSON files, no error will occur if column names break any of the rules above.   
// MAGIC </div>
// MAGIC 
// MAGIC Just because many special characters are allowed in column names in Spark SQL, that doesn't mean it's a good idea to use them. In the below example we are allowing only word-building characters (`a-zA-Z_`) in column names. Also, just because you are allowed to name a column "TABLE", please avoid that unless the dataset has a restaurant seating context. 
// MAGIC 
// MAGIC See Scala [Column](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.catalog.Column) documentation for `as`, `alias` and `name`.
// MAGIC 
// MAGIC See Python [Column](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.Column) documentation for `alias` and `name`.

// COMMAND ----------

// MAGIC %scala
// MAGIC val csvInputPath = "/user/evan.troyka@databricks.com/crime/crimeCSV/"
// MAGIC val poorColumnNamesDF = spark.read.option("sep",",")
// MAGIC                             .option("header","true")
// MAGIC                             .option("inferSchema","true")
// MAGIC                             .csv(csvInputPath)
// MAGIC                             .withColumnRenamed("Case Number","Case;@#$%^& Number")
// MAGIC                                                // let's rename a column to an absurd name
// MAGIC println("BEFORE renaming columns" + "="*30)
// MAGIC poorColumnNamesDF.printSchema
// MAGIC var renamedColumnsDF = poorColumnNamesDF
// MAGIC for(columnName <- renamedColumnsDF.columns){
// MAGIC   renamedColumnsDF = renamedColumnsDF.withColumnRenamed(columnName,
// MAGIC                                           columnName.toLowerCase // Lowercase each column name because we like that!
// MAGIC                                           .replaceAll("\\W","_") // Change all non-word building characters to an underscore
// MAGIC                                           .replaceAll("_+","_")) // Change all repeating underscores to a single underscore
// MAGIC                                                                  // WARNING: Two columns could end up with the same name 
// MAGIC                                                                  //          "A@#$B" and "A@#$%^&B" would both become "A_B"
// MAGIC                                                         
// MAGIC }
// MAGIC println("AFTER renaming columns" + "="*30)
// MAGIC renamedColumnsDF.printSchema
// MAGIC // We can now write renamedColumnsDF as Parquet without getting the error 
// MAGIC // as before as long as we don't have multiple columns with same name.
// MAGIC val parquetOutputPath = "/user/evan.troyka@databricks.com/crime/temp/demoparquet"
// MAGIC renamedColumnsDF.write.mode("overwrite").parquet(parquetOutputPath)
// MAGIC spark.read.parquet(parquetOutputPath).printSchema
// MAGIC 
// MAGIC println("="*70)
// MAGIC println("Let's attempt to write the DataFrame with poor column names in CSV format. Spark SQL allows it.")
// MAGIC val csvOutputPath = "/user/evan.troyka@databricks.com/crime/temp/democsv"
// MAGIC poorColumnNamesDF.write
// MAGIC                  .mode("overwrite")
// MAGIC                  .option("sep",",")
// MAGIC                  .option("header","true") // column names with poorly chosen characters will indeed be honored
// MAGIC                  .csv(csvOutputPath)
// MAGIC 
// MAGIC // Read it back in for confirmation!
// MAGIC spark.read.option("sep",",")
// MAGIC           .option("header","true")
// MAGIC           .csv(csvOutputPath).printSchema()
// MAGIC 
// MAGIC println("="*70)
// MAGIC println("Let's attempt to write the DataFrame with poor column names in JSON format. Spark SQL allows it just as with CSV.")
// MAGIC val jsonOutputPath = "/user/evan.troyka@databricks.com/crime/temp/demojson"
// MAGIC poorColumnNamesDF.write
// MAGIC                  .mode("overwrite")
// MAGIC                  .json(jsonOutputPath) // No error occurs
// MAGIC // Read it back in for confirmation!
// MAGIC spark.read.json(jsonOutputPath).printSchema()
// MAGIC 
// MAGIC println("="*70)
// MAGIC println("However, if we attempt to write the DataFrame with illegal column names as Parquet files, Spark SQL will NOT allow it.")
// MAGIC try {
// MAGIC     poorColumnNamesDF.write.mode("overwrite").parquet(parquetOutputPath)
// MAGIC } catch {
// MAGIC     case e: org.apache.spark.sql.AnalysisException  => println("\n\t"+e.message)
// MAGIC }
// MAGIC 
// MAGIC println("="*70)
// MAGIC println("Also, if we attempt to write the DataFrame with illegal column names as a table, Spark SQL will NOT allow it.")
// MAGIC try {
// MAGIC     poorColumnNamesDF.write.mode("overwrite").saveAsTable("evan_troyka_databricks_com_db.table_with_poor_column_names")
// MAGIC } catch {
// MAGIC     case e: org.apache.spark.sql.AnalysisException  => println("\n\t"+e.message)
// MAGIC }
// MAGIC 
// MAGIC // Now we will clean up the output we wrote for this demonstration
// MAGIC //dbutils.fs.rm("/user/evan.troyka@databricks.com/crime/temp",true)
// MAGIC println("="*70)

// COMMAND ----------

// MAGIC %python
// MAGIC import re
// MAGIC csvInputPath = "/user/evan.troyka@databricks.com/crime/crimeCSV/"
// MAGIC poorColumnNamesDF = (spark.read.option("sep",",")
// MAGIC                             .option("header","true")
// MAGIC                             .option("inferSchema","true")
// MAGIC                             .csv(csvInputPath)
// MAGIC                             .withColumnRenamed("Case Number","Case;@#$%^& Number") )
// MAGIC                                                # let's rename a column to an absurd name
// MAGIC print("BEFORE renaming columns", "="*30)
// MAGIC poorColumnNamesDF.printSchema()
// MAGIC renamedColumnsDF = poorColumnNamesDF
// MAGIC for columnName in renamedColumnsDF.columns:
// MAGIC   renamedColumnsDF = (renamedColumnsDF.withColumnRenamed(
// MAGIC                                           columnName,
// MAGIC                                           re.sub("_+","_",re.sub("\W","_",columnName.lower()))))
// MAGIC #                                             Lowercase each column name because we like that!
// MAGIC #                                             Change all non-word-building characters to an underscore
// MAGIC #                                          .  Change all repeating underscores to a single underscore
// MAGIC #                                             WARNING: Two columns could end up with the same name 
// MAGIC #                                                      "A@#$B" and "A@#$%^&B" would both become "A_B" 
// MAGIC print("AFTER renaming columns", "="*30)
// MAGIC renamedColumnsDF.printSchema()
// MAGIC # We can now write renamedColumnsDF as Parquet without getting the error 
// MAGIC # as before as long as we don't have multiple columns with same name.
// MAGIC parquetOutputPath = "/user/evan.troyka@databricks.com/crime/temp/demoparquet"
// MAGIC (renamedColumnsDF.write
// MAGIC                 .mode("overwrite")
// MAGIC                 .parquet(parquetOutputPath))
// MAGIC spark.read.parquet(parquetOutputPath).printSchema()
// MAGIC 
// MAGIC print("="*70)
// MAGIC print("Let's attempt to write the DataFrame with poor column names in CSV format. Spark SQL allows it.")
// MAGIC csvOutputPath = "/user/evan.troyka@databricks.com/crime/temp/democsv/"
// MAGIC (poorColumnNamesDF.write
// MAGIC                       .mode("overwrite")
// MAGIC                       .option("sep",",")
// MAGIC                       .option("header","true") # column names with poorly chosen characters will indeed be honored
// MAGIC                       .csv(csvOutputPath))
// MAGIC # Read it back in for confirmation!
// MAGIC (spark.read.option("sep",",")
// MAGIC            .option("header","true")
// MAGIC            .csv(csvOutputPath).printSchema()) # No error occurs
// MAGIC 
// MAGIC print("="*70)
// MAGIC print("Let's attempt to write the DataFrame with poor column names in JSON format. Spark SQL allows it just as with CSV.")
// MAGIC jsonOutputPath = "/user/evan.troyka@databricks.com/crime/temp/demojson/"
// MAGIC (poorColumnNamesDF.write
// MAGIC                  .mode("overwrite")
// MAGIC                  .json(jsonOutputPath)) # No error occurs
// MAGIC # Read it back in for confirmation!
// MAGIC spark.read.json(jsonOutputPath).printSchema()
// MAGIC 
// MAGIC print("="*70)
// MAGIC print("However, if we attempt to write the DataFrame with illegal column names as Parquet files, Spark SQL will NOT allow it.")
// MAGIC try:
// MAGIC   poorColumnNamesDF.write.mode("overwrite").parquet(parquetOutputPath)
// MAGIC except Exception as e:
// MAGIC   print("\n\t",e)
// MAGIC   
// MAGIC print("="*70)
// MAGIC print("Also, if we attempt to write the DataFrame with illegal column names as a table, Spark SQL will NOT allow it.")
// MAGIC try:
// MAGIC   poorColumnNamesDF.write.mode("overwrite").saveAsTable("evan_troyka_databricks_com_db.table_with_poor_column_names")
// MAGIC except Exception as e:
// MAGIC     print("\n\t",e)
// MAGIC   
// MAGIC # Now we will clean up the output we wrote for this demonstration
// MAGIC # dbutils.fs.rm("/user/evan.troyka@databricks.com/crime/temp/",True)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### How do I turn a column of type array of strings into a column of single strings made up of the array elements?

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.functions._
// MAGIC val tomDF = spark.read.text("/mnt/training/tom-sawyer/tom.txt")
// MAGIC // our original DF has simple lines of text, 1 column named "value"
// MAGIC tomDF.printSchema
// MAGIC tomDF.show(5,false)
// MAGIC 
// MAGIC // let's create a dataframe with a single column of type array of strings
// MAGIC val arrOfStrDF = tomDF.withColumn("string_array", split(col("value"),"\\W")).drop("value")
// MAGIC arrOfStrDF.printSchema
// MAGIC arrOfStrDF.show(5,false)
// MAGIC 
// MAGIC // Now we will take that DataFrame and turn each array back into a single string using a space as a delimiter
// MAGIC val backToPlainTextDF = arrOfStrDF.withColumn("line_from_array", concat_ws(" ",$"string_array")).drop("string_array")
// MAGIC backToPlainTextDF.printSchema
// MAGIC backToPlainTextDF.show(10,false)

// COMMAND ----------

// MAGIC %python
// MAGIC # Here is the same thing in Python
// MAGIC from pyspark.sql.functions import *
// MAGIC tomDF = spark.read.text("/mnt/training/tom-sawyer/tom.txt")
// MAGIC # our original DF has simple lines of text, 1 column named "value"
// MAGIC tomDF.printSchema()
// MAGIC tomDF.show(5,False)
// MAGIC 
// MAGIC # let's create a dataframe with a single column of type array of strings
// MAGIC arrOfStrDF = tomDF.withColumn("string_array", split(col("value"),"\\W")).drop("value")
// MAGIC arrOfStrDF.printSchema()
// MAGIC arrOfStrDF.show(5,False)
// MAGIC 
// MAGIC # Now we will take that DataFrame and turn each array back into a single string using a space as a delimiter
// MAGIC backToPlainTextDF = (arrOfStrDF.withColumn("line_from_array", 
// MAGIC                                            concat_ws(" ", col("string_array"))).drop("string_array"))
// MAGIC backToPlainTextDF.printSchema()
// MAGIC backToPlainTextDF.show(10,False)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Programmatically retrieve the size of a DataFrame in RAM? 

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.storage.StorageLevel
// MAGIC 
// MAGIC val fileName = "dbfs:/mnt/training/wikipedia/clickstream/2015_02_clickstream.tsv"
// MAGIC val myStringSchema = 
// MAGIC     "prev_id integer, curr_id integer, n integer, prev_title string, curr_title string, type string"
// MAGIC 
// MAGIC val testDF = spark.read
// MAGIC         .option("header",true)
// MAGIC         .option("sep","\t")
// MAGIC         .schema(myStringSchema)
// MAGIC         .format("csv")
// MAGIC         .load(fileName)
// MAGIC         .persist(StorageLevel.MEMORY_ONLY)
// MAGIC 
// MAGIC testDF.foreach(_ => ())  // force materialization of the data into cache
// MAGIC val catalyst_plan = testDF.queryExecution.logical  // grab the logical plan
// MAGIC val executedQuery = spark.sessionState.executePlan(catalyst_plan) //execute the logical plan
// MAGIC 
// MAGIC     // grab the stats from the optimized plan and retrieve the size in bytes
// MAGIC val testDFSizeInBytes = executedQuery.optimizedPlan.stats.sizeInBytes.toLong 
// MAGIC 
// MAGIC val fmstr = "The size of testDF in RAM in %s is: %,.1f %s"
// MAGIC println(fmstr.format("Bytes",testDFSizeInBytes/1.0,"bytes"))
// MAGIC println(fmstr.format("Megabytes",testDFSizeInBytes/1024.0/1024.0,"MBs"))
// MAGIC println(fmstr.format("Gigabytes",testDFSizeInBytes/1024.0/1024.0/1024.0,"Gigs"))
// MAGIC 
// MAGIC println("="*80)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Including a column with the name of the file that supplied a row's values.
// MAGIC 
// MAGIC The `functions` object has an operation that creates a column that include the name of the file from which a row's data was derived. The name of the function is `input_file_name()`. The newly created column will literally be named "**input_file_name()**"" and will have a type of `string`. So it is likely one would want to rename the column with `.alias` or `.name`.  Scala also has `.as` that Python does not have. 
// MAGIC 
// MAGIC From the docs:
// MAGIC <div style="color:slategray; font-weight:normal; font-family:Verdana, Geneva, sans-serif; font-size:16px; margin-left:25px">
// MAGIC Creates a string column for the file name of the current Spark task.
// MAGIC </div>
// MAGIC 
// MAGIC See [functions](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions) documentation

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.functions.{input_file_name}
// MAGIC val path = "/user/evan.troyka@databricks.com/crime/crimeCSV/"
// MAGIC val crimeDF = spark.read.option("sep",",")
// MAGIC                         .option("header","true")
// MAGIC                         .option("inferSchema","true")
// MAGIC                         .csv(path)
// MAGIC                         .select("ID","Case Number","Date")
// MAGIC                         .withColumn("from_filename", input_file_name())
// MAGIC display(crimeDF.limit(5))

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import col, input_file_name
// MAGIC path = "/user/evan.troyka@databricks.com/crime/crimeCSV/"
// MAGIC crimeDF =  (spark.read.option("sep",",")
// MAGIC                       .option("header","true")
// MAGIC                       .option("inferSchema","true")
// MAGIC                       .csv(path)
// MAGIC                       .select("ID","Case Number", "Date", input_file_name().alias("filename")) )
// MAGIC display(crimeDF.limit(5))

// COMMAND ----------

// MAGIC %md
// MAGIC This works with binary files as well. Let's try that same code but with Parquet files instead.

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.functions.{input_file_name}
// MAGIC val path = "/user/evan.troyka@databricks.com/crime/crimeParquet/"
// MAGIC val crimeDF = spark.read.parquet(path)
// MAGIC                         .select("ID","Case_Number","Date")
// MAGIC                         .withColumn("from_filename", input_file_name()) 
// MAGIC display(crimeDF.limit(5))

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import input_file_name
// MAGIC path = "/user/evan.troyka@databricks.com/crime/crimeParquet/"
// MAGIC crimeDF = (spark.read.parquet(path)
// MAGIC                      .select("ID","Case_Number","Date")
// MAGIC                      .withColumn("from_filename", input_file_name()) )
// MAGIC display(crimeDF.limit(5))

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Including a column with the Spark partition id for a row's data
// MAGIC 
// MAGIC The `functions` object has an operation that creates a column that include the id (number that is zero-based) of the Spark partition from which a row's data was derived. The name of the function is `spark_partition_id()`. The newly created column will literally be named "**spark_partition_id()**"" and will have a type of `integer`. So it is likely one would want to rename the column with `.alias` or `.name`.  Scala also has `.as` that Python does not have. 
// MAGIC 
// MAGIC From the docs:
// MAGIC <div style="color:slategray; font-weight:normal; font-family:Verdana, Geneva, sans-serif; font-size:16px; margin-left:25px">
// MAGIC This is non-deterministic because it depends on data partitioning and task scheduling.
// MAGIC </div>
// MAGIC 
// MAGIC See [functions](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions) documentation

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.functions.{spark_partition_id,col}
// MAGIC val path = "/user/evan.troyka@databricks.com/crime/crimeParquet/"
// MAGIC val crimeDF = spark.read.parquet(path)
// MAGIC                         .repartition(5)
// MAGIC                         .select(col("ID"),col("Case_Number"),col("Date"), spark_partition_id().alias("spark_part_id"))
// MAGIC println("The number of partitions is: " + crimeDF.rdd.getNumPartitions)
// MAGIC crimeDF.dropDuplicates("spark_part_id").sort("spark_part_id").show(false)

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import spark_partition_id, col
// MAGIC path = "/user/evan.troyka@databricks.com/crime/crimeParquet/"
// MAGIC crimeDF = (spark.read.parquet(path)
// MAGIC                      .repartition(5)
// MAGIC                      .select(col("ID"),col("Case_Number"),col("Date"), spark_partition_id().alias("spark_part_id")) )
// MAGIC print("The number of partitions is: ", crimeDF.rdd.getNumPartitions())
// MAGIC crimeDF.dropDuplicates(["spark_part_id"]).sort("spark_part_id").show(truncate=False)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC ## Including a column with a unique row id?
// MAGIC 
// MAGIC The `functions` object has an operation that creates a column that generates a unique row id. The name of the function is `monotonically_increasing_id()`. The newly created column will literally be named "**monotonically_increasing_id()**" and will have a type of `long`. It is likely one would want to rename the column with `.alias` or `.name`.  Scala also has `.as` that Python does not have. 
// MAGIC 
// MAGIC From the docs:
// MAGIC <div style="color:slategray; font-weight:normal; font-family:Verdana, Geneva, sans-serif; font-size:16px; margin-left:25px">
// MAGIC A column expression that generates monotonically increasing 64-bit integers.
// MAGIC 
// MAGIC The generated ID is guaranteed to be monotonically increasing and unique, but not consecutive. The current implementation puts the partition ID in the upper 31 bits, and the record number within each partition in the lower 33 bits. The assumption is that the data frame has less than 1 billion partitions, and each partition has less than 8 billion records.
// MAGIC </div>
// MAGIC 
// MAGIC See [functions](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$) documentation

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.functions.{monotonically_increasing_id,col,min,max,format_number}
// MAGIC val path = "/user/evan.troyka@databricks.com/crime/crimeParquet/"
// MAGIC val crimeDF = spark.read.parquet(path)
// MAGIC                         .select(monotonically_increasing_id().alias("generated_id"),
// MAGIC                                 col("ID").alias("original_id"),
// MAGIC                                 col("Case_Number"))
// MAGIC val low  = crimeDF.select(min(col("generated_id"))).first.getLong(0)
// MAGIC val high = crimeDF.select(format_number(max(col("generated_id")),0)).first.getString(0)
// MAGIC println("The lowest generated id value is:\t" + low)
// MAGIC println("The hightest generated id value is:\t" + high)
// MAGIC crimeDF.show(5,false)
// MAGIC crimeDF.sort(col("generated_id").desc).withColumn("generated_id",format_number(col("generated_id"),0)).show(5,false)
// MAGIC println("="*70)

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import monotonically_increasing_id,col,min,max,format_number
// MAGIC path = "/user/evan.troyka@databricks.com/crime/crimeParquet/"
// MAGIC crimeDF = (spark.read.parquet(path)
// MAGIC                       .select(monotonically_increasing_id().alias("generated_id"),
// MAGIC                               col("ID").alias("original_id"),
// MAGIC                               col("Case_Number")) )
// MAGIC low  = crimeDF.select(min(col("generated_id")).alias("min")).first()["min"]
// MAGIC high = crimeDF.select(format_number(max(col("generated_id")),0).alias("max")).first()["max"]
// MAGIC print("The lowest generated id value is:\t", low)
// MAGIC print("The hightest generated id value is:\t", high)
// MAGIC crimeDF.show(5,False)
// MAGIC crimeDF.sort(col("generated_id").desc()).withColumn("generated_id",format_number(col("generated_id"),0)).show(5,False)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC ## What are the configuration options for Delta readers and writers?
// MAGIC 
// MAGIC <pre style="width:55%; color:black; background-color:#f2f2f2; 
// MAGIC            font-weight:normal; font-family:Courier New; font-size:16px; 
// MAGIC             margin-left:15px; ">
// MAGIC myDF = spark.read.option("config-property","config-value")
// MAGIC                   .format("delta")
// MAGIC                   .load("/path/to/delta-table")
// MAGIC                   
// MAGIC spark.write.option("config-property","config-value")
// MAGIC                   .format("delta")
// MAGIC                   .save("/path/to/delta-dir")`               
// MAGIC </pre>
// MAGIC 
// MAGIC | Option Name 	| Type 	| Default Value 	| Reading/ Writing  	| Description 	| Docs link 	|
// MAGIC |----------------------	|---------	|---------------	|-------------------	|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|--------------------------------------------------------------------------------------------------------------------	|
// MAGIC | `replaceWhere` 	| String 	| none 	| Writing 	| An option to overwrite only the data that matches predicates over partition columns. You can selectively overwrite only the data that matches predicates over partition columns. The following option atomically replaces the month of January with the dataframe data: `.option("replaceWhere", "date >= '2017-01-01' AND date <= '2017-01-31'")` 	| [Docs](https://docs.databricks.com/delta/delta-batch.html#overwrite-using-dataframes) 	|
// MAGIC | `mergeSchema` 	| Boolean 	| † 	| Writing 	| An option to allow automatic schema merging during a write operation. Automerging is off when table ACLs are enabled. Columns that are present in the DataFrame but missing from the table are automatically added as part of a write transaction when either this option is `true` or when property `spark.databricks.delta.schema.autoMerge` is set to `true`. Cannot be used with `INSERT INTO` or `.write.insertInto()`. 	| [Docs](https://docs.databricks.com/delta/delta-batch.html#automatic-schema-update) 	|
// MAGIC | `overwriteSchema` 	| Boolean 	| False 	| Writing 	| An option to allow overwriting schema and partitioning during an overwrite write operation. If ACLs are enabled, we can't change the schema of an operation through a write, which requires MODIFY permissions, when schema changes require OWN permissions. By default, overwriting the data in a table does not overwrite the schema. When overwriting a table using `mode("overwrite")` without `replaceWhere`, you may still want to override the schema of the data being written. You can choose to replace the schema and partitioning of the table by setting this to true. 	| [Docs](https://docs.databricks.com/delta/delta-batch.html#replace-table-schema) 	|
// MAGIC | `maxFilesPerTrigger` 	| Integer 	| 1000 	| Reading 	| For streaming: This specifies the maximum number of new files to be considered in every trigger. Must be a positive integer and default is 1000. 	| [Docs](https://docs.databricks.com/delta/delta-streaming.html#delta-table-as-a-stream-source) 	|
// MAGIC | `ignoreFileDeletion` 	| ------ 	| ------ 	|  	| For streaming: **Deprecated** as of [4.1](https://docs.databricks.com/release-notes/runtime/4.1.html#deprecations). Use `ignoreDeletes` or `ignoreChanges`. 	| [Docs](https://docs.databricks.com/release-notes/runtime/4.1.html#deprecations) 	|
// MAGIC | `ignoreDeletes` 	| Boolean 	| False 	| Reading 	|  For streaming: Ignore transactions that delete data at partition boundaries. For example, if your source table is partitioned by date, and you delete data older than 30 days, the deletion will not be propagated downstream, but the stream can continue to operate.††   	| [Docs](https://docs.databricks.com/delta/delta-streaming.html#ignoring-updates-and-deletes) 	|
// MAGIC | `ignoreChanges` 	| Boolean 	| False 	| Reading 	| For streaming: Re-process updates if files had to be rewritten in the source table due to a data changing operation such as `UPDATE`, `MERGE INTO`, `DELETE` (within partitions), or `OVERWRITE`. Unchanged rows may still be emitted, therefore your downstream consumers should be able to handle duplicates. Deletes are not propagated downstream. Option `ignoreChanges` subsumes `ignoreDeletes`. Therefore if you use `ignoreChanges`, your stream will not be disrupted by either deletions or updates to the source table.†† 	| [Docs](https://docs.databricks.com/delta/delta-streaming.html#ignoring-updates-and-deletes) 	|
// MAGIC | `optimizeWrite` 	| Boolean 	| False 	| Writing 	| Whether to add an adaptive shuffle before writing out the files to break skew and coalesce data into chunkier files. 	| [Docs](https://github.com/delta-io/delta/blob/master/src/main/scala/org/apache/spark/sql/delta/DeltaOptions.scala) 	|
// MAGIC 
// MAGIC † Default value derived from `sqlConf.getConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE)` which is set with `spark.databricks.delta.schema.autoMerge.enabled` (<a href='https://github.com/delta-io/delta/blob/master/src/main/scala/org/apache/spark/sql/delta/sources/DeltaSQLConf.scala' target="_blank">source</a>)
// MAGIC 
// MAGIC †† Since Delta Lake tables retain all history by default, in many cases you can delete the output and checkpoint and restart the stream from the beginning.
// MAGIC 
// MAGIC + See [DeltaConfig](https://github.com/delta-io/delta/blob/master/src/main/scala/org/apache/spark/sql/delta/DeltaConfig.scala) source code.
// MAGIC + See [DeltaSqlConf](https://github.com/delta-io/delta/blob/master/src/main/scala/org/apache/spark/sql/delta/sources/DeltaSQLConf.scala) source code.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC ### Example of a `replaceWhere` option in a Delta table.
// MAGIC 
// MAGIC Suppose you want to de-duplicate data in a Delta tables for a specific table partition.
// MAGIC 
// MAGIC <pre style="width:55%; color:black; background-color:#f2f2f2; 
// MAGIC            font-weight:normal; font-family:Courier New; font-size:16px;
// MAGIC             margin-left:15px; ">
// MAGIC spark.sql("select * from table where partition=foo")
// MAGIC       .dropDuplicates()
// MAGIC       .write.format("delta")
// MAGIC       .mode("overwrite")
// MAGIC       .option("replaceWhere", "parition=foo")
// MAGIC       .save("…")
// MAGIC </pre>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### How do I use Koalas in a Databricks notebook?
// MAGIC 
// MAGIC ##### What is Koalas?
// MAGIC 
// MAGIC (from [Koalas: Easy Transition from pandas to Apache Spark](https://databricks.com/blog/2019/04/24/koalas-easy-transition-from-pandas-to-apache-spark.html))
// MAGIC 
// MAGIC <div style="margin-left:20px">
// MAGIC *Koalas is a new (Spring 2019) open source project that augments PySpark’s DataFrame API to make it compatible with pandas.
// MAGIC 
// MAGIC Pandas has been popular for a long time. The problem? pandas does not scale well to big data. It was designed for small data sets that a single machine could handle. On the other hand, Apache Spark has emerged as the de facto standard for big data workloads. Today many data scientists use pandas for coursework, pet projects, and small data tasks, but when they work with very large data sets, they either have to migrate to PySpark to leverage Spark or downsample their data so that they can use pandas.
// MAGIC 
// MAGIC Now with Koalas, data scientists can make the transition from a single machine to a distributed environment without needing to learn a new framework. As you can see below, you can scale your pandas code on Spark with Koalas just by replacing one package with the other.* </div>
// MAGIC 
// MAGIC You will get an error if you install Koalas from a notebook that has a default language of Scala.  Create a notebook with a default of Python and enter the following into a cell. 
// MAGIC &nbsp;
// MAGIC 
// MAGIC <pre style="width:55%; color:black; background-color:#f2f2f2; 
// MAGIC            font-weight:normal; font-family:Courier New; font-size:16px; 
// MAGIC             margin-left:15px; ">
// MAGIC dbutils.library.installPyPI("koalas")
// MAGIC dbutils.library.restartPython()
// MAGIC </pre>
// MAGIC 
// MAGIC 
// MAGIC You can then import and use as follows:
// MAGIC &nbsp;
// MAGIC <pre style="width:65%; color:black; background-color:#f2f2f2; 
// MAGIC            font-weight:normal; font-family:Courier New; font-size:16px;
// MAGIC             margin-left:15px; ">
// MAGIC import databricks.koalas as ks
// MAGIC df = ks.DataFrame({'x': [1, 2], 'y': [3, 4], 'z': [5, 6]})
// MAGIC df.columns = ['x', 'y', 'z1']
// MAGIC df['x2'] = df.x * df.x
// MAGIC </pre>

// COMMAND ----------

// MAGIC %md
// MAGIC ### Sharing values between languages in a Spark notebook.
// MAGIC 
// MAGIC The namespace (scope) for variables within a language is limited to that language. But there are a few hacks around this. 
// MAGIC + For simple data, you can share values...
// MAGIC   + ... through the `spark.conf` object
// MAGIC   + ... through `dbutils.widgets`
// MAGIC + For sharing DataFrames, you can use `createOrReplaceTempView`. 

// COMMAND ----------

// MAGIC %md
// MAGIC Let's look at using `spark.conf` to share data across language namespaces.

// COMMAND ----------

// MAGIC %scala
// MAGIC val scalaVariable = "I'm a scala variable value"
// MAGIC // I am going to store that in the SparkConf object using Scala code
// MAGIC spark.conf.set("com.databricks.training.evan.evanScalaVar", scalaVariable)

// COMMAND ----------

// MAGIC %python
// MAGIC # Now I will retrieve it in some Python code
// MAGIC fromScalaCodeValue = spark.conf.get("com.databricks.training.evan.evanScalaVar")
// MAGIC print("My python variable has a value of: ", fromScalaCodeValue)

// COMMAND ----------

// MAGIC %md
// MAGIC Another way to share simple data (not complex objects like DataFrames), is to use the widgets feature of `dbutils`.  Simply store a value in a widget using one language scope and then retrieve the value in another langauges scope. (Tested with Python and Scala only. I have not tried this with `%sql` or `%r` cells.)

// COMMAND ----------

// MAGIC %scala
// MAGIC val someScalaVar = "Scala value from text field"
// MAGIC // create a widget and put a value in it using one language
// MAGIC dbutils.widgets.text("scalaWidget",someScalaVar, "Scala Value" )

// COMMAND ----------

// MAGIC %python
// MAGIC # retreive a widget value using another language
// MAGIC somePythonVar = dbutils.widgets.get("scalaWidget")
// MAGIC print("My python var has a value of: " + somePythonVar)
// MAGIC dbutils.widgets.remove("scalaWidget")

// COMMAND ----------

// MAGIC %md
// MAGIC The above examples (`spark.conf` and widgets) are really only useful for simple data. If you want to share a DataFrame between Scala code and Python code, use a temporary view. 

// COMMAND ----------

// MAGIC %python
// MAGIC path = "/mnt/training/tom-sawyer/tom.txt"
// MAGIC myPythonDF = spark.read.text(path)
// MAGIC myPythonDF.show(5,False)
// MAGIC myPythonDF.createOrReplaceTempView("python_created_temp_view")

// COMMAND ----------

// MAGIC %md
// MAGIC Let's use a simple SQL statement to see the view we created above in a list of tables. Views have a namespace called "global_temp" but views are associate with no database. 

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC show tables in global_temp

// COMMAND ----------

// MAGIC %md
// MAGIC Now we can use that view to create a DataFrame in Scala from the view created in Python. 

// COMMAND ----------

// MAGIC %scala
// MAGIC val myScalaDF = spark.sql("select * from python_created_temp_view")
// MAGIC myScalaDF.show(5,false)
// MAGIC spark.sql("DROP VIEW IF EXISTS python_created_df_view")
// MAGIC println("="*80)

// COMMAND ----------

// MAGIC %md
// MAGIC Now we clean up :)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC The below `%sql` cell could be programmatically execution in either Python or Scala.
// MAGIC <pre style="width:75%; color:black; background-color:#f2f2f2; 
// MAGIC            font-weight:normal; font-family:Courier New; font-size:16px; 
// MAGIC             margin-left:15px; ">
// MAGIC spark.sql("drop view if exists evan_temp_view")
// MAGIC spark.sql("drop view if exists global_temp.evan_global_temp_view")
// MAGIC spark.sql("drop view if exists python_created_temp_view") </pre>

// COMMAND ----------

// MAGIC %sql
// MAGIC drop view if exists evan_temp_view;
// MAGIC drop view if exists global_temp.evan_global_temp_view;
// MAGIC drop view if exists python_created_temp_view;
// MAGIC show tables in global_temp;

// COMMAND ----------

// MAGIC %scala
// MAGIC dbutils.widgets.removeAll
// MAGIC dbutils.fs.rm("/user/evan.troyka@databricks.com/temp/",true)

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.widgets.removeAll()
// MAGIC dbutils.fs.rm("/user/evan.troyka@databricks.com/temp/",True)

// COMMAND ----------

// MAGIC %md
// MAGIC ## How can I share values from one notebook to another without using `%run`?
// MAGIC 
// MAGIC <pre>
// MAGIC %python
// MAGIC __builtins__.foo=5
// MAGIC %python
// MAGIC #In New Notebook
// MAGIC println(foo)
// MAGIC 
// MAGIC Scala
// MAGIC 
// MAGIC %scala
// MAGIC package foo;
// MAGIC object Bar {
// MAGIC  var baz=5;
// MAGIC }
// MAGIC %scala
// MAGIC foo.Bar.baz=6 (
// MAGIC </pre>

// COMMAND ----------

// MAGIC %md
// MAGIC ### Run the following Python code in this notebook.

// COMMAND ----------

// MAGIC %python
// MAGIC __builtin__.foo=5

// COMMAND ----------

// MAGIC %md 
// MAGIC #### Now run this Python code from another notebook that is currently attached to the same cluster as this notebook.
// MAGIC <pre>
// MAGIC %python
// MAGIC # In another notebook 
// MAGIC # That notebook needs to be currently attached to the same cluster
// MAGIC println(foo)
// MAGIC </pre>

// COMMAND ----------

// MAGIC %scala
// MAGIC package myfoo;
// MAGIC object Bar {
// MAGIC  var baz=5;
// MAGIC }

// COMMAND ----------

// MAGIC %md
// MAGIC <pre>
// MAGIC %scala
// MAGIC val fooBarBaz = myfoo.Bar.baz
// MAGIC println(fooBarBaz)
// MAGIC </pre>