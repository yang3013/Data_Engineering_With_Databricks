# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Partition Best Practices
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Define a Spark partition
# MAGIC * Understand partition sizing for Read Partitions, Shuffle Partitions, and Write Partitions
# MAGIC * Understand the Hash Partitioner, and Range Partitioner

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Introduction
# MAGIC 
# MAGIC In this lesson, we will discuss partitions. We can define a **partition** as a small piece of the total data set in a system.
# MAGIC 
# MAGIC Google defines a partition like this:
# MAGIC > the action or state of dividing or being divided into parts.
# MAGIC 
# MAGIC When we are working with Spark, generally a Spark action tends to be one of three main operations:
# MAGIC 0. Read
# MAGIC 0. Transform
# MAGIC 0. Write
# MAGIC 
# MAGIC These operations map extremely well to three different types of Spark partitions:
# MAGIC 
# MAGIC | Type | Description |
# MAGIC | ---- | ----------- |
# MAGIC | Read | These partitions are essentially a map of how data is going to be split-up so that it can flow into Spark tasks and can then be transformed and sent to future stages. |
# MAGIC | Shuffle | These partitions are used when shuffling data for joins or aggregations. |
# MAGIC | Write | These partitions send the final data to storage. |
# MAGIC 
# MAGIC We will discuss these three partition types further in the following sections.
# MAGIC 
# MAGIC **Note**: Partitions are widely used in many different technologies and it is important to appreciate and understand, for example, that a Hive partition and a Spark partition are not the same concept.
# MAGIC 
# MAGIC To ensure the best Spark performance, we need to understand:
# MAGIC * How many partitions we have in each stage
# MAGIC * How much data is in each partition
# MAGIC * How the data are being divided-up into the partitions
# MAGIC * How to change the number and size of the partitions to achieve balance
# MAGIC 
# MAGIC Let's see how we can achieve the best performance through best practices.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This data uses the Wikipedia **Pageviews By Seconds** data set.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# Use a schema to avoid the overhead of inferring the schema
# In the case of CSV/TSV it requires a full scan of the file.
schema = StructType(
  [
    StructField("timestamp", StringType(), False),
    StructField("site", StringType(), False),
    StructField("requests", IntegerType(), False)
  ]
)

fileName = "/mnt/training/wikipedia/pageviews/pageviews_by_second.tsv"

# Create our initial Dataframe
initialDF = (spark.read
  .option("header", "true")
  .option("sep", "\t")
  .schema(schema)
  .csv(fileName)
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's display our data.

# COMMAND ----------

display(initialDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We can see above that our data consists of:
# MAGIC * When the record was created
# MAGIC * The site (mobile or desktop)
# MAGIC * The number of requests
# MAGIC 
# MAGIC There is one record per site, per second, and captures the number of requests made in that one second.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Partition Basics
# MAGIC 
# MAGIC ###1. Read Partitions
# MAGIC 
# MAGIC Let's now see what the default level of parallelism is.
# MAGIC 
# MAGIC In a system with 8 cores, we would expect to see default parallelism of 8.
# MAGIC 
# MAGIC By default, we can read up to 128 MB per partition when loading data, which will give us 8 partitions if data <= 1024 MB (1 GB). Our partition size would need to be changed if, for example, we were working with much larger data in memory.

# COMMAND ----------

cores = sc.defaultParallelism

print("There are {} cores, or slots.".format(cores))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's find out how many partitions we have.

# COMMAND ----------

partitions = initialDF.rdd.getNumPartitions()

print("There are {} partitions.".format(partitions))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Finally, let's see what the default size of a partition is.

# COMMAND ----------

maxPartitionBytes = int(spark.conf.get("spark.sql.files.maxPartitionBytes"))

print("maxPartitionBytes is set to {} MB.".format(int(maxPartitionBytes / (1024 * 1024))))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC So, we have:
# MAGIC * Default parallelism is 8
# MAGIC * Partition size is 128 MB
# MAGIC 
# MAGIC `default parallelism * maxPartitionBytes == 8 * 128 == 1024`
# MAGIC 
# MAGIC When using a large number of cores, reducing the default partition size below 128 MB will help utilize all available cores for read and later stages.
# MAGIC 
# MAGIC **Note**: Default parallelism is normally equal to the sum of cores from all executors or, with shuffle operations, it is equal to the largest number of partitions in the parent RDD. Therefore, it should not generally be changed but, based upon the environment, it may appropriate to do so.

# COMMAND ----------

# MAGIC %md
# MAGIC ###2. Shuffle Partitions
# MAGIC 
# MAGIC When wide transformations are performed, such as `groupBy` or `join`, data need to be moved in the cluster to the core that requires that data. Spark uses Shuffle Partitions to accomplish this task. The Shuffle Partitions are temporarily written to local storage attached to an executor.
# MAGIC 
# MAGIC We can check the default value, as follows:

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We can change this setting with the following command:

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC And double-check that it has been changed:

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **Tip**: When using autoscaling in some cloud environments, scaling down the number of Shuffle Partitions during an action may result in the loss of some Shuffle Partitions. Losing Shuffle Partitions will result in task failure. Therefore, it is important to weigh-up **cost** against **predictable completion time** when using autoscaling in cloud environments.

# COMMAND ----------

# MAGIC %md
# MAGIC ###3. Write Partitions
# MAGIC 
# MAGIC The Write Partitions send the final data to persistent storage. How many files are created on persistent storage is determined by the number of Write Partitions and their contents. Very often, a write is combined with a final shuffle and the number of output files will be equal to the value of `spark.sql.shuffle.partitions`.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Optimizing Partitions
# MAGIC 
# MAGIC ###1. Read Partitions
# MAGIC 
# MAGIC We have previously seen the default value of `spark.sql.files.maxPartitionBytes` and we can modify this to control the number of partitions using, for example:
# MAGIC 
# MAGIC `spark.conf.set("spark.sql.files.maxPartitionBytes", n)`
# MAGIC 
# MAGIC where the value of *n* is in bytes, e.g. 16777216 (16 MB).
# MAGIC 
# MAGIC For the majority of cases, the value of `spark.sql.files.maxPartitionBytes` does not need to be modified. However, there are a number of scenarios where we may wish to adjust the Read Partition size:
# MAGIC 0. Where there is a need to increase parallelism
# MAGIC 0. Where there is heavily compressed data
# MAGIC 0. Where there is heavily nested or repetitive data
# MAGIC 0. Where there is data generation using the `explode()` function
# MAGIC 
# MAGIC Let's discuss these in turn.
# MAGIC 
# MAGIC ####1.1 Parallelism
# MAGIC 
# MAGIC Let's consider an example where there is a need to meet a particular Service Level Agreement (SLA). Let's assume that the required number of cores will also be available to meet that SLA.
# MAGIC 
# MAGIC In our example, we have 10 GB of new data arriving into our system. Using the default value for `spark.sql.files.maxPartitionBytes` of 128 MB, that means we need 80 partitions. In our cluster, let's say that it takes 20 seconds to process 1 partition. However, our SLA is 10 seconds or less to process 1 partition.
# MAGIC 
# MAGIC To meet the SLA we can, therefore, double the number of partitions to 160 by reducing the value of `spark.sql.files.maxPartitionBytes` to 64 MB. We would also need to increase the core count to at least 160.
# MAGIC 
# MAGIC ####1.2 Heavily Compressed Data
# MAGIC 
# MAGIC There may be occasional and rare situations where highly compressed data on disk balloons into very large data. This could seriously limit parallelism. It is, therefore, important to use the Spark UI to check the shuffle sizes.
# MAGIC 
# MAGIC ####1.3 Heavily Nested Data
# MAGIC 
# MAGIC Data coming into Spark may be heavily nested. For example, consider an array with nested arrays. Some of the nested arrays may be quite long and cause spills. Furthermore, arrays larger than JVM memory will result in OOM errors.
# MAGIC 
# MAGIC **Tip**: Prevent unlimited length nested data coming into the Spark environment by validating the size of nested data at the source.
# MAGIC 
# MAGIC ####1.4 Generating Data
# MAGIC 
# MAGIC Functions such as `explode()` can generate considerable data and may make Read Partitions very large. It is, therefore, important to control the use of such functions to prevent this problem.

# COMMAND ----------

# MAGIC %md
# MAGIC ###2. Shuffle Partitions
# MAGIC 
# MAGIC A shuffle operation can be expensive in Spark, but there are a number of ways to mitigate this.
# MAGIC 
# MAGIC Recall that we saw earlier that the default number of Shuffle Partitions in Spark is 200. It is recommended to keep the target size of each shuffle partition to 200 MB or less per shuffle task given the default cluster configuration and 8 GB per core. With compute-intensive cluster nodes we may only get 2 GB per core and so 200 MB should be reduced to 50 MB.
# MAGIC 
# MAGIC Using 200 partitions of 200 MB each means that the maximum we could store would be approximately 40 GB. How could we manage larger data?
# MAGIC 
# MAGIC When sizing the number of Shuffle Partitions, there are two key considerations:
# MAGIC 0. The quantity of data arriving into the largest shuffle stage, across all jobs, of an action
# MAGIC 0. The largest size of each Shuffle Partition
# MAGIC 
# MAGIC Using this information, we can formulate an equation to determine the number of shuffle partitions to use. Let's look at an example, assuming the default cluster configuration mentioned above:

# COMMAND ----------

target_partition_size       = 200     # 200 MB
largest_shuffle_stage_input = 4000000 #  ~4 TB

shuffle_partition_count = int(largest_shuffle_stage_input / target_partition_size)

print(shuffle_partition_count)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Cluster size is also important and if the number of cores in the cluster is smaller than the partitions calculated above, use the number of cores. The reason is that we should not waste cores during the second round of shuffle although we may slightly increase the partition file size. However, if the number of cores is larger than the partitions calculated above, reduce the cluster size so that other users can also benefit from it.
# MAGIC 
# MAGIC Let's look at another example with partitions as a factor of the number of cores:

# COMMAND ----------

target_partition_size       = 100     #  100 MB
largest_shuffle_stage_input = 55300   # 55.3 GB

shuffle_partition_count = int(largest_shuffle_stage_input / target_partition_size)

print(shuffle_partition_count)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC If we have a system with 128 cores, then:

# COMMAND ----------

number_of_cores = 128

shuffle_partition_count = int(shuffle_partition_count / number_of_cores) * number_of_cores

print(shuffle_partition_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ###3. Write Partitions
# MAGIC 
# MAGIC It is also very important to understand Write Partitions in terms of:
# MAGIC * File size and number of files
# MAGIC * File content
# MAGIC 
# MAGIC One major problem when working with large files is that this may reduce performance since not all cores may be fully utilized. For example, consider that we have a cluster of 100 cores and we need to write out our data to 1 GB files and we have 10 GB of data. We would be using 10 cores to achieve this, but the remaining 90 would be idle. Therefore, we need to ensure that our cluster is being utilized for other operations to increase parallelism and throughput.
# MAGIC 
# MAGIC When reducing the file count to increase the file size, we can use `repartition(n)` or `coalesce(n)`.
# MAGIC 
# MAGIC ####repartition(n) or coalesce(n)
# MAGIC 
# MAGIC In the Spark API documentation, `coalesce(n)` is described as follows:
# MAGIC > Returns a new Dataset that has exactly numPartitions partitions, when fewer partitions are requested.<br/>
# MAGIC > If a larger number of partitions is requested, it will stay at the current number of partitions.
# MAGIC 
# MAGIC In the Spark API documentation, `repartition(n)` is described as follows:
# MAGIC > Returns a new Dataset that has exactly numPartitions partitions.
# MAGIC 
# MAGIC The key difference between the two:
# MAGIC * `coalesce(n)` is a **narrow** transformation and can only be used to reduce the number of partitions
# MAGIC * `repartition(n)` is a **wide** transformation and can be used to reduce or increase the number of partitions
# MAGIC 
# MAGIC So, if we are increasing the number of partitions, we have only one choice: `repartition(n)`
# MAGIC 
# MAGIC If we are reducing the number of partitions, we can use either one:
# MAGIC * `coalesce(n)` is a **narrow** transformation and performs better because it avoids a shuffle. However, `coalesce(n)` cannot guarantee even **distribution of records** across all partitions. For example, with `coalesce(n)` we may have a few partitions containing 80% of all the data.
# MAGIC * `repartition(n)` will give us a relatively **uniform distribution**. But `repartition(n)` is a **wide** transformation meaning we have the added cost of a shuffle operation.
# MAGIC 
# MAGIC Let's look at an example:

# COMMAND ----------

# Create our initial Dataframe. We can let it infer the
# schema because the cost for parquet files is really low.
alternateDF = (spark.read
  .parquet("/mnt/training/wikipedia/pageviews/pageviews_by_second.parquet")
)

print("There are {} partitions.".format(alternateDF.rdd.getNumPartitions()))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC In our case, if we need to go from 5 partitions to 8 partitions, our only option is `repartition(n)`.

# COMMAND ----------

repartitionedDF = alternateDF.repartition(8) # Partitioned by row hash

print("There are {} partitions.".format(repartitionedDF.rdd.getNumPartitions()))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC There are two default Spark partitioners:
# MAGIC 0. Hash Partitioner
# MAGIC 0. Range Partitioner
# MAGIC 
# MAGIC The base partitioner can be also extended to implement a Custom Partitioner, if required. However, this requires dropping out of SQL and working with Spark at a lower-level. The effort required to write a new partitioner may, therefore, may be quite costly to do.
# MAGIC 
# MAGIC In the call to repartition, the Hash Partitioner will evenly distribute the data over all the partitions. The distribution will be near-perfect, unless there are many duplicate rows of data in which case there will be skew.
# MAGIC 
# MAGIC The Range Partitioner can be used to distribute data based upon the values in column(s). However, this is likely to introduce skew. Examples of Range Partitioning include:
# MAGIC 
# MAGIC | Type | Example |
# MAGIC | ---- | ------- |
# MAGIC | Repartition function call | `df.repartition(8, columnName)` |
# MAGIC | Join | `dfA.join(dfB, Seq(columnA, columnB))` |
# MAGIC | groupBy | `df.groupBy(columnName).agg(...)` |
# MAGIC 
# MAGIC Let's see an example of range partitioning:

# COMMAND ----------

(repartitionedDF
  .orderBy(col("timestamp"), col("site")) # Sort the data
  .foreach(lambda x: None)                # Litterally does nothing except trigger a job
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We executed one action, but two jobs were triggered. If we look at the physical plan, using `explain()`, we can see that the reason for the extra job is **Exchange rangepartitioning**. So, Spark is executing an extra job so that it can use range partitioning. 
# MAGIC 
# MAGIC If we rerun with only 3 Million records, we should see that we have only one job and no attempt to use range partitioning.

# COMMAND ----------

(repartitionedDF
  .orderBy(col("timestamp"), col("site"))
  .explain()
)
print("-"*80)

(repartitionedDF
  .orderBy(col("timestamp"), col("site"))
  .limit(3000000)
  .explain()
)
print("-"*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC In this lesson we have discussed Spark partitions.
# MAGIC 
# MAGIC We have defined what a partition is and described Read Partitions, Shuffle Partitions, and Write Partitions. We have also discussed some best practices and techniques for sizing these different types of partitions.
# MAGIC 
# MAGIC Finally, we have discussed the Hash Partitioner and Range Partitioner.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC [Apache Spark Core Deep Dive: Proper Optimization](https://www.youtube.com/watch?v=daXEp4HmS-E)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>