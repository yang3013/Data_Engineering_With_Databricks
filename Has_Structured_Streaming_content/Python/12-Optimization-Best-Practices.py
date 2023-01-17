# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Optimization Best Practices
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Understand a range of join operations
# MAGIC * Omit expensive operations
# MAGIC * Understand persistence

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Introduction
# MAGIC 
# MAGIC In this lesson, we'll review a number of techniques that we can use to optimize performance.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Join Optimizations
# MAGIC 
# MAGIC There are a number of physical join types, summarized as follows:
# MAGIC 
# MAGIC | Name | Notes |
# MAGIC | ---- | ----- |
# MAGIC | Sort Merge Join | Scalable - just works |
# MAGIC | Broadcast Hash Join | One side must fit in memory |
# MAGIC | Broadcast Nested Loop Join | One side must fit in memory |
# MAGIC | Skew Join | Data unevenly distributed among partitions |
# MAGIC | Range Join | Using point in interval or interval overlap |
# MAGIC | Cartesian Product Join | Avoid if you can |
# MAGIC 
# MAGIC ### Sort Merge Join
# MAGIC 
# MAGIC A sort merge join is the default join. It is very scalable and provides good performance for large tables.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/images/join-standard.png" style="max-height:400px"/></p>
# MAGIC </div>
# MAGIC 
# MAGIC Here we see how all the records keyed by "green" are moved to the same partition. The process would be repeated for "red" and "blue" records.
# MAGIC 
# MAGIC | Step | Phase | Description |
# MAGIC | ---- | ----- | ----------- |
# MAGIC | 1 | Shuffle | Tables are repartitioned across the partitions in the cluster using the join keys |
# MAGIC | 2 | Sort | The data are sorted within each partition |
# MAGIC | 3 | Merge | Join the sorted and partitioned data |
# MAGIC 
# MAGIC ### Broadcast Hash Join
# MAGIC 
# MAGIC There may often be a need to join a smaller table with a significantly larger table. If the smaller table can fit in memory on a single machine in the cluster, consider using a broadcast hash join (broadcast join). The broadcast join will shuffle the contents of the smaller table to every machine in the cluster and each will store the table in memory. This avoids shuffling the much larger table.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/images/join-broadcasted.png" style="max-height:400px"/></p>
# MAGIC </div>
# MAGIC 
# MAGIC Here we see the records keyed by "red" being replicated into the first partition. The process would be repeated for each executor. The entire process would be repeated again for "green" and "blue" records.
# MAGIC 
# MAGIC Depending upon the size of the data that is being loaded into Spark, Spark uses internal heuristics to decide how to join data to other data. The catalyst optimizer will attempt to identify tables that can be broadcast automatically.
# MAGIC 
# MAGIC Automatic broadcast depends upon `spark.sql.autoBroadcastJoinThreshold`. This setting configures the maximum size in bytes for a table that will be broadcast to all worker nodes when performing a join. The default size is 10 MB. Statistics must also be available to the optimizer to use this join type.
# MAGIC 
# MAGIC We can also use a broadcast hint to force the broadcast of the table. We can add one or more hints to a `SELECT` statement inside `` /*+ ... */ `` comment blocks. Multiple hints can be specified inside the same comment block, in which case the hints are separated by commas, and there can be multiple such comment blocks. A hint has a name (for example, `BROADCAST`) and accepts 0 or more parameters. For example:
# MAGIC 
# MAGIC `` SELECT /*+ BROADCAST(customers) */ * ``<br>
# MAGIC `FROM customers, orders`<br>
# MAGIC `WHERE o_custId = c_custId`
# MAGIC 
# MAGIC ### Broadcast Nested Loop Join
# MAGIC 
# MAGIC This join is performed as a fallback if
# MAGIC * No join keys are specified
# MAGIC * It is not an inner join
# MAGIC * Either table has a broadcast hint or one side of the join is less than `spark.sql.autoBroadcastJoinThreshold` and can be broadcast
# MAGIC 
# MAGIC ### Skew Join
# MAGIC 
# MAGIC Data skew is a condition in which a table's data is unevenly distributed among partitions in the cluster. Data skew can severely downgrade performance of queries, especially those with joins.
# MAGIC 
# MAGIC Data skew can be diagnosed by visualizing the size of files or key distribution of the data. We can also identify data skew by checking partition execution metrics in the Spark UI. The Spark UI shows the quartiles of the task execution distribution and is very useful. If there is a significant difference between the **min** and **max** values in any of the metrics, further investigation should be undertaken on the data skew. Other diagnosis steps include:
# MAGIC * Check the stage that is stuck and verify that it is doing a join
# MAGIC * After a query finishes, find the stage that does a join and check the task duration distribution
# MAGIC * Sort the tasks by decreasing duration and check the first few tasks, and if one task took much longer to complete than the other tasks, there is skew
# MAGIC 
# MAGIC To ameliorate skew, Databricks SQL accepts skew hints in queries. For example:
# MAGIC 
# MAGIC | Type | Code |
# MAGIC | ---- | ---- |
# MAGIC | Table with skew | `` SELECT /*+ SKEW('orders') */ * ``<br> `FROM customers, orders`<br> `WHERE o_custId = c_custId` |
# MAGIC | Subquery with skew | `` SELECT /*+ SKEW('C1') */ * ``<br> `FROM (SELECT * FROM customers WHERE c_custId < 100) C1, orders`<br> `WHERE C1.c_custId = o_custId` |
# MAGIC 
# MAGIC With the information from these hints, Spark can construct a better query plan that does not suffer from data skew.
# MAGIC 
# MAGIC ### Range Join
# MAGIC 
# MAGIC A range join occurs when two relations are joined using a **point in interval** or **interval overlap**. The range join optimization support in Databricks Runtime can bring orders of magnitude improvement in query performance.
# MAGIC 
# MAGIC A point in interval range join is a join in which the condition contains predicates specifying that a value from one relation is between two values from the other relation. An interval overlap range join is a join in which the condition contains predicates specifying an overlap of intervals between two values from each relation. Here are some examples:
# MAGIC 
# MAGIC | Type | Example | Code |
# MAGIC | ---- | ------- | ---- |
# MAGIC | Point in interval | Using BETWEEN expressions | `SELECT *`<br> `FROM points`<br> `JOIN ranges ON points.p BETWEEN ranges.start and ranges.end` |
# MAGIC | Interval overlap | Overlap of [r1.start, r1.end] with [r2.start, r2.end] | `SELECT *`<br> `FROM r1 JOIN r2 ON r1.start < r2.end AND r2.start < r1.end` |
# MAGIC 
# MAGIC The range join optimization is performed for joins that:
# MAGIC * Have a condition that can be interpreted as a point in interval or interval overlap range join
# MAGIC * All values involved in the range join condition are of a numeric type (integral, floating point, decimal), DATE, or TIMESTAMP
# MAGIC * All values involved in the range join condition are of the same type, and in the case of the decimal type, the values also need to be of the same scale and precision
# MAGIC * It is an `INNER JOIN`, or in case of point in interval range join, a `LEFT OUTER JOIN` with point value on the left side, or `RIGHT OUTER JOIN` with point value on the right side
# MAGIC * Have a bin size tuning parameter
# MAGIC 
# MAGIC The bin size is a numeric tuning parameter that splits the values domain of the range condition into multiple bins of equal size. For example, with a bin size of 10, the optimization splits the domain into bins that are intervals of length 10.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/bin-size.png" style="max-height:400px"/></p>
# MAGIC </div>
# MAGIC 
# MAGIC To enable the range join optimization in a SQL query, we can use a range join hint to specify the bin size. The hint must contain the relation name of one of the joined relations and the numeric bin size parameter. If modifying the query is not possible, we can specify the bin size as a configuration parameter. Here are some examples:
# MAGIC 
# MAGIC | Type | Code |
# MAGIC | ---- | ---- |
# MAGIC | Range join hint | `` SELECT /*+ RANGE_JOIN(points, 10) */ * ``<br> `FROM points`<br> `JOIN ranges ON points.p >= ranges.start AND points.p < ranges.end` |
# MAGIC | Configuration parameter | `SET spark.databricks.optimizer.rangeJoin.binSize = 5` |
# MAGIC 
# MAGIC **Note**: A different bin size set through a range join hint always overrides the one set through the configuration.
# MAGIC 
# MAGIC The choice of the bin size parameter is extremely significant when determining the effectiveness of this optimization technique. Either set the bin size based on *a priori* knowledge of the data or using a numerical estimation technique *a posteriori*.
# MAGIC 
# MAGIC The range join optimization is applied only if we manually specify the bin size. <a href="https://docs.databricks.com/delta/join-performance/range-join.html#id2">Choose the bin size</a> describes how to choose an optimal bin size.
# MAGIC 
# MAGIC ### Cartesian Product Join
# MAGIC 
# MAGIC A cartesian product join is a join where each row of one dataset is joined with each row of another dataset. For example, if we have a dataset with **A** rows and we join it with another dataset with **B** rows, the cartesian product will be **A x B** rows. We'll look at an example shortly.
# MAGIC 
# MAGIC The output cardinality is the size of the result set from a query. This is equivalent to the number of rows and columns in the final result of an SQL query. Estimating the cardinality of the output can help visualize the execution steps of the query and identify opportunities to optimize the performance of an application. It is also a useful sanity check for the business logic of an application.
# MAGIC 
# MAGIC Consider the following example:
# MAGIC * Table A has 200 rows, each with a distinct key
# MAGIC * Table B has 70,000 rows
# MAGIC * Table C has 150 rows
# MAGIC 
# MAGIC Let's perform an inner join of Table A onto Table B. What is the range of the output cardinality of the result? The range is the lower and upper bounds for the number of matching keys in the join. The lower bound is 0. The upper bound is 70,000 and will occur in every record in Table B that has a matching key in Table A.
# MAGIC 
# MAGIC Let’s now perform a cartesian product join of Tables A and B. Every row in Table A is joined onto Table B. That gives us 200 x 70,000 = 14,000,000 rows.
# MAGIC 
# MAGIC Finally, let's perform a cartesian join of the result above (Table A x Table B) to Table C. That would be 14,000,000 x 150 = 2.1 billion rows.
# MAGIC 
# MAGIC By default, a cartesian join is disabled and can be enabled as follows:
# MAGIC 
# MAGIC `spark.conf.set("spark.sql.crossJoin.enabled", "true")`

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Omit Expensive Operations
# MAGIC 
# MAGIC ### Repartition
# MAGIC 
# MAGIC Repartition is a **wide** transformation and can be used to increase or decrease the number of partitions. However, it has the added cost of a shuffle operation, which can be expensive. But repartition can be useful for removing data skew. Instead of using repartition, there are a number of other operations we can also use to control the number of output files from the number of partitions:
# MAGIC 
# MAGIC | Operation | File size | Description |
# MAGIC | --------- | --------- | ----------- |
# MAGIC | Change number of shuffle partitions | Increase or Decrease | A shuffle stage often immediately precedes a write. We can use `spark.conf.set("spark.sql.shuffle.partitions", n)` to control the number of output files. |
# MAGIC | Coalesce | Increase | We can reduce the number of partitions but care is required if the last stage is a big shuffle. Also, we may not be fully utilizing cluster resources. |
# MAGIC | `maxRecordsPerFile` | Decrease | It is not common to have the file size smaller than a partition, but it can be done using `df.write.option(“maxRecordsPerFile”, n)` |
# MAGIC 
# MAGIC ### COUNT
# MAGIC 
# MAGIC Overuse of this command can cause performance problems. For development environments and small datasets, it can be valuable to confirm that correct results are being returned but use judiciously in production environments.
# MAGIC 
# MAGIC ### DISTINCT
# MAGIC 
# MAGIC The `DISTINCT` command can be very expensive, since it requires all data to be scanned, shuffled, and duplicates dropped. When using this command, we should keep in mind the following:
# MAGIC 
# MAGIC | Option | Code | Notes |
# MAGIC | ------ | ---- | ----- |
# MAGIC | Minimize scope | | Use `DISTINCT` on the smallest subset of the data. |
# MAGIC | Lazy Loading/Data Skipping | | Load only what is required and qualify with `WHERE` |
# MAGIC | Distinct before a join | | Remove duplicates before join and again after join |
# MAGIC | `approx_count_distinct()` | `df.select(approx_count_distinct("StockCode", 0.05))`<br> `SELECT approx_count_distinct(StockCode, 0.05) FROM ...` | Very fast Spark function with margin of error. No shuffle. |
# MAGIC | `dropDuplicates()` | `df.dropDuplicates("ColumnA", "ColumnB")` | Distinct on a few columns. Use before **join** and before **groupBy**. |

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Persistence
# MAGIC 
# MAGIC The most well-known method in Spark to persist data is to use `.cache()`. However, Spark provides a wide-range of storage levels that can be used with `.persist()`, such as:
# MAGIC 
# MAGIC | StorageLevel | Code | Notes |
# MAGIC | ------------ | ---- | ----- |
# MAGIC | Memory and disk | `df.persist(StorageLevel.MEMORY_AND_DISK)` | Default and equivalent to `df.cache()` |
# MAGIC | Disk | `df.persist(StorageLevel.DISK_ONLY)` | |
# MAGIC | Memory | `df.persist(StorageLevel.MEMORY_ONLY)` | |
# MAGIC 
# MAGIC **Tip**: These persistence storage levels are not available in SQL, although it is possible to use `CACHE TABLE`.
# MAGIC 
# MAGIC As `.persist()` is not an action, when an action is called on a Dataframe, the Dataframe will persist any partitions accessed during the next action. If an action does not need all the partitions, then only a subset of the Dataframe will be cached. This can be checked from **Spark UI > Storage > Fraction Cached**. The storage footprint can also be reduced by only selecting columns that are repeatedly used.
# MAGIC 
# MAGIC **Tip**: After declaring a Dataframe, use a `.cache()` or `.persist()`. Next, it is a best practice is to call an action to access all partitions. For example:
# MAGIC 
# MAGIC | .cache() | .persist() |
# MAGIC | -------- | ---------- |
# MAGIC | `employee = spark.read ...`<br> `employee_cache = employee.cache()`<br> `employee_cache.count()` | `employee = spark.read ...`<br> `employee_persist = employee.persist(StorageLevel.DISK_ONLY)`<br> `employee_persist.count()` |
# MAGIC 
# MAGIC **Tip**: Check data size carefully to decide if caching or persisting is appropriate and, if so, which storage level is most suitable. Since caching or persisting reduces memory that Spark may need for other operations, it is important to choose what to cache or persist carefully.
# MAGIC 
# MAGIC After any cached data are no longer required, it is important to clean-up using `.unpersist()` to free-up resources. For SQL, the command would be `UNCACHE TABLE`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC In this lesson we have covered several important optimization techniques.
# MAGIC 
# MAGIC There are a range of different join operations and we have seen the benefits that each of them can provide, as well as some of the trade-offs we need to consider.
# MAGIC 
# MAGIC Next, we discussed several expensive operations that also need to be carefully considered for production systems.
# MAGIC 
# MAGIC Finally, Spark has a number of storage levels, and caching can provide performance benefits for data that need to be frequently accessed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC [Databricks Documentation - Skew Join optimization](https://docs.databricks.com/delta/join-performance/skew-join.html)
# MAGIC 
# MAGIC [Databricks Documentation - Range Join optimization](https://docs.databricks.com/delta/join-performance/range-join.html)
# MAGIC 
# MAGIC [Apache Spark Core Deep Dive: Proper Optimization](https://www.youtube.com/watch?v=daXEp4HmS-E)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>