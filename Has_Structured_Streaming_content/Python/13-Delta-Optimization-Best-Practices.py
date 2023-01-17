# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Optimization Best Practices
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Use advanced Delta features to optimize a Delta Lake

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## ![Delta Logo Tiny](https://files.training.databricks.com/courses/hadoop-migration/delta-lake-tiny-logo.png) Introduction
# MAGIC 
# MAGIC Delta Lake is an open source storage layer that brings reliability to data lakes. Delta Lake provides ACID transactions, scalable metadata handling, and unifies streaming and batch data processing. Delta Lake runs on top of an existing data lake and is fully compatible with Apache Spark APIs.
# MAGIC 
# MAGIC Delta Lake on Databricks allows you to configure Delta Lake based on your workload patterns and provides optimized layouts and indexes for fast interactive queries.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/parquet-to-delta.png" style="max-height:400px"/></p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Delta Logo Tiny](https://files.training.databricks.com/courses/hadoop-migration/delta-lake-tiny-logo.png) Migrate Workloads to Delta Lake
# MAGIC 
# MAGIC Delta Lake handles the following operations automatically.
# MAGIC 
# MAGIC ### REFRESH TABLE
# MAGIC 
# MAGIC Delta tables always return the most up-to-date information, so there is no need to manually call `REFRESH TABLE` after changes.
# MAGIC 
# MAGIC ### Add and Remove Partitions
# MAGIC 
# MAGIC Delta Lake automatically tracks the set of partitions present in a table and updates the list as data is added or removed. As a result, there is no need to run `ALTER TABLE [ADD|DROP] PARTITION` or `MSCK`.
# MAGIC 
# MAGIC ### Load a Single Partition
# MAGIC 
# MAGIC As an optimization, we may sometimes directly load the partition of data we are interested in. For example:
# MAGIC 
# MAGIC `spark.read.parquet("/data/date=2017-01-01")`
# MAGIC 
# MAGIC This is unnecessary with Delta Lake, since it can quickly read the list of files from the transaction log to find the relevant ones.
# MAGIC 
# MAGIC If we are interested in a single partition, we can specify it using a `WHERE` clause. For example:
# MAGIC 
# MAGIC `spark.read.delta("/data").where("date = '2017-01-01'")`
# MAGIC 
# MAGIC For large tables with many files in the partition, this can be much faster than loading a single partition (with direct partition path, or with `WHERE`) from a Parquet table because listing the files in the directory is often slower than reading the list of files from the transaction log.
# MAGIC 
# MAGIC When we port an existing application to Delta Lake, we should avoid the following operations, which bypass the transaction log:
# MAGIC 
# MAGIC | Operation | Description |
# MAGIC | --------- | ----------- |
# MAGIC | Manually modify data | Delta Lake uses the transaction log to atomically commit changes to a table. Because the log is the source of truth, files that are written out but not added to the transaction log are not read by Spark. Similarly, even if we manually delete a file, a pointer to the file is still present in the transaction log. Instead of manually modifying files stored in a Delta table, always use the correct commands. |
# MAGIC | External readers | The data stored in Delta Lake is encoded as Parquet files. However, accessing these files using an external reader is not safe. There will be duplicates and uncommitted data and the read may fail when someone runs `VACUUM`.<br> **Tip**: Because the files are encoded in an open format, we always have the option to move the files outside Delta Lake. At that point, we can run `VACUUM RETAIN 0` and delete the transaction log. This leaves the table's files in a consistent state that can be read by the external reader. |

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## ![Delta Logo Tiny](https://files.training.databricks.com/courses/hadoop-migration/delta-lake-tiny-logo.png) Convert to Delta Lake on Databricks
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/convert-parquet-to-delta.png" style="max-height:400px"/></p>
# MAGIC </div>
# MAGIC 
# MAGIC ``CONVERT TO DELTA ([db_name.]table_name|parquet.`path/to/table`)``<br>
# MAGIC `[NO STATISTICS]`<br>
# MAGIC `[PARTITIONED BY (col_name1 col_type1, col_name2 col_type2, ...)]`
# MAGIC 
# MAGIC Convert an existing Parquet table to a Delta table in-place. This command lists all the files in the directory, creates a Delta Lake transaction log that tracks these files, and automatically infers the data schema by reading the footers of all Parquet files. The conversion process collects statistics to improve query performance on the converted Delta table. If we provide a table name, the metastore is also updated to reflect that the table is now a Delta table.
# MAGIC 
# MAGIC ### NO STATISTICS
# MAGIC 
# MAGIC Bypass statistics collection during the conversion process and finish the conversion faster. After the table is converted to Delta Lake, we can use `OPTIMIZE ... ZORDER BY` to reorganize the data layout and generate statistics.
# MAGIC 
# MAGIC ### PARTITIONED BY
# MAGIC 
# MAGIC Partition the created table by the specified columns. Required if the data is partitioned. The conversion process aborts and throws an exception if the directory structure does not conform to the `PARTITIONED BY` specification. If we do not provide the `PARTITIONED BY` clause, the command assumes that the table is not partitioned.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## ![Delta Logo Tiny](https://files.training.databricks.com/courses/hadoop-migration/delta-lake-tiny-logo.png) Optimize Performance with File Management
# MAGIC 
# MAGIC To improve query speed, Delta Lake on Databricks supports the ability to optimize the layout of data stored in cloud storage. Delta Lake on Databricks supports two layout algorithms:
# MAGIC 0. Compaction (bin-packing)
# MAGIC 0. Z-Ordering (multi-dimensional clustering)
# MAGIC 
# MAGIC Let's see how to run optimization commands and how the two layout algorithms work. We'll also discuss stale table snapshots.
# MAGIC 
# MAGIC ### Compaction (bin-packing)
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/optimize-delta.png" style="max-height:400px"/></p>
# MAGIC </div>
# MAGIC 
# MAGIC Delta Lake on Databricks can improve the speed of read queries from a table by coalescing small files into larger ones. Trigger compaction by running the `OPTIMIZE` command:
# MAGIC 
# MAGIC ``OPTIMIZE delta.`/data/events` ``
# MAGIC 
# MAGIC or
# MAGIC 
# MAGIC `OPTIMIZE events`
# MAGIC 
# MAGIC If we have a large quantity of data and only want to optimize a subset of it, we can specify an optional partition predicate using `WHERE`:
# MAGIC 
# MAGIC `OPTIMIZE events`<br>
# MAGIC `WHERE date >= '2017-01-01'`
# MAGIC 
# MAGIC **Note**:
# MAGIC * Bin-packing optimization is idempotent, meaning that if it is run twice on the same dataset, the second instance has no effect.
# MAGIC * Bin-packing aims to produce evenly-balanced data files with respect to their size on disk, but not necessarily number of tuples per file. However, the two measures are most often correlated.
# MAGIC 
# MAGIC Readers of Delta tables use snapshot isolation, which means that they are not interrupted when `OPTIMIZE` removes unnecessary files from the transaction log. `OPTIMIZE` makes no data related changes to the table, so a read before and after an `OPTIMIZE` has the same results. Performing `OPTIMIZE` on a table that is a streaming source does not affect any current or future streams that treat this table as a source. `OPTIMIZE` returns the file statistics (min, max, total, and so on) for the files removed and the files added by the operation. Optimize stats also contains the Z-Ordering statistics, the number of batches, and partitions optimized.
# MAGIC 
# MAGIC ### Data Skipping
# MAGIC 
# MAGIC Data skipping information is collected automatically when we write data into a Delta table. Delta Lake on Databricks takes advantage of this information (minimum and maximum values) at query time to provide faster queries. We do not need to configure data skipping - the feature is activated whenever applicable. However, its effectiveness depends on the layout of our data. For best results, apply Z-Ordering.
# MAGIC 
# MAGIC ### Z-Ordering (multi-dimensional clustering)
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/z-order-black.png" style="max-height:400px"/></p>
# MAGIC </div>
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/incremental-zorder.png" style="max-height:400px"/></p>
# MAGIC </div>
# MAGIC 
# MAGIC Z-Ordering is a technique to co-locate related information in the same set of files. This co-locality is automatically used by Delta Lake on Databricks data-skipping algorithms to dramatically reduce the quantity of data that needs to be read. To Z-Order data, we specify the columns to order on in the `ZORDER BY` clause:
# MAGIC 
# MAGIC `OPTIMIZE events`<br>
# MAGIC `WHERE date >= current_timestamp() - INTERVAL 1 day`<br>
# MAGIC `ZORDER BY (eventType)`
# MAGIC 
# MAGIC If we expect a column to be commonly used in query predicates and if that column has high cardinality (that is, a large number of distinct values), then use `ZORDER BY`.
# MAGIC 
# MAGIC **Note**:
# MAGIC * Z-Ordering is not idempotent but it aims to be an incremental operation. The time it takes for Z-Ordering is not guaranteed to reduce over multiple runs. However, if no new data was added to a partition that was just Z-Ordered, another Z-Ordering of that partition will not have any effect.
# MAGIC * Z-Ordering aims to produce evenly-balanced data files with respect to the number of tuples, but not necessarily data size on disk. The two measures are most often correlated, but there can be situations when that is not the case, leading to skew in optimize task times.
# MAGIC 
# MAGIC ### Improving Performance for Interactive Queries
# MAGIC 
# MAGIC At the beginning of each query Delta tables auto-update to the latest version of the table. This process can be observed in notebooks when the command status reports: **"Updating the Delta table's state"**. However, when running historical analysis on a table, we may not necessarily need up-to-the-last-minute data, especially for tables where streaming data is being ingested frequently. In these cases, queries can be run on stale snapshots of our Delta table. This can lower the latency in getting results from queries that we execute.
# MAGIC 
# MAGIC We can configure how stale our table can be by setting the Spark session configuration `spark.databricks.delta.stalenessLimit` using a time string, for example 1h, 15m, 1d for 1 hour, 15 minutes, and 1 day respectively. This configuration is a session specific configuration, therefore won’t affect how other users are accessing this table from different notebooks, jobs, or BI tools. In addition, this setting doesn’t prevent your table from updating. It just prevents a query from having to wait for the table to update. The update still occurs in the background, and will share resources fairly across the cluster. If the staleness limit is exceeded, then the query will block on the table state update.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Delta Logo Tiny](https://files.training.databricks.com/courses/hadoop-migration/delta-lake-tiny-logo.png) Delta cache
# MAGIC 
# MAGIC Delta cache can provide significant performance benefits through its ability to fully utilize fast modern hardware and a tuned data format.
# MAGIC 
# MAGIC Delta cache accelerates data reads by creating copies of remote files in a cluster nodes' local storage using a fast intermediate data format. The data are cached automatically whenever a file has to be fetched from a remote location. Successive reads of the same data are then performed locally, which results in significantly improved performance. Delta cache works for all Parquet files.
# MAGIC 
# MAGIC Here are the major characteristics of Delta cache:
# MAGIC 
# MAGIC |   | Description |
# MAGIC | - | ----------- |
# MAGIC | Stored data | Delta cache contains local copies of remote data. It can improve the performance of a wide range of queries, but cannot be used to store the results of arbitrary subqueries. |
# MAGIC | Performance | The data stored in the Delta cache can be read and operated-on faster than the data in the Spark cache. This is because the Delta cache uses efficient decompression algorithms and outputs data in the optimal format for further processing using whole-stage code generation. |
# MAGIC | Automatic control | When Delta cache is enabled, data that has to be fetched from a remote source is automatically added to the cache. This process is fully transparent and does not require any action. However, to preload data into the cache beforehand, we can use the `CACHE` command. |
# MAGIC | Disk-based | Delta cache is stored entirely on the local disk, so that memory is not taken away from other operations within Spark. Due to the high read speeds of modern SSDs, the Delta cache can be fully disk-resident without a negative impact on its performance. |
# MAGIC 
# MAGIC ### Delta cache Consistency
# MAGIC 
# MAGIC The Delta cache automatically detects when data files are created or deleted and updates its content accordingly. We can write, modify, and delete table data with no need to explicitly invalidate cached data.
# MAGIC 
# MAGIC As of Databricks Runtime 5.5, the Delta cache automatically detects files that have been modified or overwritten after being cached. Any stale entries are automatically invalidated and evicted from the cache.
# MAGIC 
# MAGIC ### Use Delta Caching
# MAGIC 
# MAGIC To use Delta caching, choose a **Delta Cache Accelerated** worker type. The Delta cache is enabled by default and configured to use at most half of the space available on the local SSDs provided with the worker nodes.
# MAGIC 
# MAGIC ### Cache a Subset of the Data
# MAGIC 
# MAGIC To explicitly select a subset of data to be cached, use the following syntax:
# MAGIC 
# MAGIC `CACHE SELECT column_name[, column_name, ...]`<br>
# MAGIC `FROM [db_name.]table_name`<br>
# MAGIC `[ WHERE boolean_expression ]`
# MAGIC 
# MAGIC We don't need to use this command for the Delta cache to work correctly (the data will be cached automatically when first accessed). But it can be helpful when we require consistent query performance.
# MAGIC 
# MAGIC ### Monitor the Delta cache
# MAGIC 
# MAGIC We can check the current state of the Delta cache on each of the executors in the Storage tab in the Spark UI. When a node reaches 100% disk usage, the cache manager discards the least recently used cache entries to make space for new data.
# MAGIC 
# MAGIC ### Configure the Delta cache
# MAGIC 
# MAGIC #### Configure Disk Usage
# MAGIC 
# MAGIC To configure how the Delta cache uses the worker nodes’ local storage, specify the following Spark configuration settings during cluster creation:
# MAGIC 
# MAGIC | Setting | Description |
# MAGIC | ------- | ----------- |
# MAGIC | `spark.databricks.io.cache.maxDiskUsage` | Disk space per node reserved for cached data in bytes. |
# MAGIC | `spark.databricks.io.cache.maxMetaDataCache` | Disk space per node reserved for cached metadata in bytes. |
# MAGIC | `spark.databricks.io.cache.compression.enabled` | Should the cached data be stored in compressed format. |
# MAGIC 
# MAGIC #### Enable the Delta cache
# MAGIC 
# MAGIC To enable and disable the Delta cache, run:
# MAGIC 
# MAGIC `spark.conf.set("spark.databricks.io.cache.enabled", "[true | false]")`
# MAGIC 
# MAGIC Disabling the cache does not result in dropping the data that is already in the local storage. Instead, it prevents queries from adding new data to the cache and reading data from the cache.
# MAGIC 
# MAGIC ### Summary
# MAGIC 
# MAGIC The following table summarizes Delta cache:
# MAGIC 
# MAGIC | Feature | Delta cache |
# MAGIC | ------- | ----------- |
# MAGIC | Stored as | Local files on a worker node. |
# MAGIC | Applied to | Any Parquet table stored on cloud-based or other file systems. |
# MAGIC | Triggered | Automatically, on the first read (if cache is enabled). |
# MAGIC | Evaluated | Lazily. |
# MAGIC | Force cache | `CACHE` and `SELECT` |
# MAGIC | Availability | Can be enabled or disabled with configuration flags, disabled on certain node types. |
# MAGIC | Evicted | Automatically on any file change, manually when restarting a cluster. |
# MAGIC 
# MAGIC To summarize, Delta cache should be used when:
# MAGIC * The same Parquet files need to be repeatedly read, such as for analytics and data exploration
# MAGIC * There is a need for an automatic solution
# MAGIC * Fast local storage is available

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## ![Delta Logo Tiny](https://files.training.databricks.com/courses/hadoop-migration/delta-lake-tiny-logo.png) Delta Transaction Log
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/delta-log.png" style="max-height:400px"/></p>
# MAGIC </div>
# MAGIC 
# MAGIC _ _delta_log_ contains files that are used by Delta. It is strongly recommended not to alter/delete/add anything to it manually. Only use Delta (SparkSQL or Dataframe) APIs with Delta tables.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Delta Logo Tiny](https://files.training.databricks.com/courses/hadoop-migration/delta-lake-tiny-logo.png) Delta Metadata
# MAGIC 
# MAGIC All state information for Delta is stored in the transaction log, which is located on cloud storage next to the data. Individual clusters can cache the state of a table, but this is updated automatically for each operation. This can be observed in a notebook as status message **"Updating the Delta table's state"**. Every query updates the snapshot of the table before executing, so we should never see stale data.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## ![Delta Logo Tiny](https://files.training.databricks.com/courses/hadoop-migration/delta-lake-tiny-logo.png) Partition Pruning
# MAGIC 
# MAGIC Partition pruning is an optimization technique to limit the number of partitions that are inspected by a query.
# MAGIC 
# MAGIC `MERGE INTO` is an expensive operation when used with Delta tables. If we don't partition the underlying data and use it appropriately, query performance can be severely impacted.
# MAGIC 
# MAGIC **Tip**: If we know which partitions a `MERGE INTO` query needs to inspect, we should specify them in the query so that partition pruning is performed.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/merge-delta.png" style="max-height:400px"/></p>
# MAGIC </div>
# MAGIC 
# MAGIC Here are some examples of `MERGE INTO`:
# MAGIC 
# MAGIC | Example | Code |
# MAGIC | ------- | ---- |
# MAGIC | Standard SQL syntax | `MERGE INTO target t USING source s`<br> `ON t.key = s.key`<br> `WHEN MATCHED THEN UPDATE SET x = y`<br> `WHEN NOT MATCHED THEN INSERT (x) VALUES y` |
# MAGIC | Clause conditions | `MERGE INTO target t USING source s`<br> `ON t.key = s.key`<br> `WHEN MATCHED AND <clause_condition1> THEN UPDATE SET x = y`<br> `WHEN NOT MATCHED AND <clause_condition2> THEN INSERT (x) VALUES y` |
# MAGIC | Multiple matched clauses and delete | `MERGE INTO target d USING source s`<br> `ON t.key = s.key`<br> `WHEN MATCHED AND <clause_condition1> THEN UPDATE SET x = y`<br> `WHEN MATCHED THEN DELETE`<br> `WHEN NOT MATCHED AND <clause_condition2> THEN INSERT (x) VALUES y` |
# MAGIC | Star to auto-expand to target table columns | `MERGE INTO target t USING source s`<br> `ON t.key = s.key`<br> `WHEN MATCHED THEN UPDATE SET *`<br> `WHEN NOT MATCHED THEN INSERT *` |
# MAGIC | Scala | `deltaTable.alias("t").merge(sourceDF.alias("s"), "t.key = s.key")`<br> `.whenMatched().updateAll().whenNotMatched().insertAll().execute()` |
# MAGIC | Python | `deltaTable.alias("t").merge(sourceDF.alias("s"), "t.key = s.key")`<br> `.whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()` |

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Delta Logo Tiny](https://files.training.databricks.com/courses/hadoop-migration/delta-lake-tiny-logo.png) Roadmap

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Delta Auto Data Loader (TBD)
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/auto_data_loader.png" style="max-height:600px"/></p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Delta Pipelines (Q2 2020)
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/delta_pipelines.png" style="max-height:600px"/></p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Expectations (Q2 2020)
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/data_expectations.png" style="max-height:600px"/></p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Other incoming features
# MAGIC 
# MAGIC * `MERGE INTO` schema evolution (Q2 2020)
# MAGIC   * When source data is missing values, fill with null values for insertions, and leave unchanged for updates
# MAGIC   * When source data contains new columns, operations should automatically evolve the Delta table's schema to include the new columns, subject to existing restrictions on Delta schema evolution
# MAGIC   * However, queries which explicitly reference columns that do not exist in the target remain disallowed
# MAGIC 
# MAGIC * `MERGE INTO` performance improvements (2020)
# MAGIC   * Use probabilistic data structures like bloom filters to improve `MERGE` performance
# MAGIC 
# MAGIC * Easier access to semi-structured data (2020)
# MAGIC   * Dot notation syntax for accessing nested structures
# MAGIC   * Automatic parsing and storing of nested fields in Delta to avoid JSON deserialization costs at query time
# MAGIC    * Statistics collection on columns as they are being parsed to allow for optimizations like data skipping

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC Databricks Delta extends Apache Spark to simplify data reliability and boost Spark's performance.
# MAGIC 
# MAGIC Building robust, high performance data pipelines can be difficult due to:
# MAGIC * Lack of indexing and statistics
# MAGIC * Data inconsistencies introduced by schema changes
# MAGIC * Pipeline failures
# MAGIC * Having to trade off between batch and stream processing
# MAGIC 
# MAGIC With Databricks Delta, data engineers can build reliable and fast data pipelines. Databricks Delta provides many benefits including:
# MAGIC * Faster query execution with indexing, statistics, and auto-caching support
# MAGIC * Data reliability with rich schema validation and transactional guarantees
# MAGIC * Simplified data pipeline with flexible `UPSERT` support and unified Structured Streaming + batch processing on a single data source

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC [Databricks Documentation - Delta Lake](https://docs.databricks.com/delta/index.html)
# MAGIC 
# MAGIC [Databricks Documentation - Delta Lake - Optimizations](https://docs.databricks.com/delta/optimizations/index.html)
# MAGIC 
# MAGIC [Databricks Knowledgebase - Delta Lake](https://kb.databricks.com/delta/index.html)
# MAGIC 
# MAGIC [Faster SQL Queries on Delta Lake with Dynamic File Pruning](https://databricks.com/blog/2020/04/30/faster-sql-queries-on-delta-lake-with-dynamic-file-pruning.html)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>