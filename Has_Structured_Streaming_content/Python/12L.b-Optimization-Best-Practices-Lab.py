# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Optimization Best Practices Lab 2
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Understand a skew join
# MAGIC * Understand the solution to skew joins in Open Source Apache Spark
# MAGIC * Understand the solution to skew joins on Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Introduction
# MAGIC 
# MAGIC ### What is a Skew Join?
# MAGIC 
# MAGIC **Data skew** in a Dataframe refers to a situation where some values occur with much greater frequency within a column than other values. An example would be a Dataframe containing retail transactions where 30% of the records are from California, 20% of the records are from New York, and the remaining records are relatively evenly distributed among the remaining states. A value that occurs significantly more than other values is called a **skew value**. In this example, California and New York would be skew values for the state column.
# MAGIC 
# MAGIC A **skew join** occurs when there is a Dataframe with data skew in the *joining column*. When a standard sort-merge join is performed by Spark, all the records from both sides of the join associated with a matching join key value must be brought to one single executor. With the example above, doing a join on the state column would result in all of the records from California being shuffled to one partition and all of the records from New York being shuffled to one partition. With such an uneven distribution of partition sizes, further processing of the resulting joined Dataframe would result in one or more tasks running much longer than the others **or possibly even an out-of-memory or other error**.
# MAGIC 
# MAGIC In this notebook, we will create a data set with data skew, see how this affects join performance, and explore techniques to improve the performance of the join.

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
# MAGIC Define some paths and clean-up.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val item_path  = userhome + "/item"
# MAGIC val sales_path = userhome + "/sales"
# MAGIC 
# MAGIC val item_path_removed  = dbutils.fs.rm(item_path, true)
# MAGIC val sales_path_removed = dbutils.fs.rm(sales_path, true)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC First import the packages used in this notebook.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Next, run the next cell to create two data sets of random values in Parquet format:
# MAGIC 
# MAGIC + Items
# MAGIC   - item_id:`long` - item id
# MAGIC   - i_price:`integer` - item price
# MAGIC + Sales
# MAGIC   - item_id:`integer` - item id 
# MAGIC   - s_quantity:`integer` - quantity of items
# MAGIC   - s_date:`integer` - date of sale
# MAGIC   
# MAGIC The sales data set has data skew in the `item_id` column, with a skew value of `100` accounting for approximately 80% of the records. Whichever task runs over the records that match this skew value will run longer than other tasks. 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Skew Data Setup
# MAGIC 
# MAGIC The cell below will create the data sets we will use for this notebook. The code is in Scala only. 

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val numSales = 100000000
# MAGIC val numItems = 3000000
# MAGIC val seed = 42
# MAGIC val item_path = userhome + "/item"
# MAGIC val sales_path = userhome + "/sales"
# MAGIC 
# MAGIC // Item with id 100 is in 80% of all sales
# MAGIC 
# MAGIC spark.range(numSales).selectExpr(
# MAGIC   s"case when rand($seed) < 0.8 then 100 else cast(rand($seed) * $numSales as int) end as item_id",
# MAGIC   s"cast(rand($seed) * 100 as int) as s_quantity",
# MAGIC   s"cast(now() as int) - cast(rand($seed) * 360 as int) * 3600 * 24 as s_date"
# MAGIC ).write.mode("overwrite").parquet(sales_path)
# MAGIC 
# MAGIC spark.range(numItems).selectExpr(
# MAGIC   s"id as item_id",
# MAGIC   s"cast(rand($seed) * 1000 as int) as i_price"
# MAGIC ).write.mode("overwrite").parquet(item_path)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's read each data set and examine their schemas.

# COMMAND ----------

item_df = spark.read.parquet(userhome + "/item")
item_df.printSchema()

sales_df = spark.read.parquet(userhome + "/sales")
sales_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We will reset the value for `spark.sql.shuffle.partitions` from a default of 200 to 80. Why 80? This is not a magic number nor was it calculated using a defined formula. The original creator of this example used trial and error to come up with an appropriate number of partitions for shuffle operations in this use case.

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 80)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Below, we perform a normal join. We will find one task that takes a ridiculously long time to complete compared to the other tasks. When running the cell below, feel free to cancel after confirming a long run time. For this size of data, a few minutes is an unreasonable runtime. You can look at the Spark UI and note the run times of the other tasks for comparison sake. 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Normal Join 
# MAGIC 
# MAGIC The code in the next cell - when uncommented and run - will experience one straggling task that will eventually cause a failure. We likely won't wait for completion or an error. Feel free to cancel the job. 
# MAGIC 
# MAGIC Here is an example error message displayed in the notebook. 
# MAGIC <pre style="font-size:11pt; font-family:Courier New">
# MAGIC org.apache.spark.SparkException: Job aborted due to stage failure: Task 50 in stage 33.0 failed 4 times, most recent failure: Lost task 50.3 in stage 33.0 (TID 539, 10.139.64.6, executor 2): ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: Remote RPC client disassociated. Likely due to containers exceeding thresholds, or network issues. Check driver logs for WARN messages.
# MAGIC </pre>
# MAGIC 
# MAGIC Or, you might get an `OutOfMemoryError`
# MAGIC <pre style="font-size:11pt; font-family:Courier New">
# MAGIC org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 41.0 failed 4 times, most recent failure: Lost task 0.3 in stage 41.0 (TID 656, 10.139.64.6, executor 4): java.lang.OutOfMemoryError: Java heap space
# MAGIC </pre>

# COMMAND ----------

normal_join = item_df.join(sales_df, "item_id")
# normal_join.count() # Uncomment this statement and run this cell to see the failed job, otherwise, continue

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Why the Latency and Why the Error?
# MAGIC 
# MAGIC The reason for one task taking so long and then failing is because all the records for `item_id` 100 will be processed by 1 task. And 80% of all the records in the `sales_df` Dataframe have an `item_id` value of 100.
# MAGIC 
# MAGIC Let's have a look at the distribution of `item_id` in the `sales_df` Dataframe.

# COMMAND ----------

total_count = sales_df.count()

print(f"Total record count: {total_count:,}")

counts_df = sales_df.groupBy("item_id").count()

dist_df = (counts_df.select(col("item_id"),
                            col("count"),
                            lit(col("count")/total_count).alias("percent_of_total"))
                    .orderBy(col("count").desc()))

high = dist_df.first()["count"]

print(f"Highest count: {high:,}")

display(dist_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Skew
# MAGIC 
# MAGIC * We see that 80% of `item_id` has the value 100
# MAGIC * This will lead to considerable skew; under the hood Spark will put all these rows in the same partition when joining (due to hash partitioning)
# MAGIC * Any unfortunate executor that has to deal with this partition will at some point fail

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Salted Join
# MAGIC 
# MAGIC One (Open Source) solution against skew is by making use of Salted Joins. Salting is a technique where we will add random values to the join key of one of the tables. The idea is if the join condition is satisfied by key1 == key1, it should also get satisfied by key1&#95;&lt;salt&gt; = key1&#95;&lt;salt&gt;
# MAGIC 
# MAGIC Salted Join works as follows:
# MAGIC * Define a range of values (in our example, up to 32), we will call it `SALT_VAL`; the Dataframe will be named `salted_df` and the column will be named `salt_val`
# MAGIC * Cross join our non-skewed Dataframe with the `salted_df` Dataframe; ensure that we repartition
# MAGIC * To our skewed Dataframe, add a uniform-distributed column with values, we also denote it as `salt_val`
# MAGIC * We then concatenate our join columns with `salt_val`, and join the Dataframes on these new columns

# COMMAND ----------

# Define range of random values we want
SALT_VAL = 32

# Generate df that contains our possible values (0 to SALT_VAL)
salted_df = spark.range(SALT_VAL).select(col("id").alias("salted_val"))

# 1. Cross join our non skew DF with the DF containing all possible random values (salted_df)
# 2: Create new column to join on by concatenating item_id and salted_val
salted_left = (item_df # .repartition(24)
               .crossJoin(salted_df)
               .withColumn("join_val",concat(col("item_id"), lit("_"), col("salted_val"))))
print("Schema of salted_left DataFrame")
salted_left.printSchema()

# 1. Create column with uniform distribution between 0 and SALT_VAL in our skewed dataframe
# 2. Create new variable to join on by concatenating s_item_id and salted_val
salted_right = (sales_df
                .withColumn("salted_val", (lit(SALT_VAL) * rand()).cast("int"))
                .withColumn("join_val", concat(col("item_id"), lit("_"), col("salted_val"))))
print("Schema of salted_right DataFrame")
salted_right.printSchema()

# Perform the join
salted_join = salted_left.join(salted_right, "join_val")
print("Schema of salted_join DataFrame")
salted_join.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Shuffle Partitions
# MAGIC 
# MAGIC How do we tune our job for a Salted join? Some rough guidelines:
# MAGIC * Decide how many total shuffle partitions we want (40 seems ok for this example; this was determined by trial and error)
# MAGIC * Estimate % of our data affected by the skewed value (= 80%)
# MAGIC * The skewed value should then cover 80% of 40 partitions = 32
# MAGIC * If we want perfectly equal partitions, our SALT_VAL should be set to 32
# MAGIC * This is a rough guideline: the higher the SALT_VAL, the more copies we need of our non-skew Dataframe

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 40)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now we invoke an action (job) on our joined Dataframe that includes salting. 

# COMMAND ----------

salted_join.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The performance of the above job will likely not be super fast, but it will not fail as we saw previously. 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Why Do Salted Joins Work?
# MAGIC 
# MAGIC * Essentially, we split up the rows with the skewed value (item_id = 100) by concatenating it with randomly generated numbers
# MAGIC * In our example, by having 32 different values for SALT_VAL, we split up the skewed value into 32 partitions

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Salted Join Considerations
# MAGIC 
# MAGIC * Trade-off between blowing up non-skew Dataframe and number of shuffle partitions
# MAGIC * Next to that, just generally error prone
# MAGIC * Only do it when there is a LOT of skew, otherwise the overhead of the cross join makes it sub-optimal

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Databricks Skew Hint
# MAGIC 
# MAGIC When running a Spark application on Databricks, we can use a hint that is not available in Open Source Apache Spark. That hint is `skew`. While there is not a function named `skew()` like there is with `broadcast()`, the function `hint()` can take hint arguments. We will be passing the name of the skew column, "item_id", as well as the skew value, "100".

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Before we invoke any action (job), we set our shuffle partitions to our default parallelism. With the size of data we have this should be appropriate. With larger datasets, we might want to choose a factor of default parallelism of 2x, 3x, 4x, etc.

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)
print("Shuffle partitions set to: ", spark.conf.get("spark.sql.shuffle.partitions"))

# COMMAND ----------

joined_df = sales_df.hint("skew", "item_id", "100").join(item_df, "item_id")
joined_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Summary
# MAGIC 
# MAGIC * Syntax: `skewed_df.hint("skew", "skew_column" `[optional]`, "skew_value" `[optional]`)`
# MAGIC * Using skew hints allows Spark to use an efficient form of salting
# MAGIC * Only use skew hints when there is significant skew, as the overhead of the salting under the hood might not be worth it

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC [Databricks Documentation - Skew Join Optimization](https://docs.databricks.com/delta/join-performance/skew-join.html#skew-join-optimization)
# MAGIC 
# MAGIC [Databricks Documentation - SQL Hints](https://docs.databricks.com/spark/latest/spark-sql/language-manual/select.html#hints)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Appendix A: Why is Skew (generally) not as big a problem for groupBy + agg?
# MAGIC 
# MAGIC Partial aggregations are done before the shuffle, which usually helps in dealing with skew automatically.
# MAGIC 
# MAGIC View the explain plan below.<br>
# MAGIC <pre>
# MAGIC == Physical Plan ==
# MAGIC Sort [count#2433L DESC NULLS LAST], true, 0
# MAGIC +- Exchange rangepartitioning(count#2433L DESC NULLS LAST, 8), [id=#8533]
# MAGIC    +- *(2) HashAggregate(keys=[item_id#2285], functions=[finalmerge_count(merge count#2439L) AS count(item_id#2285)#2431L])
# MAGIC       +- Exchange hashpartitioning(item_id#2285, 8), [id=#8529]
# MAGIC          +- *(1) HashAggregate(keys=[item_id#2285], functions=[partial_count(item_id#2285) AS count#2439L])
# MAGIC             +- *(1) FileScan parquet [item_id#2285] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex[dbfs:/user/evan.troyka@databricks.com/sales], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<item_id:int>
# MAGIC </pre>
# MAGIC 
# MAGIC You will find an item reading<br><br>
# MAGIC   `HashAggregate(keys=[item_id#2285], functions=[partial_count(item_id#2285) AS count#2439L])`<br/><br/>
# MAGIC The portion reading `partial_count(item_id#2285)` indicates that sub-totals will be computed and then brought together for a grand total. Aggregates are less likely to cause a skew problem because of partial processing. A join is very different. The fields from both sides of a join need to be brought together into a new record for the final joined Dataframe, and then processed in some manner (counting, displaying, analyzing, etc).

# COMMAND ----------

(sales_df.groupBy("item_id").agg(
  count("item_id").alias("count"))
 .orderBy(col("count").desc())
 .select("item_id", "count").explain())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Appendix B: Broadcast the Skew Records
# MAGIC 
# MAGIC Another option if you know the skew value is to use a broadcast hint. With this approach we separate the entire data set into data sets that do not contain skewed values and datasets that contain the rows that indeed have skewed values. The rows with skewed values could then be broadcast rather than salted. For the example below, the non-skewed rows will be joined as normal. The skewed data will be broadcast. And then the two results will be unioned.

# COMMAND ----------

SKEW_VAL = 100

# Split DF: one DF for the skew value, one DF for the non-skew values
items_remaining = item_df.filter(col("item_id") != SKEW_VAL)
items_100 = item_df.filter(col("item_id") == SKEW_VAL)

# We do the same for the sale records that we just did with the item records
sales_remaining = sales_df.filter(col("item_id") != SKEW_VAL)
sales_100 = sales_df.filter(col("item_id") == SKEW_VAL)

# The non-skewed data is joined normally
joined_remaining = items_remaining.join(sales_remaining,"item_id")

# The skewed data will be joined separately
# Because there's only 1 item left in items_100 we can just broadcast it
joined_100 = sales_100.join(broadcast(items_100),"item_id")

# Union DFs after completing the joins.
union_of_joins = joined_remaining.union(joined_100)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Invoking an action like `count()` on the unioned data should not cause a failure.

# COMMAND ----------

union_of_joins.count()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>