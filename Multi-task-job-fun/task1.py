# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Task1
# MAGIC 
# MAGIC This is a simple task designed to show two things. 
# MAGIC 
# MAGIC 1. It has a simple select for the purposes of showing something if the user looked at task progress in the ui
# MAGIC 2. It sets a key value pair in ```dbutils.jobs.taskvalues```

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Notes, this is meant to be run as ```task1``` in a multi task job
# MAGIC 
# MAGIC The retrieveing of the value in ```task2``` depends on this
# MAGIC 
# MAGIC 
# MAGIC I will include the json configuration of the multi task job and that can be used as a starting point to compare
# MAGIC 
# MAGIC The key thing for this demo is that this notebook is run as "task1", and that it runs before the notebook "task2"
# MAGIC <pre>
# MAGIC 
# MAGIC {
# MAGIC     "job_id": 477593862584363,
# MAGIC     "creator_user_name": "tom.hanlon@databricks.com",
# MAGIC     "run_as_user_name": "tom.hanlon@databricks.com",
# MAGIC     "run_as_owner": true,
# MAGIC     "settings": {
# MAGIC         "name": "task_fun",
# MAGIC         "email_notifications": {
# MAGIC             "no_alert_for_skipped_runs": false
# MAGIC         },
# MAGIC         "webhook_notifications": {},
# MAGIC         "timeout_seconds": 0,
# MAGIC         "max_concurrent_runs": 1,
# MAGIC         "tasks": [
# MAGIC             {
# MAGIC                 "task_key": "task1",
# MAGIC                 "notebook_task": {
# MAGIC                     "notebook_path": "/Users/tom.hanlon@databricks.com/Multi-task-job-fun/task1",
# MAGIC                     "source": "WORKSPACE"
# MAGIC                 },
# MAGIC                 "existing_cluster_id": "0831-172629-mga92f5n",
# MAGIC                 "timeout_seconds": 0,
# MAGIC                 "email_notifications": {}
# MAGIC             },
# MAGIC             {
# MAGIC                 "task_key": "task2",
# MAGIC                 "depends_on": [
# MAGIC                     {
# MAGIC                         "task_key": "task1"
# MAGIC                     }
# MAGIC                 ],
# MAGIC                 "notebook_task": {
# MAGIC                     "notebook_path": "/Users/tom.hanlon@databricks.com/Multi-task-job-fun/task2",
# MAGIC                     "source": "WORKSPACE"
# MAGIC                 },
# MAGIC                 "existing_cluster_id": "0831-172629-mga92f5n",
# MAGIC                 "timeout_seconds": 0,
# MAGIC                 "email_notifications": {}
# MAGIC             }
# MAGIC         ],
# MAGIC         "format": "MULTI_TASK"
# MAGIC     },
# MAGIC     "created_time": 1664992038263
# MAGIC }
# MAGIC 
# MAGIC 
# MAGIC </pre>

# COMMAND ----------

display(spark.sql("select 1 id"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # SET a value for key
# MAGIC 
# MAGIC Random integer between 0 and 10 here

# COMMAND ----------

import random

value = (random.randint(0,10))
print(value)

# COMMAND ----------

dbutils.jobs.taskValues.set("key", value)

# Set value of  “key” to "value"
# to retrieve
#dbutils.jobs.taskValues.get(task1, key)


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Optional test code
# MAGIC 
# MAGIC Run this code to trigger task2 to exit at the top of the notebook

# COMMAND ----------

# value = 10000
# dbutils.jobs.taskValues.set("key", value)





# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Retrieving values
# MAGIC 
# MAGIC To retrieve a value us
# MAGIC 
# MAGIC ```dbutils.jobs.taskValues.get(<task name>, <key name>, debugValue="<debugValue")```
# MAGIC 
# MAGIC debugValue is useful for testing purposes. 
# MAGIC 
# MAGIC When dbutils.jobs.taskValues.get is run outside of a multi-task (workflow/job) it will fail, or return null or something less than useful, by providing a debugValue you can run the notebook interactively to test before you run in a workflow
# MAGIC 
# MAGIC 
# MAGIC If you call taskValues.get outside of a workflow you will see this error
# MAGIC 
# MAGIC ```Must pass debugValue when calling get outside of a job context. debugValue cannot be None.```

# COMMAND ----------

dbutils.jobs.taskValues.get("task1", "key", debugValue="hey")