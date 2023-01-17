# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # This is a quick set of notebooks to demonstrate setting and getting of values in a multi-task job
# MAGIC 
# MAGIC 
# MAGIC This might be useful if you want an event or value generated in one task to influence the behavior of a task that happens later in the pipeline.
# MAGIC 
# MAGIC In this example the rough outline of some code that could be used to stop the execution of a notebook if the value passed to it is outside of a specified range.
# MAGIC 
# MAGIC 
# MAGIC # The Exercise
# MAGIC 
# MAGIC Review each notebook, and then run them as task1 and task2 of a job. 
# MAGIC 
# MAGIC Note that the naming of each task has to be ```task1``` and ```task2``` as the taskname is used to retrieve the values