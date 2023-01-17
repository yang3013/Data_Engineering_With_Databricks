# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Working with an IDE
# MAGIC 
# MAGIC ### Learning Objectives:
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Understand the benefits of Databricks Connect
# MAGIC * Install and Configure Databricks Connect
# MAGIC * Develop Spark jobs that run on Databricks Clusters from an IDE

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## What is Databricks Connect?
# MAGIC 
# MAGIC Databricks Connect is a client library for Apache Spark. 
# MAGIC 
# MAGIC It allows you to write jobs in Spark outside of the Databricks environment and executes them remotely on a Databricks cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Benefits
# MAGIC 
# MAGIC ### With Databricks Connect you can:
# MAGIC * Run large scale Spark jobs without Spark Submission scripts or IDE Plugins from any Python, Java, Scala, or R application
# MAGIC * Debug using your IDE
# MAGIC * Iterate quickly when developing libraries
# MAGIC   * Each client session is isolated from each other, so you do not need to restart the cluster after changing library dependencies
# MAGIC * Shut down idle clusters without losing work
# MAGIC   * Because the client application is decoupled from the cluster, it is unaffected by cluster restarts or upgrades, which would normally cause you to lose all the variables, RDDs, and Dataframe objects defined in a notebook

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Cluster Set-Up
# MAGIC 
# MAGIC In order to execute code on a remote cluster, we must first set-up a cluster.
# MAGIC 
# MAGIC First, click the clusters icon on the left side menu.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/miQQwAK.png" width=80/></p>
# MAGIC </div>
# MAGIC 
# MAGIC Then, hit the blue **+ Create Cluster** button.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/t5UrF3z.png" width=70%/></p>
# MAGIC </div>
# MAGIC 
# MAGIC Ensure that you select a Databricks Runtime of 5.5, 6.1 or higher.
# MAGIC 
# MAGIC Finally, scroll-down to **Advanced Options** and then click the **Spark** tab.
# MAGIC 
# MAGIC Copy and paste `spark.databricks.service.server.enabled true` into the **Spark Config** box to set the Spark Configuration for Databricks Connect.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/3.png" width=70%/></p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Client Set-Up
# MAGIC 
# MAGIC Now that we have a working cluster, let's create our client on our local machine to connect to it. 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Client Requirements
# MAGIC 
# MAGIC In order to work properly, ensure that the following steps are completed before continuing:
# MAGIC 0. You have Java 8 installed (not Java 11)
# MAGIC 0. You have the correct version of Python installed to match your cluster's runtime
# MAGIC 
# MAGIC Here is a quick cheat sheet for Databricks runtime Python versions:
# MAGIC * Databricks 5.x has Python 3.5
# MAGIC * Databricks 5.x ML has Python 3.6
# MAGIC * Databricks 6.x and 6.x ML have Python 3.7
# MAGIC   
# MAGIC **Note**: It is recommended to use an Anaconda Virtual Environment, which is what we will use in this demo. 
# MAGIC 
# MAGIC To create the environment, use the command: `conda create --name dbconnect python=3.7`.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/4.png" width=70%/></p>
# MAGIC </div>
# MAGIC 
# MAGIC Replace `python=3.7` with whatever version your cluster runs.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Install Client
# MAGIC 
# MAGIC Activate the conda environment with `conda activate dbconnect`.
# MAGIC 
# MAGIC Next, run `pip uninstall pyspark`.
# MAGIC 
# MAGIC Finally, run `pip install -U databricks-connect==6.*.*`, filling in your Databricks runtime version.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/5.png" width=70%/></p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Configure Connection Properties
# MAGIC 
# MAGIC Now let's configure our client so we can reach the cluster we set-up.
# MAGIC 
# MAGIC We are going to set 4 properties using the Databricks Command Line Interface (CLI).
# MAGIC 
# MAGIC In your conda environment, run `databricks-connect configure`.
# MAGIC 
# MAGIC You will be prompted to fill in the following values:
# MAGIC 
# MAGIC * URL 
# MAGIC   * A URL of the form *https://adb-<(unique-id)>.<(no)>.azuredatabricks.net/* 
# MAGIC   * To get this URL, click the Databricks logo at the top of the left menu, then copy the URL
# MAGIC   * Example:
# MAGIC     * https://adb-6380537854481617.17.azuredatabricks.net/
# MAGIC <!---
# MAGIC <img src="https://files.training.databricks.com/courses/hadoop-migration/6.png" width=400/>
# MAGIC -->
# MAGIC * User Token
# MAGIC   * These are unique tokens used for identification. To get your user token, click the top right account icon
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/7.png" width=20%/></p>
# MAGIC </div>
# MAGIC   * Next, click **User Settings** on the drop-down menu, and then move to the **Access Tokens** tab
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/8.png" width=40%/></p>
# MAGIC </div>
# MAGIC   * Click **Generate New Token** and copy the token
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/images/awscore/s3bucket-secrets/copy-token.png" width=40%/></p>
# MAGIC </div>
# MAGIC * ClusterId
# MAGIC   * Click on the **Clusters** menu, and then select the cluster you would like to attach to remotely
# MAGIC   * Go to the **Advanced Options** drop-down menu, then move to the **Tags** tab
# MAGIC   * Copy the **ClusterId** from the table
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/10.png" width=50%/></p>
# MAGIC </div>
# MAGIC 
# MAGIC * Org ID
# MAGIC   * Azure-only, see ?o=orgId in URL. Otherwise, use the default value 0
# MAGIC  
# MAGIC * Port
# MAGIC   * Set to the default 15001
# MAGIC   
# MAGIC Run `databricks-connect test` to test the connection.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## IDE Set-Up
# MAGIC 
# MAGIC Now let's take a look at how to set-up our IDE to work with Databricks Connect. 
# MAGIC 
# MAGIC For this demo, we will use VS Code, because it is free and widely used. Instructions for other IDEs can be found [here](https://docs.databricks.com/dev-tools/databricks-connect.html#language-python).

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC First, download VS Code. Also download the Python extension, which can be found [here](https://marketplace.visualstudio.com/items?itemName=ms-python.python).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Configure VS Code
# MAGIC 
# MAGIC * Open the the Command Palette (Command+Shift+P on macOS and Ctrl+Shift+P on Windows/Linux)
# MAGIC   * Search for `Python: Select Interpreter` and choose your virtual environment
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/11.png" width=80%/></p>
# MAGIC </div>
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/12.png" width=80%/></p>
# MAGIC </div>
# MAGIC   
# MAGIC * Open the Command Palette again.
# MAGIC   * Search for `Preferences: Open Settings (JSON)` to access `settings.json`
# MAGIC * We are going to add two settings: `python.venvPath` and `python.linting.enabled`
# MAGIC   * For `python.venvPath`, run `databricks-connect get-jar-dir`, copy the output and add it to the json pair with `python.venvPath` and add a comma to maintain the JSON format
# MAGIC   * Next, add `python.linting.enabled: false`, again do not forget the comma
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/13.png" width=80%/></p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Simple Example
# MAGIC 
# MAGIC Let's try a simple example now that everything is set-up! 
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/14.png" width=80%/></p>
# MAGIC </div>
# MAGIC 
# MAGIC We can also go back into the Spark UI in our cluster to view the Job we just completed. 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Dataframe Example
# MAGIC 
# MAGIC We can also work with data stored in the Databricks environment. 
# MAGIC 
# MAGIC Here, we have stored World Population Data in a table in the Databricks environment.
# MAGIC 
# MAGIC We can see it in the IDE too!
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/Screen%20Shot%202020-05-02%20at%202.12.22%20AM.png" width=80%/></p>
# MAGIC </div>
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/Screen%20Shot%202020-05-02%20at%202.12.37%20AM.png" width=20%/></p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Connect Limitations
# MAGIC 
# MAGIC It is important to note that some features available in the Databricks environment are not supported with Databricks Connect. These features include:
# MAGIC * Structured Streaming
# MAGIC * Running arbitrary code that is not part of a Spark Job on the remote cluster
# MAGIC * Scala, Python, and R APIs for Delta tables
# MAGIC * Most utilities in Databricks Utilities; however, `dbutils.fs` and `dbutils.secrets` are supported
# MAGIC * Apache Zeppelin 0.7.x and lower
# MAGIC 
# MAGIC It is also important to note that, currently, DBR versions 7.0 and 7.1 are not compatible with Databricks Connect.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Summary
# MAGIC 
# MAGIC We have accomplished a lot here. 
# MAGIC 
# MAGIC We have:
# MAGIC * Created a virtual anaconda environment with the correct Python version for our cluster
# MAGIC * Downloaded and configured Databricks Connect from our local machine in that environment
# MAGIC * Linked our IDE to that virtual machine
# MAGIC * Ran Spark Code with our cluster connected remotely through that IDE

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Troubleshooting
# MAGIC 
# MAGIC If you run into issues, try some of the troubleshooting tips at the bottom of [this article](https://docs.databricks.com/dev-tools/databricks-connect.html).

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC [Databricks Connect](https://docs.databricks.com/dev-tools/databricks-connect.html)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>