# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Migrate Oozie Workflows to Azure Data Factory (ADF)
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Use Azure Data Factory to schedule data workflows
# MAGIC * Convert Oozie workflows to Azure Data Factory
# MAGIC * Convert workflows that use spark-submit to use the Databricks Jobs API

# COMMAND ----------

# MAGIC %md
# MAGIC ##Orchestration Tools
# MAGIC 
# MAGIC A common need when migrating from a Hadoop infrastructure to an on-cloud solution is to migrate existing data workflows. The most common tool for workflow orchestration in Hadoop is Oozie and in this lesson we will consider alternate orchestration options and will demonstrate using Azure Data Factory as a workflow management tool for managing your data in Azure Databricks. 
# MAGIC 
# MAGIC The purpose of Oozie is to string together multiple steps of a data workflow and have that workflow triggered by an event, for example a certain time of day or new data landing in a data source. The good news is that this is very easy to do with Databricks and can be enhanced using Azure Data Factory as well. 
# MAGIC 
# MAGIC Azure Databricks has a job scheduling tool built in. It's possible to schedule notebooks to run at a specific time interval and this combined with the ability for notebooks to run other notebooks, allows you to replicate your data workflow directly inside of Azure Databricks using notebooks and the job scheduler. 
# MAGIC 
# MAGIC While it's possible to schedule your notebooks directly from Databricks, the workflow may become hard to follow and debug as it may become more complicated and interact with more tools in the Azure ecosystem. A technology like Azure Data Factory is perfect for orchestrating more complicated data pipelines.
# MAGIC 
# MAGIC Let's start by comparing Oozie and Azure Data Factory.

# COMMAND ----------

# MAGIC %md
# MAGIC ##Apache Oozie
# MAGIC 
# MAGIC * Apache Oozie is a workflow management system to manage Hadoop jobs
# MAGIC * Oozie workflow jobs are Directed Acyclic Graphs (DAGs) of actions
# MAGIC * It is deeply integrated with the rest of the Hadoop stack supporting a number of Hadoop jobs out-of-the-box 
# MAGIC * Workflow is expressed as XML and consists of two types of nodes: control and action
# MAGIC 
# MAGIC ##Azure Data Factory
# MAGIC * Azure Data Factory is a managed cloud service used to build data pipelines
# MAGIC * Pipelines in Azure Data Factory are built using a UI or JSON
# MAGIC * Azure Data Factory can connect to many Azure services, such as Azure Databricks
# MAGIC * Azure Data Factory components are:
# MAGIC   * Pipelines - A logical group of activities to perform a data task
# MAGIC   * Activity - A processing step in the pipeline
# MAGIC   * Linked Services - Enables configuration to other Azure services
# MAGIC   * Triggers - Events that kick off a pipeline
# MAGIC   
# MAGIC We can use Azure Data Factory in place of an Oozie workflow to schedule jobs and notebooks needed for our data workflows. Let's do this with an example.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##Review existing Workflows
# MAGIC 
# MAGIC An important step in migrating existing workflows is to consider if you can eliminate unnecessary steps. For example, it is common for an on-prem Hadoop workflow to have an additional step to compact files in HDFS, to avoid the small file problem. When Databricks Delta auto-optimize feature is used, it is unnecessary to have such an additional compaction step.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Convert Oozie Workflow to Azure Data Factory
# MAGIC 
# MAGIC The team has an existing workflow build in Oozie and our job is to convert it to Azure Data Factory. The current workflow is triggered manually with an `oozie` command, it downloads a Hive data dump, saves the data as a new table, and sends either a success or fail email depending on whether the workflow completed or not.
# MAGIC 
# MAGIC The following diagram describes this workflow in Oozie and Hive and shows how we will map it to Azure Data Factory and Azure Databricks:
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/courses/hadoop-migration/oozie-adf.png" width=40%/>
# MAGIC 
# MAGIC Our goal is to trigger the workflow with Azure Data Factory, extract the Hive data dump, save it as a Delta table using a Databricks notebook, and finally send an email for fail or success in ADF.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC For the existing workflow, our team sent us a [directory](https://files.training.databricks.com/courses/hadoop-migration/oozie.zip
# MAGIC ) that contains the files for the current Oozie workflow. The directory contains:
# MAGIC 
# MAGIC * README.md
# MAGIC * workflow.xml
# MAGIC * job.properties
# MAGIC * scripts/
# MAGIC   * importscript.sh
# MAGIC   * oozietst.hql
# MAGIC 
# MAGIC These will map to our new architecture as follows:
# MAGIC 
# MAGIC <table>
# MAGIC   <tbody>
# MAGIC     <tr>
# MAGIC       <th>Oozie</th>
# MAGIC       <th>Azure Data Factory</th>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>workflow.xml</td>
# MAGIC       <td>ADF User Interface</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>job.properties</td>
# MAGIC       <td>Databricks configuration</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>importscript.sh</td>
# MAGIC       <td>Databricks notebook</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>oozietst.hql</td>
# MAGIC       <td>Databricks notebook</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC <table>
# MAGIC 
# MAGIC   
# MAGIC Let's take a look at some of these files.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC **workflow.xml**:
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/courses/hadoop-migration/oozie-workflow.png"/>
# MAGIC 
# MAGIC The "Extract Hive Data" action calls the `importscript.sh` script. Let's take a look at that script:
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/courses/hadoop-migration/importscript.png"/>
# MAGIC 
# MAGIC This script downloads a Hive data dump, extracts the contents, and moves the files to a new location. This is exactly the same as what we saw in the Import Hive Tables Overview lesson earlier in the course.
# MAGIC 
# MAGIC The next action, "Create Table", calls `oozietst.sql`. Let's take a look:
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/courses/hadoop-migration/oozietsthql.png"/>
# MAGIC 
# MAGIC This script creates an external Parquet table at a specific location. We've already seen how to convert the Hive data dump to Delta earlier in the course.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The first two Oozie actions have already been converted to notebooks. These notebooks can be found in the **Runnable** directory of this workspace, and their titles are:
# MAGIC * Extract-Hive-Data
# MAGIC * Create-Population-Delta
# MAGIC 
# MAGIC We want to convert the Oozie workflow to Azure Data Factory by connecting the two notebooks and sending an email whether it fails or not. For this demo, we will manually trigger the pipeline. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Open Data Factory
# MAGIC 
# MAGIC The accounts provisioned for this training should include an instance of Data Factory. Search "**data factories**" to find this resource and click on it.
# MAGIC 
# MAGIC If a Data Factory does not exist in your account, follow the instructions here:
# MAGIC [Create a Data Factory](https://docs.microsoft.com/en-us/azure/data-factory/quickstart-create-data-factory-portal#create-a-data-factory)
# MAGIC 
# MAGIC **Note**: The `Author & Monitor` button launches the Azure Data Factory UI.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## 1. Navigate to Azure Databricks Linked Service
# MAGIC 
# MAGIC Before building a pipeline, you'll add a connection back to your Databricks workspace.
# MAGIC 
# MAGIC 0. On the `Let's get started` page, select the Pencil icon in the left pane to switch to the Author tab
# MAGIC 0. Select `Connections` at the lower left corner of the Factory Resources pane
# MAGIC 0. Select `+ New` in the Connections tab under `Linked Services`
# MAGIC 0. In the `New Linked Service` window, select the `Compute` tab, then select the `Azure Databricks` tile, and select `Continue`
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/courses/azure-databricks/images/ADF-Setup-5.png"/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 2. Configure Linked Service
# MAGIC 
# MAGIC In this step, you'll generate and load an access token from Databricks to allow Data Factory to deploy and access clusters for a given workspace.
# MAGIC 
# MAGIC This is analogous to the functionality you would see with the [clusters REST API](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/clusters#clusters-api).
# MAGIC 
# MAGIC **Note**: To properly populate, this form should be completed from top to bottom. All fields not mentioned can be left with defaults.
# MAGIC 
# MAGIC 0. Select your current subscription from the drop down for `Azure subscription`
# MAGIC 0. Select the `Databricks workspace` you are currently in
# MAGIC 0. For `Select cluster`, select `Existing interactive cluster`. **Normally `New job cluster` is preferred for triggered pipelines as they use a lower cost engineering tier cluster**
# MAGIC 0. For `Access token` follow the instructions [here](https://docs.databricks.com/dev-tools/api/latest/authentication.html#generate-a-token) to generate an Access Token in the Databricks UI
# MAGIC 0. Select the name of your cluster from the drop-down list under `Choose from existing clusters`
# MAGIC 0. Select `Create`

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 3. Create an ADF Pipeline & Add a Databricks Notebook Activity
# MAGIC 
# MAGIC In this step, you'll create a new pipeline and begin the process of scheduling a Databricks notebook. Most pipelines will have multiple steps and may include Spark code refactored into .jar files and Python scripts, as well as other Data Factory activities.
# MAGIC 
# MAGIC 0. Hover over the number to the right of `Pipelines` and click on the ellipses that appear
# MAGIC 0. Select `New pipeline`
# MAGIC 0. In the `Activities` panel to the right of the `Factory Resources` panel, click `Databricks` to expand this section
# MAGIC 0. Drag the `Notebook` option into the tableau to the right

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 4. Configure Databricks Notebook Activities
# MAGIC 
# MAGIC The new pipeline should have two notebook activities, one for extracting Hive data and one for creating a Delta table. The `Extract-Hive-Data` notebook configures the correct directories for the data and passes two values (`databaseName` and `userhome`) as exit values. The pipeline will pass these values in to the `Create-Population-Delta` notebook which will build a Delta table in the correct location. The `Create-Population-Delta` runs an SQL query and passes the result as an exit value to be used as information in the email sent by the pipeline.
# MAGIC 
# MAGIC 0. With the notebook activity still selected, click the `Azure Databricks` tab near the bottom of the screen
# MAGIC 0. Select the `Databricks linked service` you created above in section 2 from the drop-down list
# MAGIC 0. Under the `Settings` tab, click the `Browse` button to enter an interactive file explorer for the directory of your linked Databricks workspace
# MAGIC 0. Navigate to the directory that contains the lesson notebooks for this course. Select the `Runnable` directory, and pick the notebook `Extract-Hive-Data`. Click `OK`. This is our first notebook, now let's add the second
# MAGIC 0. Drag another `Notebook` from the tableau to pipeline
# MAGIC 0. Repeat steps 1-3 to connect the linked service and browse the notebooks. Select the `Create-Population-Delta` notebook in the `Runnable` directory and click `OK`
# MAGIC 0. With the `Create-Population-Delta` activity selected, in Settings, click `Base parameters` to expand a drop down, and then click `New`
# MAGIC 0. We will create 2 parameters, one for each of the exit values from the first notebook. For the first, under `Name` enter `databaseName` and for `Value`, enter `@{activity('Extract-Hive-Data').output.runOutput.databaseName}`. For the second, under `Name` enter `userhome` and for `Value`, enter `@{activity('Extract-Hive-Data').output.runOutput.userhome}`
# MAGIC 0. Connect the output of the `Extract-Hive-Data` activity to the input of the `Create-Population-Delta` activity by dragging on the UI from the first activity to the second

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 5. Configure Email Activities
# MAGIC 
# MAGIC This pipeline should send an email if it fails or if it succeeds. We can pass any values from the pipeline to the emails in the same manner as above, by referencing the output of an activity. Follow the steps in this tutorial to set up an email for success and for failure: [Branching and chaining activities in a Data Factory pipeline](https://docs.microsoft.com/en-us/azure/data-factory/tutorial-control-flow-portal).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC Here is a screenshot of the configured pipeline:
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/courses/hadoop-migration/adf-pipeline.png" width=70%/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 6. Publish and Trigger the Pipeline
# MAGIC 
# MAGIC Pipelines must be published before they are run. There are a number of triggering options, including [responding to creation or deletion events in storage accounts](https://docs.microsoft.com/en-us/azure/data-factory/how-to-create-event-trigger).
# MAGIC 
# MAGIC 0. At the top left, you should see a `Publish all` button highlighted in blue with a yellow **1** on it. Click this to save your configurations (this is required to trigger the pipeline)
# MAGIC 0. Click `Add trigger` and then choose `Trigger now` from the drop-down. Click `Finish` at the bottom of the blade that appears

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 7. Monitor the Run
# MAGIC 
# MAGIC With a more complicated pipeline, you would see multiple different activities listed for a given activity run. You can read more about Databricks Jobs [here](https://docs.databricks.com/jobs.html).
# MAGIC 
# MAGIC 0. On the left most pane select the `Monitor` icon below the pencil icon. This will pull up a list of all recent pipeline runs
# MAGIC 0. In the `Actions` column, click on the left icon to `View activity runs`. This will allow you to see the current progress of your pipeline
# MAGIC 0. Your scheduled notebooks will appear in the list at the bottom of the window. Click the glasses icon to bring up the `Details`
# MAGIC 0. In the window the appears, click the `Run page url`. The page that loads will be a live view of the notebook as it runs in Azure Databricks
# MAGIC 0. Once the notebook has finished running, you'll be able to view the `Output` of the notebook by clicking the middle icon in the `Actions` column. Note that the `"runOutput"` here is the value that was passed to `dbutils.notebook.exit()` in the scheduled notebook

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Congratulations! You've now successfully converted your Oozie workflow to Azure Data Factory! 
# MAGIC 
# MAGIC These same patterns can be used to create more complicated workflows that pass values between activities. 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Spark-submit to Jobs Create API Mapping
# MAGIC 
# MAGIC You may have a pipeline that runs spark jobs using spark-submit. You can recreate these pipeline using notebooks as described above but you may also have some jobs that you want to continue to use that aren't in a notebook. Instead of triggering these jobs using spark-submit you can use the Jobs API to create or call existing jobs that run against your Databricks cluster.
# MAGIC 
# MAGIC Here is a table that can assist when mapping spark-submit commands to the Jobs API:
# MAGIC 
# MAGIC <table>
# MAGIC   <tbody>
# MAGIC     <tr>
# MAGIC       <th>Spark-submit Parameter</th>
# MAGIC       <th>How it applies on Databricks</th>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>--class</td>
# MAGIC       <td>Use the Spark Jar task to provide the main class name and the parameters.</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>--jars</td>
# MAGIC       <td>Use the libraries argument to provide the list of dependencies.</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>--py-files</td>
# MAGIC       <td>For Python jobs, use the Spark Python task. You can use the libraries argument to provide egg or wheel dependencies.</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>--master</td>
# MAGIC       <td>In the cloud, you don’t need to manage a long running master node. All the instances and jobs are managed by Databricks services. So, you can ignore this parameter on Databricks.</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>--deploy-mode</td>
# MAGIC       <td>Ignore this parameter on Databricks.</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>--conf</td>
# MAGIC       <td>In the NewCluster spec, use the spark_conf argument.</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>--num-executors</td>
# MAGIC       <td>In the NewCluster spec, use the num_workers argument. You can also use the autoscale option to provide a range (recommended).</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>--driver-memory, --driver-cores</td>
# MAGIC       <td>Based on the driver memory and cores you need, choose an appropriate instance type. ([Azure](https://azure.microsoft.com/en-us/pricing/details/databricks/)). You will provide the instance type for the driver during the pool creation. So, you can ignore this parameter during job submission.</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>--executor-memory, --executor-cores</td>
# MAGIC       <td>Based on the executor memory you need, choose an appropriate instance type. ([Azure](https://azure.microsoft.com/en-us/pricing/details/databricks/)). You will provide the instance type for the workers during the pool creation. So, you can ignore this parameter during job submission.</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>--driver-class-path</td>
# MAGIC       <td>Set spark.driver.extraClassPath to the appropriate value in spark_conf argument.</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>--driver-java-options</td>
# MAGIC       <td>Set spark.driver.extraJavaOptions to the appropriate value in the spark_conf argument.</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>--files</td>
# MAGIC       <td>Set spark.files to the appropriate value in the spark_conf argument.</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>--name</td>
# MAGIC       <td>In the Runs Submit request, use the run_name argument. In the Create Job request, use the name argument.</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC In this lesson, we demonstrated how to use Azure Data Factory as a data pipeline. You can configure ADF to run Databricks notebooks and connect activities to other services in Azure. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC - [Run a Databricks notebook with the Databricks Notebook Activity in Azure Data Factory](https://docs.microsoft.com/en-us/azure/data-factory/transform-data-using-databricks-notebook)
# MAGIC - [Passing parameters between notebooks and Data Factory](https://docs.microsoft.com/en-us/azure/data-factory/transform-data-databricks-notebook#passing-parameters-between-notebooks-and-data-factory)
# MAGIC - [Branching and chaining activities in a Data Factory pipeline](https://docs.microsoft.com/en-us/azure/data-factory/tutorial-control-flow-portal)
# MAGIC - [Expressions and functions in Azure Data Factory](https://docs.microsoft.com/en-us/azure/data-factory/control-flow-expression-language-functions)
# MAGIC - [If Condition activity in Azure Data Factory](https://docs.microsoft.com/en-us/azure/data-factory/control-flow-if-condition-activity)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>