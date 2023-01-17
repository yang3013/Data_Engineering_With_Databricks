# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks CLI
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Utilize Databricks Command Line Interface (CLI)
# MAGIC * Understand how to use DB-CLI with version control systems

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## What is Databricks CLI?
# MAGIC 
# MAGIC Databricks CLI is an intuitive interface that allows you to interact with the Databricks environment from your local machine.
# MAGIC 
# MAGIC In this lesson, we will explore the functionality Databricks CLI offers and a common use of DB-CLI: exporting notebooks to a version control system.
# MAGIC 
# MAGIC Because Databricks CLI allows you to access the Databricks environment from your local machine, it also provides a way to connect your local machine and any files or directories it contains to the Databricks environment, and vice-versa.
# MAGIC 
# MAGIC We can use that to move files to and from the Databricks environment, and then push them to a version control system, such as GitHub, from our local machine.
# MAGIC 
# MAGIC However, DB-CLI also provides many other useful features. A full list can he found [here](https://docs.databricks.com/dev-tools/cli/index.html).
# MAGIC 
# MAGIC We'll explore a number of these features, and then demonstrate how we can use them to move our notebooks to our local machines and to a remote repository.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Configuring the Databricks CLI
# MAGIC 
# MAGIC Before we can use the CLI, we have to install and configure it, so that it can connect to our specific Databricks environment.
# MAGIC 
# MAGIC ### Installation
# MAGIC 
# MAGIC To install the Databricks CLI, run `pip install databricks-cli` from the command line.
# MAGIC 
# MAGIC **Note**: Use `pip3 install databricks-cli` if Python 3 is not your default Python.
# MAGIC 
# MAGIC ### Configuring Access with a User-Generated Token
# MAGIC 
# MAGIC **Note**: The full Databricks CLI documents are available for each command using the `-h` flag.
# MAGIC 
# MAGIC First, generate a token from the Databricks UI.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/images/awscore/s3bucket-secrets/user-settings.png" width=40%/></p>
# MAGIC </div>
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/images/awscore/s3bucket-secrets/generate-token.png" width=40%/></p>
# MAGIC </div>
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/images/awscore/s3bucket-secrets/new-token.png" width=40%/></p>
# MAGIC </div>
# MAGIC 
# MAGIC Copy this value to your clipboard.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/images/awscore/s3bucket-secrets/copy-token.png" width=40%/></p>
# MAGIC </div>
# MAGIC 
# MAGIC In the command line, run `databricks configure --token`.
# MAGIC 
# MAGIC As prompted, enter the URL for the Databricks workspace.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/images/awscore/s3bucket-secrets/cli-token.png" width=80%/></p>
# MAGIC </div>
# MAGIC 
# MAGIC As prompted, paste the token generated from the Databricks UI.
# MAGIC 
# MAGIC To confirm success, run `databricks workspace ls`. You should see the directories in the top level of your workspace.
# MAGIC 
# MAGIC ### Reviewing Tokens and Adding Additional Workspaces
# MAGIC 
# MAGIC Token configurations are saved to `~/.databrickscfg`.
# MAGIC 
# MAGIC To review and edit this file in vim, run `vi ~/.databrickscfg`.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/images/awscore/s3bucket-secrets/databrickscfg.png" width=50%/></p>
# MAGIC </div>
# MAGIC 
# MAGIC The recently added token will be listed under `[DEFAULT]`.
# MAGIC 
# MAGIC You can add additional profiles by adding the option `--profile <profile_name>` to your `databricks configure --token` command, or by directly editing this file. Each entry will have the form:
# MAGIC 
# MAGIC ```
# MAGIC [PROFILE-NAME]
# MAGIC host = <host-url>
# MAGIC token = <token>
# MAGIC ```
# MAGIC 
# MAGIC All Databricks CLI commands will accept the `--profile` option, but will use the `DEFAULT` profile unless otherwise specified.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Databricks CLI Features
# MAGIC 
# MAGIC Now that we have DB-CLI configured, let's explore some of its most useful features.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## 1 - Secrets CLI
# MAGIC 
# MAGIC One convenient use of DB-CLI is to manage your secrets remotely. Below we go through creating a scope and creating a secret.
# MAGIC 
# MAGIC ### Create a Scope
# MAGIC 
# MAGIC To create a new scope, execute `databricks secrets create-scope --scope <(scope-name)>`.
# MAGIC 
# MAGIC You can confirm success by running `databricks secrets list-scopes`.
# MAGIC 
# MAGIC ### Add a Secret
# MAGIC 
# MAGIC Secrets are added as key/value pairs. The key will be visible and should describe the value it corresponds to.
# MAGIC 
# MAGIC To add a new secret, execute `databricks secrets put --scope <(scope-name)> --key <(key)>`.
# MAGIC 
# MAGIC This will open a vim editor in which you can enter the secret value.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/images/awscore/s3bucket-secrets/put-secret-key.png" width=60%/></p>
# MAGIC </div>
# MAGIC 
# MAGIC **Note**: You can pass secret values directly as strings in the command line using the `--string-value <(TEXT)>` option, or load the contents of a file using `--binary-file <(PATH)>`.
# MAGIC 
# MAGIC To confirm your secret has loaded, run `databricks secrets list --scope <(scope-name)>`.
# MAGIC 
# MAGIC **Warning**: While secrets provide a great way to manage tokens and other credentials you don't wish to display in plain text, users in the Databricks workspace with access to these secrets will be able to read and print out the bytes. Secrets should, therefore, be conceived of as a more secure way to manage access to resources than distributing credentials in files or plain text, but **you should only grant access to individuals that can be trusted with these secrets**.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## 2 - Clusters CLI
# MAGIC 
# MAGIC We can also manage clusters from the command line. 
# MAGIC 
# MAGIC Let's look at how we would start and then terminate a cluster. 
# MAGIC 
# MAGIC We can create a cluster using `databricks clusters create <(JSON STRING OR FILE PATH)>` with instructions [here](https://docs.databricks.com/dev-tools/api/latest/clusters.html#clusters-api)
# MAGIC 
# MAGIC For this demo, we have already created a cluster, but it is inactive. 
# MAGIC 
# MAGIC ### Start a Cluster Remotely
# MAGIC 
# MAGIC To start the cluster, we first find the Cluster ID. We can do this with `databricks clusters list` to show active or recently terminated clusters.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/15.png" width=60%/></p>
# MAGIC </div>
# MAGIC 
# MAGIC Next, we run `databricks clusters start --cluster-id (ClusterID)`
# MAGIC 
# MAGIC Now we have a running cluster!
# MAGIC 
# MAGIC To delete the cluster,  we can run `databricks clusters delete --cluster-id (CluserID)`

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## 3 - DBFS CLI
# MAGIC 
# MAGIC We can also move files to and from the local machine to DBFS with DB-CLI. 
# MAGIC 
# MAGIC ### Navigate DBFS Remotely
# MAGIC 
# MAGIC First, we can navigate the DBFS like a normal file system. 
# MAGIC 
# MAGIC We can traverse DBFS by running `databricks fs <COMMAND>`, or `dbfs <COMMAND>` for short. 
# MAGIC 
# MAGIC For example, we can run `dbfs ls` to list current files and directories. 
# MAGIC 
# MAGIC We can copy a file from our local machine to DBFS with `dbfs cp <FILE> <PATH>`. For example, let's make our text file.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/16.png" width=60%/></p>
# MAGIC </div>
# MAGIC 
# MAGIC Next, we can move it and see it is now in our DBFS.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/17.png" width=60%/></p>
# MAGIC </div>
# MAGIC 
# MAGIC To do this process in reverse and move from DBFS to our local machine, we can run `dbfs cp <PATH> <DESTINATION-PATH>`

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## 4 - Jobs CLI
# MAGIC 
# MAGIC It is also possible to schedule and execute jobs remotely.
# MAGIC 
# MAGIC Here, we'll demonstrate using DB-CLI to run a job with its associated job id. 
# MAGIC 
# MAGIC ### Get the Job Id
# MAGIC 
# MAGIC After adding the job, we can grab its job id here. 
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/19.png" width=100%/></p>
# MAGIC </div>
# MAGIC 
# MAGIC ### Running the Job Remotely
# MAGIC 
# MAGIC Next, we just run `databricks jobs run-now --job-id (job-id)`.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/20.png" width=80%/></p>
# MAGIC </div>
# MAGIC 
# MAGIC Looking back at the jobs menu, we can see that it ran.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/21.png" width=100%/></p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 5 - Workspace CLI
# MAGIC 
# MAGIC This CLI allows a user to list, import, export, and delete folders and notebooks. 
# MAGIC 
# MAGIC ### List Workspace
# MAGIC 
# MAGIC To list the files and directories use `databricks workspace ls`.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Workspace with Version Control
# MAGIC 
# MAGIC ### Basic Idea
# MAGIC 
# MAGIC The DB-CLI Workspace tool is very useful if we want to move our code, which is stored in a notebook, to an external version control system, such as GitHub.
# MAGIC 
# MAGIC If you are unfamiliar with Git, a great online tutorial can be found [here](https://product.hubspot.com/blog/git-and-github-tutorial-for-beginners).
# MAGIC 
# MAGIC The core idea is that we use the Workspace CLI to export and import notebooks and files to and from our local machine and the Databricks workspace.
# MAGIC 
# MAGIC Once the notebook is exported to our local machine, we can push it to GitHub, for example, as normal. 
# MAGIC 
# MAGIC ### Import Files to Databricks Workspace (Local -> Databricks)
# MAGIC 
# MAGIC If we wanted to move a file from our local machine into a notebook to edit in the Databricks environment, we could run `databricks workspace import_dir &lt;SOURCEPATH&gt; &lt;TARGETPATH&gt;`.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/22.png" width=80%/></p>
# MAGIC </div>
# MAGIC 
# MAGIC We can see from the output that the file got imported.
# MAGIC 
# MAGIC ### Export Files (Databricks -> Local)
# MAGIC 
# MAGIC To move to our local machine, we run `databricks workspace export_dir &lt;SOURCEPATH&gt; &lt;TARGETPATH&gt;`.
# MAGIC 
# MAGIC Let's say we have the following notebook:
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/24.png" width=70%/></p>
# MAGIC </div>
# MAGIC 
# MAGIC To move it, we place it a folder we wish to export.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/23.png" width=70%/></p>
# MAGIC </div>
# MAGIC 
# MAGIC Now we have the notebook as a local file! Depending on your version control system, you handle the next steps a little differently. 
# MAGIC 
# MAGIC For GitHub, you could run `git init` in the localexportdir, `git add *`, `git commit -m &lt;MESSAGE&gt;`, configure your remote connection, and then run a `git push` command.
# MAGIC 
# MAGIC These steps would be identical as you would treat a normal local file. The notebook is just a local file now. If you wanted to get the updated code from the remote repo into your Databricks environment, the process would just be in reverse.
# MAGIC 
# MAGIC You could pull the most recent commit, and run `import_dir` to get it back into Databricks.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>