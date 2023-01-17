# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # CI/CD with Azure DevOps
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Understand the benefits of CI/CD pipelines
# MAGIC * Integrate Databricks with an Azure DevOps CI/CD pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## What is CI/CD?
# MAGIC 
# MAGIC CI/CD stands for Continuous Integration/Continuous Deployment. It is a popular software development model aimed at making the process of developing and combining code in a group and then deploying it as easy as possible.
# MAGIC 
# MAGIC In large software development teams, often bugs can be introduced when different developers write code in isolation and then combine it in the final project. The more code that is done before combination is attempted; the more errors have the potential to compound. 
# MAGIC 
# MAGIC The CI/CD model aims at fixing this by having automated testing so that developers frequently push and pull from the master branch, as long as these tests pass. This allows other users to quickly understand and adapt to change other developers make to the master branch, instead of finding out much later when it could be a problem. 
# MAGIC 
# MAGIC The model also has automated testing for the deployment side, so that once the master branch is organized and passes all its tests, the new code is then checked as a whole. If it all passes, it is then put immediately into deployment so it can reach users faster.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Azure DevOps Pipeline
# MAGIC 
# MAGIC We have seen how you can use Databricks CLI to export notebooks and use them in various pipelines and version control systems. However, Databricks also directly integrates with Azure DevOps, making it extremely easy to manage your pipeline directly from the Databricks environment.
# MAGIC 
# MAGIC For this demonstration, let's consider the following scenario:
# MAGIC 
# MAGIC You are working as part of a 3-person team on a new feature. On the team, everyone will code in their own folder in a shared workspace and commit to the same feature branch. In this case, the feature branch is in Azure DevOps. 
# MAGIC 
# MAGIC When a commit is made to the feature branch, it will be promoted through automated testing to the “shared” directory of the current workspace.
# MAGIC 
# MAGIC The “shared” directory should be used only for manual testing and double-checking. No code should be developed here.
# MAGIC 
# MAGIC There is another CI/CD pipeline that checks in code to a QA branch that promotes the feature through to the QA environment. This demo covers the committing to the feature branch and promoting the code to the “shared” folder only.
# MAGIC 
# MAGIC ### Model
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/CICD_Azure_Devops.png", width=800/></p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Pipeline
# MAGIC 
# MAGIC The diagram above illustrates the steps we will need to take to work with our pipeline. Let's walk through this process. 
# MAGIC 
# MAGIC First, we are going to want an active cluster attached to our notebook. 
# MAGIC 
# MAGIC Second, we need a feature branch within the repo in Azure DevOps that we can use to develop this feature on. Create a new branch in the Repo in Azure DevOps.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/cicd%20screenshots/new-branch.png" width=500/></p>
# MAGIC </div>
# MAGIC 
# MAGIC Here, our branch is called nz-feature.
# MAGIC 
# MAGIC Next, we need a notebook feature to commit. We will develop our feature in the “LoadAllNations” demo notebook. Let’s make sure the notebook is synced with Azure DevOps.
# MAGIC 
# MAGIC In Databricks we can do this in Git Preferences. 
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/cicd%20screenshots/git-preferences.png" width=500/></p>
# MAGIC </div>
# MAGIC 
# MAGIC Once we have our notebook synced, we can develop our feature here.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/cicd%20screenshots/finished-feature.png", width=1000/>
# MAGIC </p>
# MAGIC </div>
# MAGIC 
# MAGIC This is our feature, with a comment at the bottom indicating we are ready to commit. 
# MAGIC 
# MAGIC As an example, let’s say we’ve completed our feature and for this demo it will be represented as the comments at the bottom of this notebook.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Commit
# MAGIC 
# MAGIC In order to commit, we simply have to save our notebook once it is linked to our Azure DevOps workspace and pipeline. 
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/cicd%20screenshots/save-revision.png", width=500/></p>
# MAGIC </div>
# MAGIC 
# MAGIC Once we have the link to the Azure workspace and select our feature branch, we just save the notebook and it will be updated automatically and pushed onto the pipeline. Notice we selected `Also commit to Git` in the save menu. 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Pull Request
# MAGIC 
# MAGIC Once we have pushed our feature onto our feature branch, we can create a pull request within Azure DevOps.
# MAGIC 
# MAGIC Our pipeline is configured to kick off when a change is made to the develop branch. By completing the Pull Request, we should be able to see that our pipeline is now running.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/cicd%20screenshots/create-pr.png", width=700/></p>
# MAGIC </div>
# MAGIC 
# MAGIC Now we choose to pull to our develop branch.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/cicd%20screenshots/new-pr.png", width=700/></p>
# MAGIC </div>
# MAGIC 
# MAGIC If we go to Pipelines in Azure DevOps, we can see we now have one running.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/cicd%20screenshots/running-ci-pipeline.png", width=700/></p>
# MAGIC </div>
# MAGIC 
# MAGIC This pipeline is our CI Build pipeline. When it runs through this pipeline, it will:
# MAGIC * Configure the build agent
# MAGIC * Get the latest changes
# MAGIC * Execute unit tests
# MAGIC * Package code
# MAGIC * Publish results
# MAGIC * Generate an artefact
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/cicd%20screenshots/ci-details.png" width=700/></p>
# MAGIC </div>
# MAGIC 
# MAGIC It's worth noting that when the pipeline, perhaps for unit tests, requires access to Databricks functionality, it uses a configured version of dbconnect, which is part of the pipeline as well. 
# MAGIC 
# MAGIC The end result of the CI pipeline is an artefact that is ready to be picked up and used by the CD pipeline. The CD pipeline is set to be manually triggered, let’s check it out.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Release Pipeline
# MAGIC 
# MAGIC Now we have to create a release so that we can trigger the release pipeline and move our artefact we just created in the CI pipeline to our shared notebook. Once it is there, we can check over the code in the shared file before releasing it from there to the final QA environment. 
# MAGIC 
# MAGIC This is our release pipeline. The second part is the release part we are about to manually trigger. 
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/cicd%20screenshots/release-pipeline.png" width=700/></p>
# MAGIC </div>
# MAGIC 
# MAGIC We can see the history of our past releases and then create a new release. This will move our most recent CI pipeline artefact to the shared folder.
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/cicd%20screenshots/create-release-1.png" width=700/></p>
# MAGIC </div>
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/cicd%20screenshots/create-release-2.png" width=700/></p>
# MAGIC </div>
# MAGIC 
# MAGIC We can click on our pipeline to see that it is running. 
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/cicd%20screenshots/running-release.png" width=700/></p>
# MAGIC </div>
# MAGIC 
# MAGIC Once it is completed, we are ready to kick off our release pipeline for CD. This pipeline will:
# MAGIC * Set Python version
# MAGIC * Unpack build artefact
# MAGIC * Deploy notebook to workspace
# MAGIC * Deploy library to DBFS
# MAGIC * Install library on cluster
# MAGIC * Execute integration tests
# MAGIC * Publish test results

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## That's it!
# MAGIC 
# MAGIC We now have our feature in the shared folder back in our Databricks workspace. After manual analysis, we can then release the feature into a QA environment.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>