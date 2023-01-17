# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Azure Databricks Hadoop Migration
# MAGIC 
# MAGIC ### COURSE DESCRIPTION
# MAGIC This course is a 5 half-day hands-on training experience that will provide attendees with the technical skills required for planning, developing, and optimizing a real-world use case with Databricks. In this case, we have selected the scenario of a Hadoop migration which is in strong demand from customers. During the class, you will build a migration inventory, establish a phased migration plan, design a reference architecture, and implement a data pipeline. This class is taught on Azure Databricks.
# MAGIC 
# MAGIC ### PREREQUISITES
# MAGIC - Proficiency in SQL and either Python or Scala
# MAGIC - Completion of the Core Technical Training course OR completion of the following self-paced training courses:
# MAGIC   - Getting Started with Apache Spark SQL
# MAGIC   - ETL Part 1: Data Extraction
# MAGIC   - ETL Part 2: Data Transformation and Loads
# MAGIC   - ETL Part 3: Production
# MAGIC   - Managed Delta Lake
# MAGIC   - Structured Streaming
# MAGIC - The class will be taught primarily in Python
# MAGIC - Experience working with Azure

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agenda
# MAGIC 
# MAGIC <table>
# MAGIC   <tbody>
# MAGIC     <tr>
# MAGIC       <th>Topic</th>
# MAGIC       <th>Description</th>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>[Migration Overview]($./01-Migration-Overview)</td>
# MAGIC       <td>Outline the phases and personas in a migration project</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>[Architecture Overview]($./02-Architecture-Overview)</td>
# MAGIC       <td>Consider the implementation details of different SQL engines</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>[Architecture Lab]($./02L-Architecture-Lab)</td>
# MAGIC       <td>Lab</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>[Metadata Migration]($./03-Metadata-Migration)</td>
# MAGIC       <td>Discuss the role of and migrate metadata</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>[Import Hive Tables Overview]($./04-Import-Hive-Tables-Overview)</td>
# MAGIC       <td>Migrate Hive tables to Spark tables</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>[Import ORC Format]($./05-Import-ORC-Format)</td>
# MAGIC       <td>Migrate ORC formatted tables to Spark tables</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>[Import Hive Format Lab]($./06L-Import-Hive-Format-Lab)</td>
# MAGIC       <td>Lab</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>[SQL Endpoint]($./07-SQL-Endpoint)</td>
# MAGIC       <td>Connect to BI tools using JDBC/ODBC</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>[Verify Import]($./08-Verify-Import)</td>
# MAGIC       <td>Verify data migration did not introduce errors</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>[Verify Import Lab]($./08L-Verify-Import-Lab)</td>
# MAGIC       <td>Lab</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>[Importing User Defined Functions]($./09.a-Importing-User-Defined-Functions)</td>
# MAGIC       <td>Migrate User Defined Functions to Spark</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>[Comparing User Defined Functions]($./09.b-Comparing-User-Defined-Functions)</td>
# MAGIC       <td>Compare different implementations of User Defined Functions</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>[Spark UI]($./10.a-Spark-UI)</td>
# MAGIC       <td>Explore the Spark UI</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>[Spark UI Demo]($./10.b-Spark-UI)</td>
# MAGIC       <td>Spark UI Demo</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>[Ganglia Metrics]($./10.c-Spark-UI)</td>
# MAGIC       <td>Explore Ganglia Metrics</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>[Metrics with Datadog]($./10.d-Spark-UI)</td>
# MAGIC       <td>Use Datadog for Spark and System Monitoring</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>[Spark UI Lab]($./10L-Spark-UI-Lab)</td>
# MAGIC       <td>Lab</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>[Partition Best Practices]($./11-Partition-Best-Practices)</td>
# MAGIC       <td>Consider best practices for partitioning</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>[Partition Best Practices Lab]($./11L-Partition-Best-Practices-Lab)</td>
# MAGIC       <td>Lab</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>[Optimization Best Practices]($./12-Optimization-Best-Practices)</td>
# MAGIC       <td>Consider best practices for optimizing Spark workloads</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>[Optimization Best Practices Lab 1]($./12L.a-Optimization-Best-Practices-Lab)</td>
# MAGIC       <td>Lab</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>[Optimization Best Practices Lab 2]($./12L.b-Optimization-Best-Practices-Lab)</td>
# MAGIC       <td>Lab</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>[Delta Optimization Best Practices]($./13-Delta-Optimization-Best-Practices)</td>
# MAGIC       <td>Consider best practices for optimizing Delta</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>[Delta Optimization Best Practices Lab 1]($./13L.a-Delta-Optimization-Best-Practices-Lab)</td>
# MAGIC       <td>Lab</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>[Delta Optimization Best Practices Lab 2]($./13L.b-Delta-Optimization-Best-Practices-Lab)</td>
# MAGIC       <td>Lab</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>[Delta Optimization Best Practices Lab 3]($./13L.c-Delta-Optimization-Best-Practices-Lab)</td>
# MAGIC       <td>Lab</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>[Advanced Streaming]($./14-SS-Delta/00-SS-best-practices)</td>
# MAGIC       <td>Advanced Structured Streaming and Best Practices</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>[Migrate Oozie Workflows]($./15-Migrate-Oozie-Workflows)</td>
# MAGIC       <td>Migrate Oozie workflows to Azure Data Factory</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>[IDE Databricks Connect]($./16-IDE-DB-Connect)</td>
# MAGIC       <td>Work with Databricks from an IDE</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>[Databricks CLI]($./17-Databricks-CLI)</td>
# MAGIC       <td>Use the Databricks CLI to interact with your workspace</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>[CICD Azure DevOps]($./18-CICD-Azure-DevOps)</td>
# MAGIC       <td>Configure CI/CD using Azure DevOps</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>