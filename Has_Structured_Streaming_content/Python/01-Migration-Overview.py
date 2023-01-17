# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Migration Overview
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Discuss the high-level steps of a migration
# MAGIC * Consider the personas involved in a migration and their responsibilities 
# MAGIC * Understand the typical decisions that are made to guide a migration engagement and the context for how those decisions are made
# MAGIC * Use your understanding of the typical personas and responsibilities involved in a migration engagement to work effectively with a broader team

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phases of a Migration Project
# MAGIC 
# MAGIC A Migration from Hadoop to Databricks is a big project with many people involved. The decision to undertake a migration is a critical one, whether it is an on-prem to cloud or cloud-to-cloud migration.
# MAGIC 
# MAGIC We will break down and discuss migration within the following 5 general steps:
# MAGIC 0. Discovery
# MAGIC 0. Planning
# MAGIC 0. Implementation
# MAGIC 0. Validate
# MAGIC 0. Go Live
# MAGIC 
# MAGIC #### Discovery
# MAGIC * Inventory and assess existing tools and technologies
# MAGIC * Identify data sources, workflows and their complexity, user access and security requirements
# MAGIC * Review Service Level Agreements (SLAs)
# MAGIC 
# MAGIC #### Planning
# MAGIC * Define the migration plan i.e. incremental, parallel, lift and shift
# MAGIC * Describe the data migration strategy
# MAGIC * Evaluate level of effort to migrate each pipeline
# MAGIC * Outline security approach and design
# MAGIC * Define a performance mitigation plan
# MAGIC 
# MAGIC #### Implementation 
# MAGIC * Install tools
# MAGIC * Execute the data and pipeline migration
# MAGIC * Configure and deploy the Security models and controls
# MAGIC * Establish and configure monitoring and performance alerts
# MAGIC 
# MAGIC #### Validate
# MAGIC * Confirm pipelines and workloads create the same results as on-prem
# MAGIC * Review Security models and controls
# MAGIC 
# MAGIC #### Go Live
# MAGIC * Cutover to new system
# MAGIC * Document entire project

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The steps outlined above give a high-level view of what is involved to complete a migration. These steps may take months to complete and usually happen incrementally. In most cases, the business owners will identify a single use case to migrate to Databricks as a first step. After successfully migrating over a key workload, the business will plan and prioritize which workloads should be migrated next. Close alignment between the migration team lead and the customer ensure that expectations are aligned and successful execution of this plan. 
# MAGIC 
# MAGIC These steps can be further broken down into separate phases that different personas will be responsible for. Before we look at the phases of a migration, let's consider the personas involved in a migration project and their required skills. Afterwards, we will look at each phase in detail and identify where you may have responsibilities and what this course will address.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Personas & Skills
# MAGIC 
# MAGIC An entire migration project will require a diverse set of skills and personas to complete the many phases. Let's take a few minutes and consider the people that may be involved in a migration project:
# MAGIC 
# MAGIC <table>
# MAGIC   <tbody>
# MAGIC     <tr>
# MAGIC       <th>Role</th>
# MAGIC       <th>Skills Required</th>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Cloud Solutions Architect</td>
# MAGIC       <td>
# MAGIC         <ul>
# MAGIC           <li>Understand a variety of Cloud services and know how to select the best services for different use cases</li>
# MAGIC           <li>Strong knowledge of networking in the Cloud</li>
# MAGIC           <li>Cloud security foundation knowledge</li>
# MAGIC           <li>Understand big data computing design patterns and anti-patterns</li>
# MAGIC         </ul>
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Data Engineer</td>
# MAGIC     
# MAGIC       <td>
# MAGIC         <ul>
# MAGIC           <li>Master several programming languages such as Java/Scala/Python</li>
# MAGIC           <li>Understand distributed computing</li>
# MAGIC           <li>Deep knowledge of distributed computing frameworks such as Apache Spark</li>
# MAGIC         </ul>
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Data Scientist</td>
# MAGIC       <td>
# MAGIC         <ul>
# MAGIC           <li>Master several programming languages such as Python and R</li>
# MAGIC           <li>Understand Machine Learning (ML)/Deep Learning (DL) algorithms and best practices</li>
# MAGIC           <li>Master common ML/DL frameworks such as Scikit-Learn, Keras, Tensorflow, Pytorch, etc.</li>
# MAGIC           <li>Know how to use Apache Spark MLlib to implement distributed ML solutions</li>
# MAGIC         </ul>
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Data Analyst</td>
# MAGIC       <td>
# MAGIC         <ul>
# MAGIC           <li>Master SQL</li>
# MAGIC           <li>Know how to use Databricks notebooks to explore and visualize data</li>
# MAGIC           <li>Know how to use BI tools such as Power BI to visualize data and create dashboards</li>
# MAGIC         </ul>
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>IT Operator</td>
# MAGIC       <td>
# MAGIC         <ul>
# MAGIC           <li>Hands-on experience to setup and configure common workflow management tools such as Azure Data Factory (ADF)</li>
# MAGIC           <li>Understand the procedures and tools to start/stop/restart/monitor production jobs</li>
# MAGIC         </ul>
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Security Architect</td>
# MAGIC       <td>
# MAGIC         <ul>
# MAGIC           <li>Deep knowledge about security and compliance requirements in the enterprise</li>
# MAGIC           <li>Understand the security best practices in Azure Cloud</li>
# MAGIC         </ul>
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Hadoop Administrator</td>
# MAGIC       <td>
# MAGIC         <ul>
# MAGIC           <li>Deep knowledge of the current on-prem Hadoop cluster configuration and its operation</li>
# MAGIC         </ul>
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Databricks Administrator</td>
# MAGIC       <td>
# MAGIC         <ul>
# MAGIC           <li>Understand Databricks deployment architecture</li>
# MAGIC           <li>Familiar with Databricks workspace features</li>
# MAGIC           <li>Understand how to provision and manage workspaces</li>
# MAGIC           <li>Manage permissions and SCIM integration</li>
# MAGIC         </ul>
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Azure Cloud Administrator</td>
# MAGIC       <td>
# MAGIC         <ul>
# MAGIC           <li>Hands-on experience launching and configuring different Azure services</li>
# MAGIC           <li>Experience configuring and monitoring firewalls, routers, VPN, and other network services in Azure Cloud
# MAGIC </li>
# MAGIC           <li>Experience configuring system monitoring solutions in Azure Cloud</li>
# MAGIC         </ul>
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>DevOps Engineer</td>
# MAGIC       <td>
# MAGIC         <ul>
# MAGIC           <li>Experience configuring CI/CD pipelines and understand associated best practices</li>
# MAGIC           <li>Understand major DevOps and automation tools
# MAGIC </li>
# MAGIC           <li>Master common scripting languages such as Linux shell, python, etc.</li>
# MAGIC           <li>Familiarity with source control systems such as GitHub</li>
# MAGIC         </ul>
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Business Analyst</td>
# MAGIC       <td>
# MAGIC         <ul>
# MAGIC           <li>Understand the business requirement of the applications</li>
# MAGIC           <li>Good communication skills to convey the business requirement to dev team</li>
# MAGIC         </ul>
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Quality Assurance Engineer</td>
# MAGIC       <td>
# MAGIC         <ul>
# MAGIC           <li>Master SQL language</li>
# MAGIC           <li>Understand business requirements and have the ability to translate them into testing plans</li>
# MAGIC         </ul>
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC This class is designed to prepare our partners with the skills and knowledge required to contribute to a team in delivering successful Hadoop migration engagements.  In this class we will follow the approach and best practices that our implementation teams at Databricks follow.  
# MAGIC 
# MAGIC In the topics below, those with a <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> sign, have been identified as the specific areas where you will most likely have direct responsibility in the near term. These topics will be visited in depth during the course of this class.
# MAGIC 
# MAGIC #### Discovery & Assessment
# MAGIC ##### Workload Discovery activities
# MAGIC * Discover existing data storage. Is HDFS the sole storage? Does some data already exist in Azure Storage? Does any data reside in Kudu or HBase? Is any data stored in third-party storage such as Elasticsearch or Cassandra? What is the size of each data storage location?
# MAGIC * Discover existing jobs. Typical applications running on Hadoop include spark batch jobs, spark streaming jobs, MapReduce jobs, Hive jobs, Impala jobs, etc. 
# MAGIC * Discover third-party tools that connect to Hadoop. A typical use case is BI tools that run ad-hoc queries via Hive or Impala.
# MAGIC * Discover resource requirements. For example, for a typical Spark application, the size and configuration of the cluster including cores, memory, storage type and volume, for the drive and executors as well as the number of executors, etc.
# MAGIC 
# MAGIC ##### Databricks sizing activities
# MAGIC * Choose the right type of cluster for different types of workloads; Databricks support both job clusters and interactive clusters
# MAGIC * Decide the cluster sizing including instance types and number of executors
# MAGIC 
# MAGIC ##### TCO activities
# MAGIC * Calculate on-prem deployment cost
# MAGIC * Estimate cost of running on Azure (Storage cost + Compute cost + Migration cost)
# MAGIC 
# MAGIC #### Security & Compliance
# MAGIC * Gather network security requirements. What are the firewall rules? Is there any IP blacklist? 
# MAGIC * Gather authentication requirements. Is there any need for Single Sign On (SSO)? What is the SSO Identity Provider?
# MAGIC * Gather encryption at rest/in motion requirement. Does the data need to be encrypted? Do other assets such as notebooks, job definitions, and cluster definitions need to be encrypted? What are the encryption types? 
# MAGIC * Gather encryption in motion requirements. For example, do we need encryption in transit between Spark workers
# MAGIC * Gather assets ACL requirements. Assets can include data, jobs, clusters, notebooks, etc. 
# MAGIC * Gather fine grained authorization requirements. Do we need column/row level security
# MAGIC * Gather audit requirements. What events require auditing?  
# MAGIC * Gather compliance requirements. PCI? HIPAA? 
# MAGIC 
# MAGIC #### New Environment Setup
# MAGIC * Provision Databricks workspaces
# MAGIC * Add users and assign admin rights
# MAGIC * Enable SCIM integration or Passthrough Authentication
# MAGIC * Mount storage to DBFS
# MAGIC 
# MAGIC #### Networking Activities 
# MAGIC * Network security and setup activities
# MAGIC 
# MAGIC #### Data Migration Activities <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/>
# MAGIC * Configure target storage with desired security features, such as access policies, encryption, etc. 
# MAGIC * Bulk data copy
# MAGIC * Migrate NoSQL database data such as HBase
# MAGIC 
# MAGIC #### Metadata Migration Activities <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/>
# MAGIC * Metastore migration for existing data
# MAGIC * Migrate table DDL for existing data
# MAGIC 
# MAGIC #### Application Migration Activities <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/>
# MAGIC * Migrate Spark applications running on YARN to Spark applications running on Databricks Runtime
# MAGIC * Migrate Hive/Impala applications to Spark
# MAGIC   * Migrate HiveQL to SparkSQL
# MAGIC   * Migrate Hive UDF/Impala UDF to Spark UDF
# MAGIC   * Review existing Hive scripts to remove unnecessary pieces if Databricks Delta is used. For example, it is unnecessary to run msck in Databricks Delta
# MAGIC * Migrate Streaming applications
# MAGIC   * Migrate on-prem Kafka installation to Azure
# MAGIC   * Migrate DStream application to Structured Streaming applications
# MAGIC * Setup DB Connect with popular IDEs to ease development
# MAGIC 
# MAGIC #### Workflow Migration Activities <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/>
# MAGIC * Migrate Oozie workflows to Azure Data Factory
# MAGIC 
# MAGIC #### Security Migration Activities 
# MAGIC * Export Sentry policy from Hive Metastore
# MAGIC * Convert Sentry ACL policy to Databricks Table ACL statements
# MAGIC * Convert Sentry ACL policy to new security policy
# MAGIC * Configure storage encryption and corresponding Databricks cluster encryption settings
# MAGIC * Configure workspaces assets ACL including jobs, notebooks, clusters, etc. 
# MAGIC * Create notebooks to analyze audit log
# MAGIC 
# MAGIC #### DevOps Activities <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/>
# MAGIC * Establish Git repository and setup projects in Git
# MAGIC * Setup Azure DevOps server
# MAGIC * Define CI/CD pipelines
# MAGIC 
# MAGIC #### Testing and QA Activities <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> 
# MAGIC * Define functional acceptance criteria
# MAGIC * Define performance acceptance criteria
# MAGIC * Write testing plans
# MAGIC * Perform functional testing
# MAGIC * Perform performance testing
# MAGIC * Write Unit and Integration tests
# MAGIC 
# MAGIC #### Operational Integration Activities
# MAGIC * Define operational notebooks
# MAGIC * Setup applications/jobs monitoring
# MAGIC * Define data lifecycle management strategies
# MAGIC * Define disaster recovery strategy
# MAGIC 
# MAGIC #### Cutover Activities
# MAGIC * Pre-cutover planning
# MAGIC * Simulate Cutover or dual execution
# MAGIC * Execute cutover
# MAGIC * Post-cutover support

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Target Technology Stack 
# MAGIC 
# MAGIC Let's take a few minutes to consider which technologies are used in Hadoop and what their parallels are in Databricks and as Cloud Native Components. 
# MAGIC 
# MAGIC <table>
# MAGIC   <tbody>
# MAGIC     <tr>
# MAGIC       <th>Workload</th>
# MAGIC       <th>Hadoop Component</th>
# MAGIC       <th>Databricks and Cloud Native Component</th>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Data Engineering and Machine Learning</td>
# MAGIC       <td>Spark on YARN</td>
# MAGIC       <td>Spark on Databricks</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>ETL via SQL</td>
# MAGIC       <td>Hive/Impala</td>
# MAGIC       <td>Spark (SQL Notebook) on Databricks</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Streaming Processing</td>
# MAGIC       <td>Spark DStream/Storm</td>
# MAGIC       <td>Spark Structured Streaming</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Batch Processing Data</td>
# MAGIC       <td>MapReduce</td>
# MAGIC       <td>Spark on Databricks</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Distributed Storage</td>
# MAGIC       <td>HDFS/Kudu</td>
# MAGIC       <td>ADLS Gen2/Delta</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>NoSQL Database</td>
# MAGIC       <td>HBase</td>
# MAGIC       <td>CosmosDB</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Streaming Ingestion</td>
# MAGIC       <td>Kafka</td>
# MAGIC       <td>Kafka/Event Hubs</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Workflow Management</td>
# MAGIC       <td>Oozie</td>
# MAGIC       <td>Notebook Workflow/Azure Data Factory (ADF)</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Machine Learning</td>
# MAGIC       <td>Cloudera Data Science Workbench</td>
# MAGIC       <td>Databricks Notebook + ML Runtime + MLFlow + Horovod</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Audit</td>
# MAGIC       <td>Cloudera Audit Server</td>
# MAGIC       <td>Databricks Audit Log/SOMETHING</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>ACL</td>
# MAGIC       <td>Sentry/Ranger</td>
# MAGIC       <td>Table ACL/ADLS Gen2 Credential Passthrough/Azure Active Directory</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC As detailed in this lesson, a migration is a large project that involves many people and requires alignment between many parts of the organization. Detailed planning and assessment of current infrastructure is key for a successful migration project and working closely with Databricks can ensure success. In this course, we are assuming that all discovery and planning phases are complete, the network, security, and environments have been configured, and a copy of the source data has landed in the cloud. We will focus the rest of the course on those topics in which you may have direct responsibility, highlighting specific sections from the implementation phases. Those phases are:
# MAGIC * Data Migration
# MAGIC * Metadata Migration
# MAGIC * Application Migration
# MAGIC * Workflow Migration
# MAGIC * DevOps Activities
# MAGIC * Testing and QA

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC * [Databricks Migration Questionnaire](https://docs.google.com/document/d/1KZi4UuNjm0lOgrSiJBbtKnhL7UvUF0pRxOqPFmqbWsE/edit) 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>