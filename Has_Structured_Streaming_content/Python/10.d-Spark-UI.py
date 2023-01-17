# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Spark UI Datadog Demo
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Understand how to install the Datadog agent for Spark and System Monitoring

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Introduction
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <p><img src="https://files.training.databricks.com/courses/hadoop-migration/datadog.png" style="max-height:400px"/></p>
# MAGIC </div>
# MAGIC 
# MAGIC This init script installs the Datadog agent to collect system metrics on every node in a cluster. It also configures the cluster for Spark monitoring.
# MAGIC 0. Configure `&lt;init-script-folder&gt;` with the location to put the init script (recommendation is to use `dbfs:/databricks/scripts`)
# MAGIC 0. Replace `&lt;your-api-key&gt;` in the `DD_API_KEY` parameter with your Datadog account key
# MAGIC 0. Run this notebook to create the script `datadog-install-driver-only.sh`
# MAGIC 0. Configure a cluster with the `datadog-install-driver-only.sh` cluster-scoped init script using the UI, Databricks CLI, or by invoking the Clusters API

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Scripts Directory
# MAGIC 
# MAGIC Check if the scripts directory exists.

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC If scripts directory does not exist, create it.

# COMMAND ----------

script_path = "<init-script-folder>" # Recommendation is to use dbfs:/databricks/scripts

dbutils.fs.mkdirs(script_path)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Install the Agent

# COMMAND ----------

dbutils.fs.put(script_path + "/datadog-install-driver-only.sh","""
#!/bin/bash

echo "Running on the driver? $DB_IS_DRIVER"
echo "Driver ip: $DB_DRIVER_IP"

cat <<EOF >> /tmp/start_datadog.sh
#!/bin/bash

if [ \$DB_IS_DRIVER ]; then
  echo "On the driver. Installing Datadog ..."
  
  # INSTALL THE DATADOG AGENT
  DD_AGENT_MAJOR_VERSION=6 DD_API_KEY=<your-api-key> DD_SITE="datadoghq.eu" bash -c "\$(curl -L https://raw.githubusercontent.com/DataDog/datadog-agent/master/cmd/agent/install_script.sh)"
  
  # WAITING UNTIL MASTER PARAMS ARE LOADED, THEN GRABBING IP AND PORT
  while [ -z \$gotparams ]; do
    if [ -e "/tmp/master-params" ]; then
      DB_DRIVER_PORT=\$(cat /tmp/master-params | cut -d' ' -f2)
      gotparams=TRUE
    fi
    sleep 2
  done

  current=\$(hostname -I | xargs)  
  
  # WRITING SPARK CONFIG FILE FOR STREAMING SPARK METRICS
  echo "init_config:
instances:
    - resourcemanager_uri: http://\$DB_DRIVER_IP:\$DB_DRIVER_PORT
      spark_cluster_mode: spark_standalone_mode
      cluster_name: \$current" > /etc/datadog-agent/conf.d/spark.yaml

  # RESTARTING AGENT
  sudo service datadog-agent restart

fi
EOF

# CLEANING UP
if [ \$DB_IS_DRIVER ]; then
  chmod a+x /tmp/start_datadog.sh
  /tmp/start_datadog.sh >> /tmp/datadog_start.log 2>&1 & disown
fi
""", True)

# COMMAND ----------

# Stop the notebook in case of a "run all"

dbutils.notebook.exit("stop")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Troubleshooting
# MAGIC 
# MAGIC ### 1. Check the `/tmp` directory
# MAGIC 
# MAGIC Look for `datadog_start.log` and `start_datadog.sh`.

# COMMAND ----------

# MAGIC %sh ls /tmp

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Check `datadog_start.log`. Look for any errors.

# COMMAND ----------

# MAGIC %sh cat /tmp/datadog_start.log

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Check `start_datadog.sh`. Look for any errors.

# COMMAND ----------

# MAGIC %sh cat /tmp/start_datadog.sh

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 2. Check the Parameters

# COMMAND ----------

# MAGIC %sh cat /tmp/master-params

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Ensure the `yaml` file is correctly configured.

# COMMAND ----------

# MAGIC %sh cat /etc/datadog-agent/conf.d/spark.yaml

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 3. Check for Probe and Agent errors
# MAGIC 
# MAGIC Check the agent status. Look for any errors.

# COMMAND ----------

# MAGIC %sh sudo datadog-agent status

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Stop the probe, if necessary.

# COMMAND ----------

# MAGIC %sh sudo service datadog-agent-sysprobe stop

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Stop the agent, if necessary.

# COMMAND ----------

# MAGIC %sh sudo service datadog-agent stop

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 4. Check Logging and Socket Permissions
# MAGIC 
# MAGIC Check the agent logging permissions.

# COMMAND ----------

# MAGIC %sh ls -l /var/log/datadog/

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Change the agent logging permissions, if required.

# COMMAND ----------

# MAGIC %sh sudo chown -R dd-agent:dd-agent /var/log/datadog/

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Check the agent socket permissions.

# COMMAND ----------

# MAGIC %sh ls -al /opt/datadog-agent/run

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Change the agent socket permissions, if required.

# COMMAND ----------

# MAGIC %sh chown dd-agent -R /opt/datadog-agent/run

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 5. Start the Probe and Agent
# MAGIC 
# MAGIC Start the probe. This must be done before the agent is started.

# COMMAND ----------

# MAGIC %sh sudo service datadog-agent-sysprobe start

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Start the agent.

# COMMAND ----------

# MAGIC %sh sudo service datadog-agent start

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>