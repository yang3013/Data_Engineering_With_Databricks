# Databricks notebook source

username = (sc._jvm.com.databricks.logging.AttributionContext.current().tags().get(
  sc._jvm.com.databricks.logging.BaseTagDefinitions.TAG_USER()).x())

userhome = f"dbfs:/user/{username}"

# COMMAND ----------

import re

database = f"""ss_delta_demo_{re.sub("[^a-zA-Z0-9]", "_", username)}_db"""

# COMMAND ----------

print(f"""
username: {username}
userhome: {userhome}
database: {database}""")

# COMMAND ----------

dbutils.widgets.text("mode", "setup")
mode = dbutils.widgets.get("mode")

# COMMAND ----------

if mode == "reset":
  spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

# COMMAND ----------

class BuildPaths:
  
    def __init__(self, base_path=f"{userhome}/ss-delta-demo/delta_tables", source_dir="/mnt/ss-delta-demo-source/med-recordings", db="ss_delta_demo"):
        spark.sql(f"USE {db}")
        self.sourceDir = source_dir

        self.basePath = base_path
        self.checkpointPath = f"{self.basePath}/_checkpoints"

        self.bronzeTable = f"{self.basePath}/bronze"
        self.bronzeCheckpoint = f"{self.checkpointPath}/bronze_checkpoint"

        self.silverPath = f"{self.basePath}/silver"
        self.silverCheckpointPath = f"{self.checkpointPath}/silver"

        self.silverRecordingsTable = f"{self.silverPath}/recordings"
        self.silverChartsTable = f"{self.silverPath}/charts"
        self.silverPIITable = f"{self.silverPath}/pii"
        self.recordingsMRNTable = f"{self.silverPath}/recordings_mrn"
        self.piiCurrentPath = f"{self.silverPath}/pii_current"
        self.chartsValidTable = f"{self.silverPath}/charts_valid"
        self.clinicLookupTable = f"{self.silverPath}/clinic_lookup"

        self.silverRecordingsCheckpoint = f"{self.silverCheckpointPath}/silver_recordings"
        self.silverChartsCheckpoint = f"{self.silverCheckpointPath}/silver_charts"
        self.silverPIICheckpoint = f"{self.silverCheckpointPath}/silver_pii"
        self.recordingsMRNCheckpoint = f"{self.silverCheckpointPath}/recordings_mrn"
        self.piiCurrentCheckpoint = f"{self.silverCheckpointPath}/pii_current"
        
        self.goldPath = f"{self.basePath}/gold"
        self.goldCheckpointPath = f"{self.checkpointPath}/gold"
        
        self.hourlyDemographicsTable = f"{self.goldPath}/hourly_demographics"
        self.clinic6Table = f"{self.goldPath}/clinic_6"            
        self.errorRateTable = f"{self.goldPath}/error_rate"
        
        self.hourlyDemographicsCheckpoint = f"{self.goldCheckpointPath}/hourly_demographics"
        self.clinic6Checkpoint = f"{self.goldCheckpointPath}/clinic_6"
        
    def __repr__(self):
        return self.__dict__.__repr__().replace(", ", ",\n")

# COMMAND ----------

Paths = BuildPaths(db=database)

# COMMAND ----------

if mode == "reset":
  dbutils.fs.rm(Paths.basePath, True)

# COMMAND ----------

if mode == "reset":
  if "/mnt/ss-delta-demo-source/" in [x.mountPoint for x in dbutils.fs.mounts()]:
    dbutils.fs.unmount("/mnt/ss-delta-demo-source/")
    

# COMMAND ----------

container = "ss-delta-demo-source"
storage_acct = "enbpublicus"

# SAS Token expires 2021-01-01
SASTOKEN = "?sv=2019-12-12&ss=b&srt=sco&sp=rl&se=2021-01-01T23:22:36Z&st=2020-09-10T14:22:36Z&spr=https&sig=TQYkvnJckYZWW1g%2F3gHUny6MLrnJ4etdR8iwFzb06xU%3D" 
SOURCE = f"wasbs://{container}@{storage_acct}.blob.core.windows.net/"
URI = f"fs.azure.sas.{container}.{storage_acct}.blob.core.windows.net"

mountedSource = "/mnt/ss-delta-demo-source/"

try:
  dbutils.fs.mount(
    source=SOURCE,
    mount_point=mountedSource,
    extra_configs={URI:SASTOKEN})
except Exception as e:
  if "Directory already mounted" in str(e):
    pass # Ignore error if already mounted.
  else:
    raise e

container = "med-recordings"
storage_acct = "enbpublic"

# SASTOKEN valid until 2021-01-01
SASTOKEN = "?sv=2019-12-12&ss=b&srt=sco&sp=rl&se=2021-01-02T04:32:00Z&st=2020-09-04T19:32:00Z&spr=https&sig=1A0h%2BnS53PxreKO2iTTaJQkuRtDQPpboqM2hP7PyTOE%3D"
SOURCE = f"wasbs://{container}@{storage_acct}.blob.core.windows.net/"
URI = f"fs.azure.sas.{container}.{storage_acct}.blob.core.windows.net"

BasePath = "/mnt/med-recordings/"

try:
  dbutils.fs.mount(
    source=SOURCE,
    mount_point=BasePath,
    extra_configs={URI:SASTOKEN})
except Exception as e:
  if "Directory already mounted" in str(e):
    pass # Ignore error if already mounted.
  else:
    raise e
