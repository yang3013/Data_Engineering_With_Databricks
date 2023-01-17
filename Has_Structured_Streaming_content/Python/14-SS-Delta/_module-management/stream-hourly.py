# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Kafka Simulator
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> **Students should not run this notebook during instructor-led delivery.** The notebooks included in this module are currently aligned toward instructor-led, interactive delivery. As such, this notebook will only be run by the **instructor**. _If you wish to execute this independently for later review of the materials, you **can**, but you'll want to ensure you clean up your workspace once you've finished execution._
# MAGIC 
# MAGIC This notebook is designed to be run as a standalone job on a tiny driver-only cluster as a fake Kafka source. When deploying this notebook as a job, please use the following cluster settings (changed values are indicated by red boxes):
# MAGIC - Worker Type: **Standard_F4s** 
# MAGIC - Workers: **0**
# MAGIC - Spark Config: **`spark.master local[*]`**
# MAGIC 
# MAGIC ![Fake Kafka Cluster Setting](https://files.training.databricks.com/images/enb/med_data/fake-kafka-cluster-azure.png)
# MAGIC  
# MAGIC An hour of data will arrive approximately every 7.5s. This script is configured to move 3 months of data, which should provide enough files for approximately 3.5 hours of streaming.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Before the fake stream begins, we'll do a batch migration of data for the month of December.

# COMMAND ----------

# MAGIC %md
# MAGIC Clear the mount points used.

# COMMAND ----------

# [dbutils.fs.unmount(mount) 
#  for mount in ["/mnt/med-recordings/", "/mnt/ss-delta-demo-WRITE/"] 
#  if mount in [x.mountPoint for x in dbutils.fs.mounts()]];

# COMMAND ----------

# MAGIC %md
# MAGIC Mount the object stores for the source and target of our Kafka simulator.

# COMMAND ----------

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

# COMMAND ----------

container = "ss-delta-demo-source"
storage_acct = "enbpublicus"

SOURCE = f"wasbs://{container}@{storage_acct}.blob.core.windows.net/"
URI = f"fs.azure.sas.{container}.{storage_acct}.blob.core.windows.net"

TargetPath = "/mnt/ss-delta-demo-WRITE/"

try:
  dbutils.fs.mount(
    source=SOURCE,
    mount_point=TargetPath,
    extra_configs={URI:dbutils.secrets.get("instructor", "ss-demo-write")}) # SASTOKEN valid until 2021-01-01
except Exception as e:
  if "Directory already mounted" in str(e):
    pass # Ignore error if already mounted.
  else:
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC **NON-INSTRUCTOR** The logic provided above requires write permissions on external block storage. If you wish to run this indepedently, you can do so by instead writing to your DBFS (and changing the `source_dir` variable where appropriate throughout the rest of the materials).

# COMMAND ----------

# TargetPath = "/ss-delta-demo-source/"

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell only if you wish to reset the base directory for this demo. (If all data has run, this may take over 3 minutes).

# COMMAND ----------

# if dbutils.fs.ls(TargetPath):
#     dbutils.fs.rm(TargetPath + "med-recordings", True)
# 
# dbutils.fs.mkdirs(TargetPath + "med-recordings")

# COMMAND ----------

class FakeKafka:
    import datetime
    import time
    
    def __init__(self,base_path,target_path):
        self.base_path = base_path
        self.target_path = target_path

    def stream(self, num_months=3, start_year=2020, start_month=1, start_day=1, start_hour=0, sleep=5):
        """
        Begins stream as specified start and streams num_months. Configured to always finish the starting month and then stream only full months if num_months > 1
        """ 
        self.start_year = start_year
        self.start_month = start_month
        self.start_day  = start_day
        self.start_hour = start_hour
        self.start_timestamp = self.datetime.datetime(start_year,
                                                start_month,
                                                start_day,
                                                start_hour)
        end_year, end_month = (start_year + 1, (start_month+num_months) % 12) if (start_month + num_months) > 12 else (start_year, start_month + num_months) 
        self.end_timestamp = self.datetime.datetime(end_year, 
                                                    end_month, 
                                                    1)
        self.time_tuples = self.make_hours(self.start_timestamp, self.end_timestamp)
        for hour in self.time_tuples:
            try:
                print(f"Moving records for {'/'.join(str(x) for x in hour[:4])}")
                self.move_day(hour)
                self.time.sleep(sleep)
            except:
                print(f"No records for {'/'.join(str(x) for x in hour[:4])}")
    
    def move_day(self, time_tuple):
        full_path = self.base_path + "/".join(str(x).zfill(2) for x in time_tuple[:4])
        dbutils.fs.cp(full_path, self.target_path, True)
        
    def make_hours(self, curr_time, max_time, time_tuples=[]):
        if curr_time >= max_time:
            return time_tuples
        time_tuples.append(curr_time.timetuple())
        new_time = curr_time + self.datetime.timedelta(hours=1)
        return self.make_hours(new_time, max_time, time_tuples)
    
    def batch(self, num_months=1, start_year=2019, start_month=12):
        month_tuple = [(start_year+1, month%12) if month > 12 else (start_year, month) for month in range(start_month, start_month + num_months)]
        [print(month) for month in month_tuple]
        for month in month_tuple:
            self.move_month(*month)
    
    def move_month(self, year=2019, month=12):
         (spark.read
            .format("parquet")
            .load(self.base_path + f"{year}/{str(month).zfill(2)}/*/*")
            .write
            .format("parquet")
            .mode("append")
            .save(self.target_path)
         )


# COMMAND ----------

kafka = FakeKafka(BasePath, TargetPath + "med-recordings")

# COMMAND ----------

# MAGIC %md
# MAGIC By default, the following cell will migrate historical records from December 2019 as a single batch. (This assumes a clean target directory, as all data is appended, and re-execution could lead to duplicate records and unexpected behavior).
# MAGIC 
# MAGIC **Ensure that you comment out this cell prior to scheduling this notebook. This data is only currently stored in a single container in 1 region. As such, this operation may take over 10 minutes due to the number of small files in the source directory and network latency.**

# COMMAND ----------

kafka.batch()

# COMMAND ----------

# MAGIC %md
# MAGIC By default, the following cell will start landing hourly records into a source directory and is configured to do this for 3 months of data starting in January 2020.

# COMMAND ----------

kafka.stream()

# COMMAND ----------

# MAGIC %md
# MAGIC **INSTRUCTOR-ONLY**
# MAGIC 
# MAGIC There is an additional half-month of data available to continue streaming on Day 2. The cell below requires:
# MAGIC 0. All data is mounted correctly.
# MAGIC 0. The `BasePath` and `TargetPath` variables (defined above in the mounting logic).
# MAGIC 0. The `FakeKafka` class definition.

# COMMAND ----------

# kafka = FakeKafka(BasePath, TargetPath + "med-recordings")
# kafka.stream(start_month=4, num_months=1)


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>