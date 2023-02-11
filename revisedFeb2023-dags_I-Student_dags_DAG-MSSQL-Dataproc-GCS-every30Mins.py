import os
import datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
   DataprocCreateClusterOperator,
   DataprocSubmitJobOperator,
   DataprocDeleteClusterOperator
)
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.utils.dates import days_ago

PROJECT_ID = "tidal-eon-374408"
CLUSTER_NAME =  "aekanun-arrdelay-regressionmodel"
REGION = "us-east1"
ZONE = "us-east1-a"
PYSPARK_URI_mssql_to_HDFS = "gs://10feb2023/G-Student.py"
PYSPARK_URI_HDFS_to_Model = "gs://10feb2023/refinedzone_H-Student.py"

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

default_dag_args = {
    'start_date': YESTERDAY,
}

# Cluster definition
# [START how_to_cloud_dataproc_create_cluster]


from airflow.providers.google.cloud.operators.dataproc import ClusterGenerator

CLUSTER_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    image_version='1.5-centos8',
    num_workers=2,
    master_machine_type="n1-standard-2",
    master_disk_type="pd-standard",
    master_disk_size=500,
    worker_machine_type="n1-standard-2",
    worker_disk_type="pd-standard",
    worker_disk_size=500,
    properties={"spark:spark.jars.packages":"com.microsoft.azure:spark-mssql-connector:1.0.2","dataproc:dataproc.conscrypt.provider.enable":"false"}
).make()


with models.DAG(
   "MSSQL-Spark-HDFS-SparkML-GCS",
   catchup=False,
   schedule_interval=datetime.timedelta(minutes=30),
   default_args=default_dag_args) as dag:

   # [START how_to_cloud_dataproc_create_cluster_operator]
   create_cluster = DataprocCreateClusterOperator(
       task_id="create_cluster",
       project_id=PROJECT_ID,
       cluster_config=CLUSTER_CONFIG,
       region=REGION,
       cluster_name=CLUSTER_NAME,
   )
   
   delete_cluster = DataprocDeleteClusterOperator(
       task_id="delete_cluster",
       project_id=PROJECT_ID,
       region=REGION,
       cluster_name=CLUSTER_NAME,
   )

   PYSPARK_JOB_mssql_to_HDFS = {
   "reference": {"project_id": PROJECT_ID},
   "placement": {"cluster_name": CLUSTER_NAME},
   "pyspark_job": {"main_python_file_uri": PYSPARK_URI_mssql_to_HDFS},
   }
   
   
   PYSPARK_JOB_HDFS_to_Model = {
   "reference": {"project_id": PROJECT_ID},
   "placement": {"cluster_name": CLUSTER_NAME},
   "pyspark_job": {"main_python_file_uri": PYSPARK_URI_HDFS_to_Model},
   }

   pyspark_task_mssql_to_HDFS = DataprocSubmitJobOperator(
       task_id="pyspark_task_mssql_to_HDFS", job=PYSPARK_JOB_mssql_to_HDFS, region=REGION, project_id=PROJECT_ID
   )
   
   pyspark_task_HDFS_to_Model = DataprocSubmitJobOperator(
       task_id="pyspark_task_HDFS_to_Model", job=PYSPARK_JOB_HDFS_to_Model, region=REGION, project_id=PROJECT_ID
   )

   create_cluster >>  pyspark_task_mssql_to_HDFS >> pyspark_task_HDFS_to_Model >> delete_cluster