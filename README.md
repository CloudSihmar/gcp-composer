# Dataproc Spark to BigQuery Demo

This project demonstrates the integration of Google Cloud Dataproc, Apache Spark, and BigQuery using Apache Airflow for orchestration. The process involves creating a Dataproc cluster, running a PySpark job to process data, and writing the results to a BigQuery table.

## Prerequisites

- Google Cloud Platform (GCP) account
- Google Cloud Storage (GCS) bucket
- Dataproc cluster
- BigQuery dataset and table
- Apache Airflow

## Steps

### 1. Set Up Google Cloud Resources

1. **Create a GCS bucket** to store the output or temporary data and upload the customers-100.csv file.


### 2. Upload the PySpark Script to GCS
Create the following PySpark script and upload it to your GCS bucket (gs://sandeep-dataproc/dataproc-bigquey.py):

```sh
'''
Author: @Sandeep Sihmar

This script is created to demo below concepts
  1. Create Spark session on Dataproc cluster
  2. Read CSV data from specified GCS bucket
  3. Apply Transformations to group and aggregate data
  4. Write resultant data to BigQuery Table
'''

# Import required modules and packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark session
spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('spark-bigquery-demo') \
  .getOrCreate()

# Define temporary GCS bucket for Dataproc to write its process data
bucket = 'sandeep-dataproc'
spark.conf.set('temporaryGcsBucket', bucket)

# Read data into Spark dataframe from CSV file available in GCS bucket
df = spark.read.option("header", True).csv('gs://sandeep-dataproc/customers-100.csv')

# Since the requirement is to transform the data, let's assume we want to select certain columns
# and perform any necessary transformations.
# For demonstration, let's assume we want to filter customers from a specific country and transform the date format.

# Example transformation: Selecting specific columns and converting date format
transformed_df = df.select(
    col('Index'),
    col('Customer Id'),
    col('First Name'),
    col('Last Name'),
    col('Company'),
    col('City'),
    col('Country'),
    col('Phone 1'),
    col('Phone 2'),
    col('Email'),
    col('Subscription Date'),
    col('Website')
).withColumnRenamed('Subscription Date', 'Subscription_Date')

# You can add any specific transformation logic as per your requirement.
# For now, let's just filter data for customers from a specific country, say 'Chile'.

filtered_df = transformed_df.filter(col('Country') == 'Chile')

# Writing the data to BigQuery
filtered_df.write.format('bigquery') \
    .option('table', 'customer_dataset.customer_data') \
    .option('createDisposition', 'CREATE_IF_NEEDED') \
    .save()

spark.stop()
```

### 3. Create the Airflow DAG
Create the following DAG script and place it in your Airflow DAGs directory:

```sh
# Import statements
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocDeleteClusterOperator, DataprocSubmitJobOperator
from airflow.utils.trigger_rule import TriggerRule

# Custom Python logic for deriving data value
yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

# Default arguments
default_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# DAG definitions
with DAG(
    dag_id='dataproc_spark_bigquery_demo',
    catchup=False,
    schedule_interval=timedelta(days=1),
    default_args=default_args
) as dag:

    # Dummy start task
    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

    # Define the cluster configuration
    cluster_config = {
        "master_config": {
            "num_instances": 1,
            "machine_type_uri": "n1-standard-2",
            "disk_config": {
                "boot_disk_type": "pd-balanced",
                "boot_disk_size_gb": 100
            }
        },
        "software_config": {
            "image_version": "2.2-debian12"
        }
    }

    # Task to create Dataproc cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        cluster_name='first-cluster',
        project_id='techlanders-internal',
        region='us-central1',
        cluster_config=cluster_config,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # Path to the PySpark script on GCS
    GCS_SCRIPT_PATH = 'gs://sandeep-dataproc/dataproc-bigquey.py'

    # Task to submit the PySpark job to Dataproc
    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id='submit_pyspark_job',
        job={
            'reference': {'project_id': 'techlanders-internal'},
            'placement': {'cluster_name': 'first-cluster'},
            'pyspark_job': {'main_python_file_uri': GCS_SCRIPT_PATH}
        },
        region='us-central1',
        project_id='techlanders-internal',
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # Task to delete Dataproc cluster
    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        cluster_name='first-cluster',
        project_id='techlanders-internal',
        region='us-central1',
        trigger_rule=TriggerRule.ALL_DONE
    )

    # Dummy end task
    end = DummyOperator(
        task_id='end',
        dag=dag,
    )

    # Setting up task dependencies
    start >> create_cluster >> submit_pyspark_job >> delete_cluster >> end
```

### 4. Deploy the DAG
Deploy the DAG script to your Airflow environment and ensure it is picked up by the scheduler.

