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
