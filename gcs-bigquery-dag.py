# Import statements
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

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
    dag_id='GCS_to_BQ_and_AGG_TABLE',
    catchup=False,
    schedule_interval=timedelta(days=1),
    default_args=default_args
) as dag:

    # Dummy start task
    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

    # GCS to BigQuery data load Operator and task
    gcs_to_bq_load = GoogleCloudStorageToBigQueryOperator(
        task_id='gcs_to_bq_load',
        bucket='sandeep-dataproc',
        source_objects=['customers-100.csv'],
        destination_project_dataset_table='techlanders-internal.customer_dataset.customers_data',
        schema_fields=[
            {'name': 'Index', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Customer_Id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'First_Name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Last_Name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Company', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'City', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Country', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Phone_1', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Phone_2', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Subscription_Date', 'type': 'DATE', 'mode': 'NULLABLE'},
            {'name': 'Website', 'type': 'STRING', 'mode': 'NULLABLE'}
        ],
        skip_leading_rows=1,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        dag=dag
    )

    # BigQuery task, operator
    create_aggr_bq_table = BigQueryOperator(
        task_id='create_aggr_bq_table',
        use_legacy_sql=False,
        allow_large_results=True,
        sql="""
            CREATE OR REPLACE TABLE techlanders-internal.customer_dataset.customers_data_agg AS
            SELECT
                Country,
                COUNT(Customer_Id) as total_customers,
                COUNT(Email) as total_emails
            FROM techlanders-internal.customer_dataset.customers_data
            GROUP BY
                Country
        """,
        dag=dag
    )

    # Dummy end task
    end = DummyOperator(
        task_id='end',
        dag=dag,
    )

    # Setting up task dependency
    start >> gcs_to_bq_load >> create_aggr_bq_table >> end
