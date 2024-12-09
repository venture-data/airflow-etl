from airflow import DAG
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta
from utils.sql_queries import CALLS_STATUS_MERGE_QUERY

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,  # Added this
    'email_on_failure': False, # Added this
    'email_on_retry': False    # Added this
}

dag = DAG(
    'calls_status_etl',
    default_args=default_args,
    schedule_interval='*/1 * * * *',
    catchup=False,
    max_active_runs=1 
)

export_calls_analysis = MySQLToGCSOperator(
    task_id='export_calls_status',
    mysql_conn_id='mysql_default',
    gcp_conn_id='google_cloud_default',
    bucket='twilio-airflow',
    filename='exports/calls_status/{{ ds }}/calls_status.csv',
    sql='SELECT * FROM `whisper-db`.`calls_status`',
    export_format='csv',        # Added this
    field_delimiter=',',        # Added this
    dag=dag
)

load_to_bq = GCSToBigQueryOperator(
    task_id='load_to_bigquery',
    bucket='twilio-airflow',
    source_objects=['exports/calls_status/{{ ds }}/calls_status.csv'],
    destination_project_dataset_table='video-data-436506.whisper.temp_calls_status',
    schema_fields=[
        {'name': 'call_sid', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'status', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'call_from', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'call_to', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'updated_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'deleted_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_TRUNCATE',
    dag=dag
)

merge_to_bq = BigQueryInsertJobOperator(
    task_id='merge_to_bigquery',
    configuration={
        'query': {
            'query': CALLS_STATUS_MERGE_QUERY,
            'useLegacySql': False,
        }
    },
    dag=dag
)
export_calls_analysis >> load_to_bq >> merge_to_bq