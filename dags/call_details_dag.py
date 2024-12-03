from airflow import DAG
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta
from utils.sql_queries import CALL_DETAILS_MERGE_QUERY

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
    'call_details_etl',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1 
)

export_call_details = MySQLToGCSOperator(
    task_id='export_call_details',
    mysql_conn_id='mysql_default',
    gcp_conn_id='google_cloud_default',
    bucket='twilio-airflow',
    filename='exports/call_details/{{ ds }}/call_details.csv',
    sql='select * from `whisper-db`.`call_details`',
    export_format='csv',        # Added this
    field_delimiter=',',        # Added this
    dag=dag
)

load_to_bq = GCSToBigQueryOperator(
    task_id='load_to_bigquery',
    bucket='twilio-airflow',
    source_objects=['exports/call_details/{{ ds }}/call_details.csv'],
    destination_project_dataset_table='video-data-436506.whisper.temp_call_details',
    schema_fields=
    [{'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    {'name': 'phone_number', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'call_sid', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'recording_sid', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'recording_link', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'call_start_time', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
    {'name': 'call_end_time', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
    {'name': 'call_duration', 'type': 'FLOAT', 'mode': 'REQUIRED'},
    {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
    {'name': 'updated_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
    {'name': 'deleted_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}], 
    write_disposition='WRITE_TRUNCATE',
    dag=dag
)

merge_to_bq = BigQueryInsertJobOperator(
    task_id='merge_to_bigquery',
    configuration={
        'query': {
            'query': CALL_DETAILS_MERGE_QUERY,
            'useLegacySql': False,
        }
    },
    dag=dag
)

export_call_details >> load_to_bq >> merge_to_bq