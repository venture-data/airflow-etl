from airflow import DAG
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime, timedelta
from utils.sql_queries import CALL_DETAILS_MERGE_QUERY

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'call_details_etl',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False
)

export_call_details = MySQLToGCSOperator(
    task_id='export_call_details',
    mysql_conn_id='mysql_default',
    google_cloud_storage_conn_id='google_cloud_default',
    bucket='your-gcs-bucket-name',
    filename='exports/call_details/{{ ds }}/call_details.csv',
    sql='SELECT * FROM call_details',
    dag=dag
)

merge_to_bq = BigQueryExecuteQueryOperator(
    task_id='merge_to_bigquery',
    sql=CALL_DETAILS_MERGE_QUERY,
    use_legacy_sql=False,
    dag=dag
)

export_call_details >> merge_to_bq