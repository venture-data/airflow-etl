# dags/keyword_processing_dag.py
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG(
    'keyword_processing_etl',
    default_args=default_args,
    schedule_interval='*/1 * * * *',  # Every minute
    catchup=False,
    max_active_runs=1
)

# Wait for calls_analysis_etl to complete
wait_for_calls_analysis = ExternalTaskSensor(
    task_id='wait_for_calls_analysis',
    external_dag_id='calls_analysis_etl',
    external_task_id='merge_to_bigquery',
    mode='reschedule',
    timeout=600,  # 10 minutes
    allowed_states=['success'],
    failed_states=['failed', 'skipped'],
    dag=dag
)

process_keywords = BigQueryInsertJobOperator(
    task_id='process_keywords',
    configuration={
        'query': {
            'query': """
            WITH cleaned_data AS (
                SELECT 
                    id,
                    TRIM(REPLACE(REPLACE(keywords, '"', ''), "'", '')) as keywords,
                    overall_sentiment
                FROM `video-data-436506.whisper.calls_analysis`
                WHERE keywords IS NOT NULL AND keywords != ''
                )

                SELECT 
                id,
                TRIM(keyword) as keyword,
                overall_sentiment
                FROM cleaned_data,
                UNNEST(SPLIT(keywords, ',')) as keyword
                WHERE TRIM(keyword) != ''
                ORDER BY id, keyword
            """,
            'destinationTable': {
                'projectId': 'video-data-436506',
                'datasetId': 'schedule_data',
                'tableId': 'keyword_data'
            },
            'writeDisposition': 'WRITE_TRUNCATE',
            'useLegacySql': False
        }
    },
    dag=dag
)

wait_for_calls_analysis >> process_keywords