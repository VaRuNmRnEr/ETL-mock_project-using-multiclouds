from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator  # ✅ Fixed import
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from datetime import timedelta

default_args = {
    'owner': 'varun',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'hilton_multicloud_pipeline',
    default_args=default_args,
    description='S3 to Snowflake to GCS ETL',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False
)

# 1️⃣ Load Raw Data from S3 to Snowflake
load_raw = SQLExecuteQueryOperator(
    task_id='load_raw_tables',
    sql="""
        COPY INTO BOOKING_SUMMARY_RAW
        FROM @HILTON_S3_STAGE/booking/
        FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1);

        COPY INTO FEEDBACK_SUMMARY_RAW
        FROM @HILTON_S3_STAGE/feedback/
        FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1);

        COPY INTO PROPERTY_SUMMARY_RAW
        FROM @HILTON_S3_STAGE/property/
        FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1);
    """,
    conn_id='snowflake_conn',  # ✅ parameter is now conn_id, not snowflake_conn_id
    dag=dag
)

# 2️⃣ Clean Data in Snowflake
clean_data = SQLExecuteQueryOperator(
    task_id='clean_data',
    sql="CALL CLEANING_PROCEDURE();",
    conn_id='snowflake_conn',
    dag=dag
)

# 3️⃣ Export Cleaned Data to GCS
export_to_gcs = SQLExecuteQueryOperator(
    task_id='export_to_gcs',
    sql="""
        COPY INTO @HILTON_GCS_STAGE/bookings/
        FROM CLEAN_BOOKING_SUMMARY
        FILE_FORMAT=(TYPE=PARQUET COMPRESSION=SNAPPY)
        HEADER = TRUE
        OVERWRITE=TRUE;

        COPY INTO @HILTON_GCS_STAGE/properties/
        FROM CLEAN_PROPERTY_SUMMARY
        FILE_FORMAT=(TYPE=PARQUET COMPRESSION=SNAPPY)
        HEADER = TRUE
        OVERWRITE=TRUE;

        COPY INTO @HILTON_GCS_STAGE/feedback/
        FROM CLEAN_FEEDBACK_SUMMARY
        FILE_FORMAT=(TYPE=PARQUET COMPRESSION=SNAPPY)
        HEADER = TRUE
        OVERWRITE=TRUE;
    """,
    conn_id='snowflake_conn',
    dag=dag
)

# 4️⃣ Pub/Sub Alert on Failure
alert_failure = PubSubPublishMessageOperator(
    task_id='notify_failure',
    project_id='sm-analytics-gcp-usecase',  # 🔁 Replace with your actual project ID
    topic='hilton-etl-alerts',
    messages=[{'data': b'ETL Pipeline Failed'}],
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)

# Task Dependencies
load_raw >> clean_data >> export_to_gcs
[load_raw, clean_data, export_to_gcs] >> alert_failure
