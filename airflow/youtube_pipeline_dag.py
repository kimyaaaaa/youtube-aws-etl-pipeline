from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
import sys

# Helper location
sys.path.append('/home/kiwilytics/airflow/helpers')

from file_detector import get_new_files, update_processed_files


# -------------------------
# Detect new files
# -------------------------

def detect_files():
    return get_new_files()


# -------------------------
# Save processed registry
# -------------------------

def save_files(**context):

    files = context['ti'].xcom_pull(
        task_ids='detect_new_files'
    )

    update_processed_files(files)


# -------------------------
# DAG
# -------------------------

with DAG(
    dag_id='youtube_etl_pipeline',
    start_date=datetime(2025,1,1),
    schedule='@daily',
    catchup=False,
    max_active_runs=1
) as dag:


    # -------------------------
    # Detect files
    # -------------------------

    detect_new_files = PythonOperator(
        task_id='detect_new_files',
        python_callable=detect_files
    )


    # -------------------------
    # Clean videos
    # -------------------------

    clean_videos = GlueJobOperator.partial(
        task_id='clean_videos',
        job_name='clean_videos_job',
        wait_for_completion=True
    ).expand(
        script_args=detect_new_files.output.map(
            lambda x: {
                '--COUNTRY': x['country']
            }
        )
    )


    # -------------------------
    # Build category dimension
    # -------------------------

    build_category = GlueJobOperator.partial(
        task_id='build_category',
        job_name='build_category_dimension',
        wait_for_completion=True
    ).expand(
        script_args=detect_new_files.output.map(
            lambda x: {
                '--COUNTRY': x['country'],
                '--INPUT_PATH':
                f"s3://youtube-etl-project-769044546127-eu-north-1-an/raw/categories/country={x['country']}/"
            }
        )
    )


    # -------------------------
    # Build fact table
    # -------------------------

    build_fact = GlueJobOperator.partial(
        task_id='build_fact',
        job_name='build_fact_trending',
        wait_for_completion=True
    ).expand(
        script_args=detect_new_files.output.map(
            lambda x: {
                '--COUNTRY': x['country']
            }
        )
    )


    # -------------------------
    # Save registry
    # -------------------------

    save_processed = PythonOperator(
        task_id='save_processed',
        python_callable=save_files
    )


    # -------------------------
    # Workflow
    # -------------------------

    detect_new_files >> clean_videos
    clean_videos >> build_category
    build_category >> build_fact
    build_fact >> save_processed
