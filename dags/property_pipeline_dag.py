from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os

PROJECT_PATH = '/Users/wenditang/Desktop/sydney-property-pipeline'  
sys.path.insert(0, PROJECT_PATH)


from src.data_loader import DataLoader
from src.etl_pipeline import ETLPipeline

# Default arguments
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'property_data_pipeline',
    default_args=default_args,
    description='Sydney property market data pipeline',
    schedule_interval='@weekly',  
    catchup=False,
    tags=['property', 'etl', 'data-engineering'],
)


def load_raw_data(**context):
    """Task 1: Load raw data"""
    loader = DataLoader()
    df = loader.load_raw_data()
    loader.explore_data(df)
    df_clean = loader.clean_data(df)
    filepath = loader.save_processed_data(df_clean)
    
    # Push filepath to XCom for next task
    context['ti'].xcom_push(key='processed_file', value=filepath)
    
    return f"Loaded {len(df_clean)} records"


def run_etl(**context):
    """Task 2: Run ETL pipeline"""
    pipeline = ETLPipeline()
    
    try:
        # Extract
        df_raw = pipeline.extract_from_raw()
        
        # Transform
        df_transformed = pipeline.transform_data(df_raw)
        
        # Load
        pipeline.load_to_processed(df_transformed)
        
        # Quality checks
        checks_passed = pipeline.run_data_quality_checks()
        
        if not checks_passed:
            raise ValueError("Data quality checks failed!")
        
        return f"ETL complete: {len(df_transformed)} records processed"
        
    finally:
        pipeline.close()


def generate_summary(**context):
    """Task 3: Generate summary statistics"""
    pipeline = ETLPipeline()
    
    try:
        pipeline.get_summary_stats()
        return "Summary generated"
    finally:
        pipeline.close()


# Define tasks
task_load = PythonOperator(
    task_id='load_raw_data',
    python_callable=load_raw_data,
    provide_context=True,
    dag=dag,
)

task_etl = PythonOperator(
    task_id='run_etl_pipeline',
    python_callable=run_etl,
    provide_context=True,
    dag=dag,
)

task_summary = PythonOperator(
    task_id='generate_summary',
    python_callable=generate_summary,
    provide_context=True,
    dag=dag,
)

task_notify = BashOperator(
    task_id='notify_completion',
    bash_command='echo "Pipeline completed successfully at $(date)"',
    dag=dag,
)

# Set task dependencies
task_load >> task_etl >> task_summary >> task_notify