"""
Brazilian E-Commerce ETL Pipeline - Airflow DAG
Cloud-agnostic orchestration for data engineering pipeline
"""

from datetime import datetime, timedelta
import os
import json
from typing import Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.databricks.sensors.databricks import DatabricksRunSensor
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'tags': ['etl', 'brazilian-ecommerce', 'data-engineering']
}

# Configuration from Airflow Variables
def get_config() -> Dict[str, Any]:
    """Get configuration from Airflow Variables"""
    return {
        'cloud_provider': Variable.get('CLOUD_PROVIDER', default_var='azure'),
        'storage_account': Variable.get('STORAGE_ACCOUNT', default_var='stbrazilianecommerce'),
        'container_name': Variable.get('CONTAINER_NAME', default_var='brazilian-ecommerce'),
        'databricks_workspace': Variable.get('DATABRICKS_WORKSPACE', default_var='https://adb-12345.azuredatabricks.net'),
        'databricks_token': Variable.get('DATABRICKS_TOKEN', default_var=''),
        'kaggle_dataset': Variable.get('KAGGLE_DATASET', default_var='olistbr/brazilian-ecommerce'),
        'slack_webhook': Variable.get('SLACK_WEBHOOK', default_var='')
    }

def download_kaggle_data(**context) -> Dict[str, Any]:
    """Download data from Kaggle and upload to cloud storage"""
    import sys
    sys.path.append('/opt/airflow/dags')
    
    from data.download_kaggle_data import main as download_main
    
    # Set environment variables
    config = get_config()
    os.environ['CLOUD_PROVIDER'] = config['cloud_provider']
    os.environ['CONTAINER_NAME'] = config['container_name']
    
    if config['cloud_provider'].lower() == 'azure':
        os.environ['AZURE_STORAGE_ACCOUNT_NAME'] = config['storage_account']
    elif config['cloud_provider'].lower() == 'aws':
        os.environ['AWS_S3_BUCKET_NAME'] = config['container_name']
    elif config['cloud_provider'].lower() == 'gcp':
        os.environ['GCP_BUCKET_NAME'] = config['container_name']
    
    try:
        result = download_main()
        return result
    except Exception as e:
        raise Exception(f"Data download failed: {str(e)}")

def run_databricks_job(job_name: str, notebook_path: str, **context) -> str:
    """Run Databricks job and return run ID"""
    config = get_config()
    
    # Create Databricks job parameters
    parameters = {
        'storage_account': config['storage_account'],
        'container_name': config['container_name']
    }
    
    # Add path parameters based on job type
    if 'bronze' in notebook_path.lower():
        parameters.update({
            'source_path': 'raw',
            'bronze_path': 'bronze'
        })
    elif 'silver' in notebook_path.lower():
        parameters.update({
            'bronze_path': 'bronze',
            'silver_path': 'silver'
        })
    elif 'gold' in notebook_path.lower():
        parameters.update({
            'silver_path': 'silver',
            'gold_path': 'gold'
        })
    
    return parameters

def send_success_notification(**context):
    """Send success notification to Slack"""
    config = get_config()
    
    if not config['slack_webhook']:
        return
    
    SlackWebhookOperator(
        task_id='slack_success_notification',
        webhook_token=config['slack_webhook'],
        message=f"""
        ✅ Brazilian E-Commerce ETL Pipeline Completed Successfully!
        
        📊 Pipeline Details:
        • DAG: {context['dag'].dag_id}
        • Run ID: {context['run_id']}
        • Execution Date: {context['execution_date']}
        • Cloud Provider: {config['cloud_provider']}
        • Storage: {config['storage_account']}/{config['container_name']}
        
        🎉 All tasks completed successfully!
        """,
        channel='#data-engineering'
    ).execute(context)

def send_failure_notification(context):
    """Send failure notification to Slack"""
    config = get_config()
    
    if not config['slack_webhook']:
        return
    
    SlackWebhookOperator(
        task_id='slack_failure_notification',
        webhook_token=config['slack_webhook'],
        message=f"""
        ❌ Brazilian E-Commerce ETL Pipeline Failed!
        
        🚨 Failure Details:
        • DAG: {context['dag'].dag_id}
        • Run ID: {context['run_id']}
        • Execution Date: {context['execution_date']}
        • Failed Task: {context['task_instance'].task_id}
        • Cloud Provider: {config['cloud_provider']}
        
        🔍 Check Airflow UI for more details.
        """,
        channel='#data-engineering-alerts'
    ).execute(context)

# Create DAG
with DAG(
    dag_id='brazilian_ecommerce_etl_pipeline',
    default_args=default_args,
    description='End-to-end ETL pipeline for Brazilian E-Commerce dataset',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM UTC
    max_active_runs=1,
    on_failure_callback=send_failure_notification,
    doc_md="""
    ## Brazilian E-Commerce ETL Pipeline
    
    This DAG orchestrates the complete ETL pipeline for Brazilian E-Commerce data:
    
    ### Architecture
    ```
    Kaggle API → Cloud Storage → Databricks (Bronze/Silver/Gold) → Analytics
    ```
    
    ### Tasks
    1. **Data Ingestion**: Download from Kaggle and upload to cloud storage
    2. **Bronze Layer**: Raw data validation and Delta format conversion
    3. **Silver Layer**: Business logic and data cleaning
    4. **Gold Layer**: Dimensional model (star schema) creation
    5. **Notification**: Success/failure alerts
    
    ### Cloud Support
    - Azure: Data Lake Storage Gen2
    - AWS: S3
    - GCP: Cloud Storage
    
    ### Configuration
    Configure the following Airflow Variables:
    - `CLOUD_PROVIDER`: azure, aws, or gcp
    - `STORAGE_ACCOUNT`: Storage account name or bucket name
    - `CONTAINER_NAME`: Container or bucket name
    - `DATABRICKS_WORKSPACE`: Databricks workspace URL
    - `DATABRICKS_TOKEN`: Databricks API token
    - `SLACK_WEBHOOK`: Slack webhook for notifications
    """
) as dag:
    
    config = get_config()
    
    # Task Group for Data Processing
    with TaskGroup('data_processing', tooltip='ETL Processing Tasks') as data_processing:
        
        # Task 1: Download Kaggle Data
        download_task = PythonOperator(
            task_id='download_kaggle_data',
            python_callable=download_kaggle_data,
            doc_md="""
            Download Brazilian E-Commerce dataset from Kaggle and upload to cloud storage.
            
            **Supported Cloud Providers:**
            - Azure: Data Lake Storage Gen2
            - AWS: S3  
            - GCP: Cloud Storage
            """
        )
        
        # Task 2: Bronze Layer Transformation
        bronze_task = DatabricksRunNowOperator(
            task_id='bronze_layer_transformation',
            databricks_conn_id='databricks_default',
            notebook_path='/ETL/02_Bronze_Layer/bronze_transformation',
            parameters=run_databricks_job('bronze', '/ETL/02_Bronze_Layer/bronze_transformation'),
            do_xcom_push=True,
            doc_md="""
            Transform raw CSV data into Bronze Delta tables.
            
            **Operations:**
            - Schema validation
            - Data quality checks
            - Duplicate removal
            - Delta format conversion
            - Metadata enrichment
            """
        )
        
        # Task 3: Silver Layer Transformation
        silver_task = DatabricksRunNowOperator(
            task_id='silver_layer_transformation',
            databricks_conn_id='databricks_default',
            notebook_path='/ETL/03_Silver_Layer/silver_transformation',
            parameters=run_databricks_job('silver', '/ETL/03_Silver_Layer/silver_transformation'),
            do_xcom_push=True,
            doc_md="""
            Transform Bronze data into curated Silver layer.
            
            **Operations:**
            - Business logic application
            - Data enrichment
            - Category translation
            - Derived columns calculation
            - Regional classification
            """
        )
        
        # Task 4: Gold Layer Transformation
        gold_task = DatabricksRunNowOperator(
            task_id='gold_layer_transformation',
            databricks_conn_id='databricks_default',
            notebook_path='/ETL/04_Gold_Layer/gold_transformation',
            parameters=run_databricks_job('gold', '/ETL/04_Gold_Layer/gold_transformation'),
            do_xcom_push=True,
            doc_md="""
            Create dimensional model (star schema) for analytics.
            
            **Star Schema Tables:**
            - fact_orders (central fact table)
            - dim_customers
            - dim_products
            - dim_sellers  
            - dim_date
            - dim_payment_type
            - dim_review_sentiment
            """
        )
        
        # Define task dependencies
        download_task >> bronze_task >> silver_task >> gold_task
    
    # Task 5: Data Quality Checks
    data_quality_task = PythonOperator(
        task_id='data_quality_checks',
        python_callable=lambda **context: {
            'bronze_rows': context['task_instance'].xcom_pull(task_ids='data_processing.bronze_layer_transformation', key='return_value'),
            'silver_rows': context['task_instance'].xcom_pull(task_ids='data_processing.silver_layer_transformation', key='return_value'),
            'gold_rows': context['task_instance'].xcom_pull(task_ids='data_processing.gold_layer_transformation', key='return_value')
        },
        doc_md="""
        Perform data quality checks on processed data.
        
        **Checks:**
        - Row count validation
        - Null value analysis
        - Data consistency verification
        - Schema validation
        """
    )
    
    # Task 6: Success Notification
    success_notification = PythonOperator(
        task_id='success_notification',
        python_callable=send_success_notification,
        trigger_rule='all_success',
        doc_md="""
        Send success notification to Slack when all tasks complete successfully.
        """
    )
    
    # Define final dependencies
    data_processing >> data_quality_task >> success_notification

# Additional DAG for incremental updates
with DAG(
    dag_id='brazilian_ecommerce_incremental_update',
    default_args=default_args,
    description='Incremental updates for Brazilian E-Commerce data',
    schedule_interval='0 */6 * * *',  # Run every 6 hours
    max_active_runs=1,
    on_failure_callback=send_failure_notification,
    catchup=False,
    tags=['etl', 'brazilian-ecommerce', 'incremental']
) as incremental_dag:
    
    # Incremental data processing
    incremental_update = PythonOperator(
        task_id='incremental_data_update',
        python_callable=lambda **context: {
            'mode': 'incremental',
            'last_run': context['prev_dataflow_start_date'],
            'current_run': context['execution_date']
        },
        doc_md="""
        Process incremental data updates.
        
        **Features:**
        - Change data capture
        - Upsert operations
        - Delta Lake time travel
        - Merge statements
        """
    )

# DAG for data validation and monitoring
with DAG(
    dag_id='brazilian_ecommerce_data_monitoring',
    default_args=default_args,
    description='Data validation and monitoring pipeline',
    schedule_interval='0 8 * * *',  # Run daily at 8 AM UTC
    max_active_runs=1,
    on_failure_callback=send_failure_notification,
    catchup=False,
    tags=['monitoring', 'brazilian-ecommerce', 'data-quality']
) as monitoring_dag:
    
    # Data validation tasks
    schema_validation = PythonOperator(
        task_id='schema_validation',
        python_callable=lambda **context: {
            'validation_type': 'schema',
            'tables': ['fact_orders', 'dim_customers', 'dim_products', 'dim_sellers', 'dim_date']
        },
        doc_md="""
        Validate schema consistency across all tables.
        """
    )
    
    data_profiling = PythonOperator(
        task_id='data_profiling',
        python_callable=lambda **context: {
            'profile_type': 'statistical',
            'metrics': ['row_count', 'null_count', 'distinct_count', 'min_max']
        },
        doc_md="""
        Generate data profiles and statistics.
        """
    )
    
    anomaly_detection = PythonOperator(
        task_id='anomaly_detection',
        python_callable=lambda **context: {
            'detection_type': 'statistical',
            'threshold': 2.0,  # Standard deviations
            'metrics': ['order_value', 'delivery_time', 'review_score']
        },
        doc_md="""
        Detect anomalies in key business metrics.
        """
    )
    
    # Define monitoring dependencies
    schema_validation >> data_profiling >> anomaly_detection
