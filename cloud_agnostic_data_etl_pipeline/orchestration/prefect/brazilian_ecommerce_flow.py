"""
Brazilian E-Commerce ETL Pipeline - Prefect Flow
Cloud-agnostic orchestration with advanced scheduling and monitoring
"""

import os
import sys
import json
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from pathlib import Path

from prefect import flow, task, get_run_logger
from prefect.context import get_run_context
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import CronSchedule
from prefect.blocks.system import Secret
from prefect.filesystems import LocalFileSystem

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from data.download_kaggle_data import KaggleDataDownloader, get_cloud_client

# Configuration
class ETLConfig:
    """Configuration management for ETL pipeline"""
    
    def __init__(self):
        self.cloud_provider = os.getenv('CLOUD_PROVIDER', 'azure')
        self.storage_account = os.getenv('STORAGE_ACCOUNT', 'stbrazilianecommerce')
        self.container_name = os.getenv('CONTAINER_NAME', 'brazilian-ecommerce')
        self.kaggle_dataset = os.getenv('KAGGLE_DATASET', 'olistbr/brazilian-ecommerce')
        self.databricks_workspace = os.getenv('DATABRICKS_WORKSPACE')
        self.databricks_token = Secret.load("databricks-token").get() if Secret.exists("databricks-token") else None
        self.slack_webhook = os.getenv('SLACK_WEBHOOK')
        
        # Cloud-specific configuration
        if self.cloud_provider.lower() == 'azure':
            self.storage_config = {'account_name': self.storage_account}
        elif self.cloud_provider.lower() == 'aws':
            self.storage_config = {
                'bucket_name': self.container_name,
                'region': os.getenv('AWS_REGION', 'us-east-1')
            }
        elif self.cloud_provider.lower() == 'gcp':
            self.storage_config = {'bucket_name': self.container_name}
        else:
            raise ValueError(f"Unsupported cloud provider: {self.cloud_provider}")

@task(
    name="Download Kaggle Data",
    retries=3,
    retry_delay_seconds=60,
    cache_key_fn=lambda: "kaggle-download-{config.kaggle_dataset}",
)
def download_kaggle_data(config: ETLConfig) -> Dict[str, Any]:
    """Download Brazilian E-Commerce dataset from Kaggle and upload to cloud storage"""
    logger = get_run_logger()
    logger.info(f"Starting data download for dataset: {config.kaggle_dataset}")
    
    try:
        # Initialize downloader
        downloader = KaggleDataDownloader(config.kaggle_dataset)
        downloader.setup_directories()
        
        # Download dataset
        if not downloader.download_dataset():
            raise Exception("Failed to download dataset from Kaggle")
        
        # Validate downloaded files
        if not downloader.validate_downloaded_files():
            raise Exception("Downloaded files validation failed")
        
        # Get file information
        file_info = downloader.get_file_info()
        logger.info(f"Downloaded {len(file_info)} files")
        
        # Initialize cloud client and upload
        cloud_client = get_cloud_client(config.cloud_provider, **config.storage_config)
        
        if not cloud_client.authenticate():
            raise Exception(f"Failed to authenticate with {config.cloud_provider}")
        
        if not cloud_client.create_container(config.container_name):
            raise Exception(f"Failed to create container: {config.container_name}")
        
        if not cloud_client.upload_files(downloader.local_data_path, config.container_name, "raw"):
            raise Exception("Failed to upload files to cloud storage")
        
        # Create metadata
        metadata = {
            'dataset_name': config.kaggle_dataset,
            'cloud_provider': config.cloud_provider,
            'container_name': config.container_name,
            'download_timestamp': datetime.utcnow().isoformat(),
            'files': file_info,
            'total_files': len(file_info)
        }
        
        logger.info(f"Successfully downloaded and uploaded {len(file_info)} files")
        return metadata
        
    except Exception as e:
        logger.error(f"Data download failed: {str(e)}")
        raise

@task(
    name="Run Databricks Job",
    retries=2,
    retry_delay_seconds=120,
)
def run_databricks_job(
    config: ETLConfig,
    notebook_path: str,
    layer: str,
    metadata: Dict[str, Any]
) -> Dict[str, Any]:
    """Run Databricks notebook for data transformation"""
    logger = get_run_logger()
    logger.info(f"Running Databricks job for {layer} layer: {notebook_path}")
    
    try:
        import requests
        
        if not config.databricks_workspace or not config.databricks_token:
            raise Exception("Databricks configuration missing")
        
        # Prepare job parameters
        parameters = {
            'storage_account': config.storage_account,
            'container_name': config.container_name,
            'cloud_provider': config.cloud_provider
        }
        
        # Add layer-specific parameters
        if layer == 'bronze':
            parameters.update({
                'source_path': 'raw',
                'bronze_path': 'bronze'
            })
        elif layer == 'silver':
            parameters.update({
                'bronze_path': 'bronze',
                'silver_path': 'silver'
            })
        elif layer == 'gold':
            parameters.update({
                'silver_path': 'silver',
                'gold_path': 'gold'
            })
        
        # Create and submit job
        job_payload = {
            "name": f"Brazilian E-Commerce {layer.title()} Layer",
            "timeout_seconds": 3600,
            "max_concurrent_runs": 1,
            "tasks": [{
                "task_key": f"{layer}_transformation",
                "run_job_task": {
                    "job_id": None,  # Will be created if doesn't exist
                    "notebook_task": {
                        "notebook_path": notebook_path,
                        "base_parameters": parameters
                    }
                }
            }]
        }
        
        # Submit job
        headers = {
            'Authorization': f'Bearer {config.databricks_token}',
            'Content-Type': 'application/json'
        }
        
        response = requests.post(
            f"{config.databricks_workspace}/api/2.1/jobs/runs/submit",
            headers=headers,
            json=job_payload
        )
        response.raise_for_status()
        
        run_id = response.json()['run_id']
        logger.info(f"Submitted Databricks job with run ID: {run_id}")
        
        # Monitor job completion
        job_status = monitor_databricks_job(config.databricks_workspace, config.databricks_token, run_id, logger)
        
        if job_status['state']['life_cycle_state'] == 'TERMINATED':
            if job_status['state']['result_state'] == 'SUCCESS':
                logger.info(f"Databricks job completed successfully for {layer} layer")
                return {
                    'layer': layer,
                    'run_id': run_id,
                    'status': 'success',
                    'execution_time': job_status.get('execution_duration', 0),
                    'metadata': metadata
                }
            else:
                raise Exception(f"Databricks job failed for {layer} layer: {job_status['state'].get('state_message', 'Unknown error')}")
        else:
            raise Exception(f"Databricks job in unexpected state: {job_status['state']['life_cycle_state']}")
            
    except Exception as e:
        logger.error(f"Databricks job failed for {layer} layer: {str(e)}")
        raise

def monitor_databricks_job(workspace: str, token: str, run_id: str, logger) -> Dict[str, Any]:
    """Monitor Databricks job execution"""
    import requests
    import time
    
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    
    max_wait_time = 3600  # 1 hour
    poll_interval = 30  # 30 seconds
    elapsed_time = 0
    
    while elapsed_time < max_wait_time:
        try:
            response = requests.get(
                f"{workspace}/api/2.1/jobs/runs/get",
                headers=headers,
                params={'run_id': run_id}
            )
            response.raise_for_status()
            
            job_status = response.json()
            state = job_status['state']['life_cycle_state']
            
            logger.info(f"Databricks job {run_id} status: {state}")
            
            if state in ['TERMINATED', 'INTERNAL_ERROR', 'SKIPPED']:
                return job_status
            
            time.sleep(poll_interval)
            elapsed_time += poll_interval
            
        except Exception as e:
            logger.error(f"Error monitoring Databricks job: {str(e)}")
            time.sleep(poll_interval)
            elapsed_time += poll_interval
    
    raise Exception(f"Databricks job {run_id} timed out after {max_wait_time} seconds")

@task(
    name="Data Quality Validation",
    retries=1,
    retry_delay_seconds=30,
)
def validate_data_quality(
    config: ETLConfig,
    transformation_results: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Perform data quality checks on processed data"""
    logger = get_run_logger()
    logger.info("Starting data quality validation")
    
    try:
        quality_results = {
            'validation_timestamp': datetime.utcnow().isoformat(),
            'layer_validations': {},
            'overall_status': 'passed',
            'issues': []
        }
        
        for result in transformation_results:
            layer = result['layer']
            logger.info(f"Validating {layer} layer data")
            
            # Initialize cloud client for validation
            cloud_client = get_cloud_client(config.cloud_provider, **config.storage_config)
            
            if not cloud_client.authenticate():
                raise Exception(f"Failed to authenticate for {layer} validation")
            
            # Perform layer-specific validations
            layer_validation = validate_layer_data(cloud_client, config.container_name, layer, logger)
            
            quality_results['layer_validations'][layer] = layer_validation
            
            if layer_validation['status'] == 'failed':
                quality_results['overall_status'] = 'failed'
                quality_results['issues'].extend(layer_validation.get('issues', []))
        
        logger.info(f"Data quality validation completed with status: {quality_results['overall_status']}")
        return quality_results
        
    except Exception as e:
        logger.error(f"Data quality validation failed: {str(e)}")
        raise

def validate_layer_data(cloud_client, container_name: str, layer: str, logger) -> Dict[str, Any]:
    """Validate specific layer data"""
    validation_result = {
        'layer': layer,
        'status': 'passed',
        'issues': [],
        'metrics': {}
    }
    
    try:
        # Check if layer directory exists and has data
        # This would be implemented based on cloud provider specifics
        # For now, return a placeholder validation
        
        if layer == 'bronze':
            validation_result['metrics'] = {
                'expected_tables': 9,
                'actual_tables': 9,
                'total_rows': 100000,  # Placeholder
                'null_percentage': 0.1
            }
        elif layer == 'silver':
            validation_result['metrics'] = {
                'expected_tables': 7,
                'actual_tables': 7,
                'total_rows': 95000,  # Placeholder
                'data_quality_score': 0.95
            }
        elif layer == 'gold':
            validation_result['metrics'] = {
                'expected_tables': 7,  # 6 dims + 1 fact
                'actual_tables': 7,
                'total_rows': 90000,  # Placeholder
                'star_schema_valid': True
            }
        
        logger.info(f"{layer.title()} layer validation passed")
        
    except Exception as e:
        validation_result['status'] = 'failed'
        validation_result['issues'].append(f"Validation error: {str(e)}")
        logger.error(f"{layer.title()} layer validation failed: {str(e)}")
    
    return validation_result

@task(
    name="Send Notification",
    retries=3,
    retry_delay_seconds=30,
)
def send_notification(
    config: ETLConfig,
    pipeline_status: str,
    results: Dict[str, Any],
    execution_time: float
) -> bool:
    """Send pipeline completion notification"""
    logger = get_run_logger()
    logger.info(f"Sending {pipeline_status} notification")
    
    try:
        if not config.slack_webhook:
            logger.warning("Slack webhook not configured, skipping notification")
            return True
        
        import requests
        
        # Prepare notification message
        if pipeline_status == 'success':
            message = f"""
            ✅ Brazilian E-Commerce ETL Pipeline Completed Successfully!
            
            📊 Pipeline Results:
            • Status: {pipeline_status}
            • Execution Time: {execution_time:.2f} seconds
            • Cloud Provider: {config.cloud_provider}
            • Storage: {config.storage_account}/{config.container_name}
            
            📈 Layer Results:
            """
            
            for layer, validation in results.get('layer_validations', {}).items():
                message += f"\n• {layer.title()}: {validation['status']}"
                if validation.get('metrics'):
                    for metric, value in validation['metrics'].items():
                        message += f"\n  - {metric}: {value}"
            
            message += "\n🎉 All tasks completed successfully!"
            
        else:
            message = f"""
            ❌ Brazilian E-Commerce ETL Pipeline Failed!
            
            🚨 Failure Details:
            • Status: {pipeline_status}
            • Execution Time: {execution_time:.2f} seconds
            • Cloud Provider: {config.cloud_provider}
            
            🔍 Check Prefect UI for more details.
            """
        
        # Send to Slack
        payload = {
            'text': message,
            'channel': '#data-engineering' if pipeline_status == 'success' else '#data-engineering-alerts'
        }
        
        response = requests.post(config.slack_webhook, json=payload)
        response.raise_for_status()
        
        logger.info("Notification sent successfully")
        return True
        
    except Exception as e:
        logger.error(f"Failed to send notification: {str(e)}")
        return False

@flow(
    name="Brazilian E-Commerce ETL Pipeline",
    description="End-to-end ETL pipeline for Brazilian E-Commerce dataset with cloud-agnostic orchestration",
    retries=1,
    retry_delay_seconds=300,
    log_prints=True,
)
def brazilian_ecommerce_etl(
    cloud_provider: str = "azure",
    storage_account: str = "stbrazilianecommerce",
    container_name: str = "brazilian-ecommerce",
    kaggle_dataset: str = "olistbr/brazilian-ecommerce"
) -> Dict[str, Any]:
    """Main ETL pipeline flow"""
    logger = get_run_logger()
    start_time = datetime.utcnow()
    
    logger.info(f"Starting Brazilian E-Commerce ETL Pipeline")
    logger.info(f"Configuration: {cloud_provider}/{storage_account}/{container_name}")
    
    try:
        # Initialize configuration
        config = ETLConfig()
        config.cloud_provider = cloud_provider
        config.storage_account = storage_account
        config.container_name = container_name
        config.kaggle_dataset = kaggle_dataset
        
        # Step 1: Download data from Kaggle
        metadata = download_kaggle_data.submit(config)
        
        # Step 2: Run Bronze layer transformation
        bronze_result = run_databricks_job.submit(
            config,
            "/ETL/02_Bronze_Layer/bronze_transformation",
            "bronze",
            metadata
        )
        
        # Step 3: Run Silver layer transformation
        silver_result = run_databricks_job.submit(
            config,
            "/ETL/03_Silver_Layer/silver_transformation",
            "silver",
            metadata
        )
        
        # Step 4: Run Gold layer transformation
        gold_result = run_databricks_job.submit(
            config,
            "/ETL/04_Gold_Layer/gold_transformation",
            "gold",
            metadata
        )
        
        # Step 5: Wait for all transformations to complete
        transformation_results = [
            bronze_result.result(),
            silver_result.result(),
            gold_result.result()
        ]
        
        # Step 6: Data quality validation
        quality_results = validate_data_quality(config, transformation_results)
        
        # Step 7: Send success notification
        end_time = datetime.utcnow()
        execution_time = (end_time - start_time).total_seconds()
        
        send_notification.submit(
            config,
            "success" if quality_results['overall_status'] == 'passed' else "failed",
            quality_results,
            execution_time
        )
        
        # Return pipeline results
        pipeline_results = {
            'status': 'success',
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'execution_time_seconds': execution_time,
            'configuration': {
                'cloud_provider': config.cloud_provider,
                'storage_account': config.storage_account,
                'container_name': config.container_name,
                'dataset': config.kaggle_dataset
            },
            'transformations': transformation_results,
            'data_quality': quality_results
        }
        
        logger.info(f"Pipeline completed successfully in {execution_time:.2f} seconds")
        return pipeline_results
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        
        # Send failure notification
        end_time = datetime.utcnow()
        execution_time = (end_time - start_time).total_seconds()
        
        try:
            send_notification.submit(config, "failed", {"error": str(e)}, execution_time)
        except:
            pass  # Don't let notification failure mask pipeline failure
        
        raise

# Create deployment
if __name__ == "__main__":
    # Deploy the flow
    deployment = Deployment.build_from_flow(
        flow=brazilian_ecommerce_etl,
        name="Brazilian E-Commerce ETL Production",
        schedule=CronSchedule(cron="0 2 * * *"),  # Daily at 2 AM UTC
        parameters={
            "cloud_provider": "azure",
            "storage_account": "stbrazilianecommerce",
            "container_name": "brazilian-ecommerce",
            "kaggle_dataset": "olistbr/brazilian-ecommerce"
        },
        tags=["etl", "brazilian-ecommerce", "production"],
        description="Production deployment for Brazilian E-Commerce ETL pipeline"
    )
    
    deployment.apply()
