#!/usr/bin/env python3
"""
Cloud-Agnostic Data Ingestion Script for Brazilian E-Commerce Dataset
Downloads Kaggle dataset and uploads to configurable cloud storage (Azure, AWS, GCP)
"""

import os
import sys
import zipfile
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
import json
from datetime import datetime
from abc import ABC, abstractmethod

import pandas as pd
import kaggle

# Cloud-specific imports
try:
    from azure.storage.filedatalake import DataLakeServiceClient
    from azure.identity import DefaultAzureCredential
    from azure.core.exceptions import AzureError
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

try:
    import boto3
    from botocore.exceptions import ClientError
    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False

try:
    from google.cloud import storage
    from google.cloud.exceptions import GoogleCloudError
    GCP_AVAILABLE = True
except ImportError:
    GCP_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_ingestion.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class CloudStorageClient(ABC):
    """Abstract base class for cloud storage clients"""
    
    @abstractmethod
    def authenticate(self) -> bool:
        """Authenticate with the cloud provider"""
        pass
    
    @abstractmethod
    def create_container(self, container_name: str) -> bool:
        """Create container/bucket if it doesn't exist"""
        pass
    
    @abstractmethod
    def upload_file(self, local_path: Path, container_name: str, remote_path: str) -> bool:
        """Upload a single file to cloud storage"""
        pass
    
    @abstractmethod
    def upload_files(self, local_path: Path, container_name: str, remote_path: str) -> bool:
        """Upload all files from local directory to cloud storage"""
        pass

class AzureDataLakeClient(CloudStorageClient):
    """Azure Data Lake Storage client implementation"""
    
    def __init__(self, account_name: str):
        self.account_name = account_name
        self.service_client = None
        
        if not AZURE_AVAILABLE:
            raise ImportError("Azure storage libraries not installed. Install with: pip install azure-storage-file-datalake azure-identity")
    
    def authenticate(self) -> bool:
        try:
            account_url = f"https://{self.account_name}.dfs.core.windows.net"
            self.service_client = DataLakeServiceClient(
                account_url=account_url,
                credential=DefaultAzureCredential()
            )
            logger.info("Successfully authenticated with Azure Data Lake")
            return True
        except AzureError as e:
            logger.error(f"Azure authentication failed: {str(e)}")
            return False
    
    def create_container(self, container_name: str) -> bool:
        try:
            file_system_client = self.service_client.get_file_system_client(container_name)
            file_system_client.create_file_system()
            logger.info(f"Created file system: {container_name}")
        except AzureError as e:
            if "already exists" in str(e):
                logger.info(f"File system {container_name} already exists")
            else:
                logger.error(f"Failed to create file system: {str(e)}")
                return False
        return True
    
    def upload_file(self, local_path: Path, container_name: str, remote_path: str) -> bool:
        try:
            file_system_client = self.service_client.get_file_system_client(container_name)
            directory_client = file_system_client.get_directory_client(remote_path)
            file_client = directory_client.create_file(local_path.name)
            
            with open(local_path, 'rb') as data:
                file_client.upload_data(data, overwrite=True)
            
            logger.info(f"Successfully uploaded {local_path.name}")
            return True
        except AzureError as e:
            logger.error(f"Failed to upload {local_path.name}: {str(e)}")
            return False
    
    def upload_files(self, local_path: Path, container_name: str, remote_path: str) -> bool:
        success = True
        for file_path in local_path.glob("*.csv"):
            if not self.upload_file(file_path, container_name, remote_path):
                success = False
        return success

class S3Client(CloudStorageClient):
    """AWS S3 client implementation"""
    
    def __init__(self, bucket_name: str, region: str = 'us-east-1'):
        self.bucket_name = bucket_name
        self.region = region
        self.s3_client = None
        
        if not AWS_AVAILABLE:
            raise ImportError("AWS libraries not installed. Install with: pip install boto3")
    
    def authenticate(self) -> bool:
        try:
            self.s3_client = boto3.client('s3', region_name=self.region)
            # Test connection by listing buckets
            self.s3_client.list_buckets()
            logger.info("Successfully authenticated with AWS S3")
            return True
        except ClientError as e:
            logger.error(f"AWS authentication failed: {str(e)}")
            return False
    
    def create_container(self, container_name: str) -> bool:
        try:
            self.s3_client.create_bucket(Bucket=container_name)
            logger.info(f"Created bucket: {container_name}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'BucketAlreadyExists':
                logger.info(f"Bucket {container_name} already exists")
            else:
                logger.error(f"Failed to create bucket: {str(e)}")
                return False
        return True
    
    def upload_file(self, local_path: Path, container_name: str, remote_path: str) -> bool:
        try:
            s3_key = f"{remote_path}/{local_path.name}"
            self.s3_client.upload_file(str(local_path), container_name, s3_key)
            logger.info(f"Successfully uploaded {local_path.name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to upload {local_path.name}: {str(e)}")
            return False
    
    def upload_files(self, local_path: Path, container_name: str, remote_path: str) -> bool:
        success = True
        for file_path in local_path.glob("*.csv"):
            if not self.upload_file(file_path, container_name, remote_path):
                success = False
        return success

class GCSClient(CloudStorageClient):
    """Google Cloud Storage client implementation"""
    
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.client = None
        
        if not GCP_AVAILABLE:
            raise ImportError("GCP libraries not installed. Install with: pip install google-cloud-storage")
    
    def authenticate(self) -> bool:
        try:
            self.client = storage.Client()
            # Test connection by listing buckets
            self.client.list_buckets(max_results=1)
            logger.info("Successfully authenticated with Google Cloud Storage")
            return True
        except GoogleCloudError as e:
            logger.error(f"GCP authentication failed: {str(e)}")
            return False
    
    def create_container(self, container_name: str) -> bool:
        try:
            bucket = self.client.create_bucket(container_name)
            logger.info(f"Created bucket: {container_name}")
        except GoogleCloudError as e:
            if "already exists" in str(e):
                logger.info(f"Bucket {container_name} already exists")
            else:
                logger.error(f"Failed to create bucket: {str(e)}")
                return False
        return True
    
    def upload_file(self, local_path: Path, container_name: str, remote_path: str) -> bool:
        try:
            bucket = self.client.bucket(container_name)
            blob_name = f"{remote_path}/{local_path.name}"
            blob = bucket.blob(blob_name)
            blob.upload_from_filename(str(local_path))
            logger.info(f"Successfully uploaded {local_path.name}")
            return True
        except GoogleCloudError as e:
            logger.error(f"Failed to upload {local_path.name}: {str(e)}")
            return False
    
    def upload_files(self, local_path: Path, container_name: str, remote_path: str) -> bool:
        success = True
        for file_path in local_path.glob("*.csv"):
            if not self.upload_file(file_path, container_name, remote_path):
                success = False
        return success

class KaggleDataDownloader:
    """Downloads and processes Brazilian E-Commerce dataset from Kaggle"""
    
    def __init__(self, dataset_name: str = "olistbr/brazilian-ecommerce"):
        self.dataset_name = dataset_name
        self.local_data_path = Path("data/raw")
        self.processed_data_path = Path("data/processed")
        
    def setup_directories(self):
        """Create necessary directories"""
        self.local_data_path.mkdir(parents=True, exist_ok=True)
        self.processed_data_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created directories: {self.local_data_path}, {self.processed_data_path}")
    
    def download_dataset(self) -> bool:
        """Download dataset from Kaggle"""
        try:
            logger.info(f"Downloading dataset: {self.dataset_name}")
            
            # Download dataset
            kaggle.api.dataset_download_files(
                self.dataset_name,
                path=str(self.local_data_path),
                unzip=True
            )
            
            logger.info("Dataset downloaded successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to download dataset: {str(e)}")
            return False
    
    def validate_downloaded_files(self) -> bool:
        """Validate that all expected CSV files are present"""
        expected_files = [
            'olist_customers_dataset.csv',
            'olist_geolocation_dataset.csv',
            'olist_order_items_dataset.csv',
            'olist_order_payments_dataset.csv',
            'olist_order_reviews_dataset.csv',
            'olist_orders_dataset.csv',
            'olist_products_dataset.csv',
            'olist_sellers_dataset.csv',
            'product_category_name_translation.csv'
        ]
        
        missing_files = []
        for file in expected_files:
            file_path = self.local_data_path / file
            if not file_path.exists():
                missing_files.append(file)
        
        if missing_files:
            logger.error(f"Missing files: {missing_files}")
            return False
        
        logger.info("All expected files are present")
        return True
    
    def get_file_info(self) -> Dict[str, Any]:
        """Get information about downloaded files"""
        file_info = {}
        
        for csv_file in self.local_data_path.glob("*.csv"):
            df = pd.read_csv(csv_file)
            file_info[csv_file.name] = {
                'rows': len(df),
                'columns': len(df.columns),
                'size_mb': csv_file.stat().st_size / (1024 * 1024),
                'column_names': df.columns.tolist()
            }
        
        return file_info

def get_cloud_client(cloud_provider: str, **kwargs) -> CloudStorageClient:
    """Factory function to get appropriate cloud storage client"""
    
    if cloud_provider.lower() == 'azure':
        return AzureDataLakeClient(kwargs['account_name'])
    elif cloud_provider.lower() == 'aws':
        return S3Client(kwargs['bucket_name'], kwargs.get('region', 'us-east-1'))
    elif cloud_provider.lower() == 'gcp':
        return GCSClient(kwargs['bucket_name'])
    else:
        raise ValueError(f"Unsupported cloud provider: {cloud_provider}")

def main():
    """Main execution function"""
    # Configuration from environment variables
    config = {
        'kaggle_dataset': os.getenv('KAGGLE_DATASET', 'olistbr/brazilian-ecommerce'),
        'cloud_provider': os.getenv('CLOUD_PROVIDER', 'azure').lower(),
        'container_name': os.getenv('CONTAINER_NAME', 'brazilian-ecommerce-raw'),
        'remote_path': os.getenv('REMOTE_PATH', 'raw')
    }
    
    # Cloud-specific configuration
    if config['cloud_provider'] == 'azure':
        config['account_name'] = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
        if not config['account_name']:
            logger.error("AZURE_STORAGE_ACCOUNT_NAME environment variable is required for Azure")
            sys.exit(1)
    elif config['cloud_provider'] == 'aws':
        config['bucket_name'] = os.getenv('AWS_S3_BUCKET_NAME')
        config['region'] = os.getenv('AWS_REGION', 'us-east-1')
        if not config['bucket_name']:
            logger.error("AWS_S3_BUCKET_NAME environment variable is required for AWS")
            sys.exit(1)
    elif config['cloud_provider'] == 'gcp':
        config['bucket_name'] = os.getenv('GCP_BUCKET_NAME')
        if not config['bucket_name']:
            logger.error("GCP_BUCKET_NAME environment variable is required for GCP")
            sys.exit(1)
    else:
        logger.error(f"Unsupported cloud provider: {config['cloud_provider']}")
        sys.exit(1)
    
    logger.info("Starting cloud-agnostic data ingestion process")
    logger.info(f"Configuration: {json.dumps(config, indent=2)}")
    
    # Step 1: Download data from Kaggle
    downloader = KaggleDataDownloader(config['kaggle_dataset'])
    downloader.setup_directories()
    
    if not downloader.download_dataset():
        logger.error("Failed to download dataset")
        sys.exit(1)
    
    if not downloader.validate_downloaded_files():
        logger.error("Dataset validation failed")
        sys.exit(1)
    
    # Get file information
    file_info = downloader.get_file_info()
    logger.info(f"Downloaded files info: {json.dumps(file_info, indent=2)}")
    
    # Step 2: Upload to cloud storage
    try:
        cloud_client = get_cloud_client(config['cloud_provider'], **config)
        
        if not cloud_client.authenticate():
            logger.error(f"{config['cloud_provider'].upper()} authentication failed")
            sys.exit(1)
        
        if not cloud_client.create_container(config['container_name']):
            logger.error(f"Failed to create {config['cloud_provider'].upper()} container")
            sys.exit(1)
        
        if not cloud_client.upload_files(downloader.local_data_path, config['container_name'], config['remote_path']):
            logger.error(f"Failed to upload files to {config['cloud_provider'].upper()}")
            sys.exit(1)
        
        logger.info(f"Successfully uploaded files to {config['cloud_provider'].upper()}")
        
    except Exception as e:
        logger.error(f"Cloud storage operation failed: {str(e)}")
        sys.exit(1)
    
    # Create and upload metadata
    metadata = {
        'dataset_name': config['kaggle_dataset'],
        'cloud_provider': config['cloud_provider'],
        'container_name': config['container_name'],
        'remote_path': config['remote_path'],
        'download_timestamp': datetime.utcnow().isoformat(),
        'files': file_info,
        'total_files': len(file_info)
    }
    
    metadata_file = Path("metadata.json")
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2, default=str)
    
    # Upload metadata file
    if hasattr(cloud_client, 'upload_file'):
        cloud_client.upload_file(metadata_file, config['container_name'], 'metadata')
    
    logger.info("Cloud-agnostic data ingestion completed successfully!")

if __name__ == "__main__":
    main()
