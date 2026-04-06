# Databricks notebook source
# MAGIC %md
# MAGIC # Brazilian E-Commerce Data Ingestion
# MAGIC 
# MAGIC This notebook downloads the Brazilian E-Commerce dataset from Kaggle and uploads it to Azure Data Lake Storage.
# MAGIC 
# MAGIC ## Architecture
# MAGIC ```
# MAGIC Kaggle API → Local Download → Azure Data Lake Storage (Raw Layer)
# MAGIC ```

# COMMAND ----------

# Install required libraries
%pip install kaggle azure-storage-file-datalake azure-identity pandas

# COMMAND ----------

# Import required libraries
import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

import pandas as pd
import kaggle
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential
from azure.core.exceptions import AzureError

# COMMAND ----------

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# COMMAND ----------

# Get parameters from ADF
dbutils.widgets.text("kaggle_dataset", "olistbr/brazilian-ecommerce")
dbutils.widgets.text("azure_storage_account", "stbrazilianecommerce")
dbutils.widgets.text("container_name", "brazilian-ecommerce-raw")

kaggle_dataset = dbutils.widgets.get("kaggle_dataset")
storage_account = dbutils.widgets.get("azure_storage_account")
container_name = dbutils.widgets.get("container_name")

logger.info(f"Parameters: dataset={kaggle_dataset}, storage={storage_account}, container={container_name}")

# COMMAND ----------

class KaggleDataDownloader:
    """Downloads and processes Brazilian E-Commerce dataset from Kaggle"""
    
    def __init__(self, dataset_name: str):
        self.dataset_name = dataset_name
        self.local_data_path = Path("/tmp/raw_data")
        
    def setup_directories(self):
        """Create necessary directories"""
        self.local_data_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created directory: {self.local_data_path}")
    
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

# COMMAND ----------

class AzureDataLakeUploader:
    """Uploads processed data to Azure Data Lake Storage"""
    
    def __init__(self, account_name: str, container_name: str):
        self.account_name = account_name
        self.container_name = container_name
        self.service_client = None
        
    def authenticate(self) -> bool:
        """Authenticate with Azure using DefaultAzureCredential"""
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
    
    def create_file_system(self) -> bool:
        """Create container/file system if it doesn't exist"""
        try:
            file_system_client = self.service_client.get_file_system_client(self.container_name)
            file_system_client.create_file_system()
            logger.info(f"Created file system: {self.container_name}")
        except AzureError as e:
            if "already exists" in str(e):
                logger.info(f"File system {self.container_name} already exists")
            else:
                logger.error(f"Failed to create file system: {str(e)}")
                return False
        return True
    
    def upload_csv_files(self, local_path: Path, remote_path: str = "raw") -> bool:
        """Upload all CSV files to Azure Data Lake"""
        try:
            file_system_client = self.service_client.get_file_system_client(self.container_name)
            
            for csv_file in local_path.glob("*.csv"):
                logger.info(f"Uploading {csv_file.name} to {remote_path}")
                
                # Create directory and file client
                directory_client = file_system_client.get_directory_client(remote_path)
                file_client = directory_client.create_file(csv_file.name)
                
                # Upload file content
                with open(csv_file, 'rb') as data:
                    file_client.upload_data(data, overwrite=True)
                
                logger.info(f"Successfully uploaded {csv_file.name}")
            
            return True
            
        except AzureError as e:
            logger.error(f"Failed to upload files: {str(e)}")
            return False
    
    def upload_metadata(self, metadata: Dict[str, Any]) -> bool:
        """Upload metadata as JSON file"""
        try:
            file_system_client = self.service_client.get_file_system_client(self.container_name)
            directory_client = file_system_client.get_directory_client("metadata")
            
            metadata_json = json.dumps(metadata, indent=2, default=str)
            file_client = directory_client.create_file("dataset_info.json")
            
            file_client.upload_data(metadata_json.encode('utf-8'), overwrite=True)
            logger.info("Metadata uploaded successfully")
            return True
            
        except AzureError as e:
            logger.error(f"Failed to upload metadata: {str(e)}")
            return False

# COMMAND ----------

# Main execution
def main():
    """Main execution function"""
    logger.info("Starting data ingestion process in Databricks")
    
    # Step 1: Download data from Kaggle
    downloader = KaggleDataDownloader(kaggle_dataset)
    downloader.setup_directories()
    
    if not downloader.download_dataset():
        logger.error("Failed to download dataset")
        raise Exception("Dataset download failed")
    
    if not downloader.validate_downloaded_files():
        logger.error("Dataset validation failed")
        raise Exception("Dataset validation failed")
    
    # Get file information
    file_info = downloader.get_file_info()
    logger.info(f"Downloaded files info: {json.dumps(file_info, indent=2)}")
    
    # Step 2: Upload to Azure Data Lake
    uploader = AzureDataLakeUploader(storage_account, container_name)
    
    if not uploader.authenticate():
        logger.error("Azure authentication failed")
        raise Exception("Azure authentication failed")
    
    if not uploader.create_file_system():
        logger.error("Failed to create Azure file system")
        raise Exception("Failed to create Azure file system")
    
    if not uploader.upload_csv_files(downloader.local_data_path):
        logger.error("Failed to upload CSV files")
        raise Exception("Failed to upload CSV files")
    
    # Upload metadata
    metadata = {
        'dataset_name': kaggle_dataset,
        'download_timestamp': datetime.utcnow().isoformat(),
        'files': file_info,
        'total_files': len(file_info),
        'databricks_run_id': dbutils.notebook.entry_point.getDbutils().notebook().getContext().runId().get()
    }
    
    if not uploader.upload_metadata(metadata):
        logger.error("Failed to upload metadata")
        raise Exception("Failed to upload metadata")
    
    logger.info("Data ingestion completed successfully!")
    
    # Return summary for ADF
    return {
        'status': 'success',
        'files_processed': len(file_info),
        'total_rows': sum(info['rows'] for info in file_info.values()),
        'storage_account': storage_account,
        'container': container_name,
        'dataset': kaggle_dataset
    }

# COMMAND ----------

# Execute main function
if __name__ == "__main__":
    result = main()
    print(f"Result: {json.dumps(result, indent=2)}")
