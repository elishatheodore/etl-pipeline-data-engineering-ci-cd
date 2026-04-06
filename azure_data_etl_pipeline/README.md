# 🔷 Azure Data ETL Pipeline - Brazilian E-Commerce Analytics

## 📋 Overview

This project implements a **production-grade, enterprise-standard ETL pipeline** for processing the Brazilian E-Commerce dataset using **Microsoft Azure services**. The pipeline follows the medallion architecture (Bronze-Silver-Gold) and integrates with Azure's complete data analytics stack.

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Kaggle API    │───▶│  Data Ingestion │───▶│ Azure Data Lake │
│                 │    │   (Python)      │    │   (Raw Layer)  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                                                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Azure Data      │───▶│ Azure           │───▶│ Azure Data Lake │
│ Factory (ADF)   │    │ Databricks      │    │ (Bronze/Silver) │
│                 │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                                                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Microsoft       │◀───│ Azure Synapse   │◀───│ Azure Data Lake │
│ Fabric          │    │ Analytics       │    │   (Gold Layer)  │
│ Lakehouse       │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
        │
        ▼
┌─────────────────┐
│ Azure Monitor   │
│ + Log Analytics │
│ + Key Vault     │
└─────────────────┘
```

## 🎯 Azure Services Used

| Service | Purpose | Configuration |
|----------|---------|---------------|
| **Azure Data Factory** | Orchestration and pipeline management | Trigger-based scheduling |
| **Azure Databricks** | Data transformation and processing | Auto-scaling clusters |
| **Azure Data Lake Storage Gen2** | Data storage (medallion architecture) | Hierarchical namespace |
| **Azure Synapse Analytics** | Data warehousing and analytics | SQL pools |
| **Microsoft Fabric** | Lakehouse and analytics | Workspace integration |
| **Azure Key Vault** | Secrets management | RBAC-controlled |
| **Azure Monitor** | Monitoring and logging | Log Analytics workspace |

## 📁 Project Structure

```
azure_data_etl_pipeline/
├── adf/
│   └── pipeline_brazilian_ecommerce.json    # ADF pipeline definition
├── databricks/
│   ├── 01_Data_Ingestion/
│   │   └── download_kaggle_data.py          # Data ingestion notebook
│   ├── 02_Bronze_Layer/
│   │   └── bronze_transformation.py         # Bronze layer processing
│   ├── 03_Silver_Layer/
│   │   └── silver_transformation.py         # Silver layer processing
│   └── 04_Gold_Layer/
│       └── gold_transformation.py           # Gold layer (star schema)
├── synapse/
│   └── create_synapse_objects.sql           # Synapse database objects
├── fabric/
│   └── lakehouse_setup.py                   # Fabric lakehouse setup
├── keyvault/
│   └── keyvault_secrets.json                # Key Vault configuration
├── cicd/
│   └── azure-pipelines.yml                  # Azure DevOps pipeline
├── monitoring/
│   └── azure_monitor_setup.py               # Monitor configuration
├── data/
│   └── download_kaggle_data.py              # Data ingestion script
└── README.md                                # This file
```

## 🚀 Quick Start

### Prerequisites

- **Azure Subscription** with appropriate permissions
- **Azure CLI** installed and configured
- **Azure DevOps** organization (for CI/CD)
- **Databricks workspace** (can be created via script)
- **Python 3.9+** with required packages

### 1. Setup Azure Resources

```bash
# Login to Azure
az login

# Create resource group
az group create \
  --name brazilian-ecommerce-rg \
  --location "East US"

# Create storage account
az storage account create \
  --name stbrazilianecommerce \
  --resource-group brazilian-ecommerce-rg \
  --location "East US" \
  --sku Standard_RAGRS \
  --kind StorageV2 \
  --hierarchical-namespace true

# Create Databricks workspace
az databricks workspace create \
  --name brazilian-ecommerce-databricks \
  --resource-group brazilian-ecommerce-rg \
  --location "East US" \
  --sku premium

# Create Synapse workspace
az synapse workspace create \
  --name brazilian-ecommerce-synapse \
  --resource-group brazilian-ecommerce-rg \
  --location "East US" \
  --storage-account stbrazilianecommerce \
  --admin-password "YourStrongPassword123!"

# Create Key Vault
az keyvault create \
  --name brazilian-ecommerce-kv \
  --resource-group brazilian-ecommerce-rg \
  --location "East US"
```

### 2. Configure Secrets in Key Vault

```bash
# Add secrets to Key Vault
az keyvault secret set \
  --vault-name brazilian-ecommerce-kv \
  --name "databricks-token" \
  --value "your-databricks-token"

az keyvault secret set \
  --vault-name brazilian-ecommerce-kv \
  --name "kaggle-api-key" \
  --value "your-kaggle-api-key"

az keyvault secret set \
  --vault-name brazilian-ecommerce-kv \
  --name "slack-webhook-url" \
  --value "your-slack-webhook-url"
```

### 3. Setup Environment Variables

```bash
# Azure Storage
export AZURE_STORAGE_ACCOUNT_NAME="stbrazilianecommerce"
export AZURE_CONTAINER_NAME="brazilian-ecommerce-raw"

# Databricks
export DATABRICKS_HOST="https://brazilian-ecommerce-databricks.azuredatabricks.net"
export DATABRICKS_TOKEN=$(az keyvault secret show --vault-name brazilian-ecommerce-kv --name databricks-token --query value -o tsv)

# Dataset
export KAGGLE_DATASET="olistbr/brazilian-ecommerce"
```

### 4. Run Data Ingestion

```bash
cd data
python download_kaggle_data.py
```

### 5. Deploy Databricks Notebooks

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure Databricks CLI
databricks configure --token

# Upload notebooks
databricks workspace import_dir ../databricks/
```

### 6. Deploy ADF Pipeline

```bash
cd ../adf
az datafactory pipeline create \
  --resource-group brazilian-ecommerce-rg \
  --factory-name brazilian-ecommerce-adf \
  --name Brazilian_Ecommerce_ETL_Pipeline \
  --pipeline-file pipeline_brazilian_ecommerce.json
```

### 7. Setup Synapse Database

```bash
cd ../synapse
# Create Synapse SQL pool and run the SQL script
az synapse sql pool create \
  --name brazilian-ecommerce-sql \
  --workspace-name brazilian-ecommerce-synapse \
  --resource-group brazilian-ecommerce-rg \
  --sql-admin-password "YourStrongPassword123!"

# Run SQL script using Synapse Studio or SQL client
```

## 📊 Data Flow

### 1. Data Ingestion (Raw → Bronze)

```python
# Download from Kaggle API
kaggle.api.dataset_download_files('olistbr/brazilian-ecommerce')

# Upload to Azure Data Lake Storage
storage_client = DataLakeServiceClient(
    account_url="https://stbrazilianecommerce.dfs.core.windows.net",
    credential=DefaultAzureCredential()
)
```

### 2. Bronze Layer Processing

```python
# Schema validation and Delta format conversion
df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(predefined_schema) \
    .load("abfss://raw@stbrazilianecommerce.dfs.core.windows.net/")

df.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://bronze@stbrazilianecommerce.dfs.core.windows.net/")
```

### 3. Silver Layer Processing

```python
# Business logic and data enrichment
df = df.withColumn("customer_region", 
    when(col("customer_state").isin("SP", "RJ", "ES", "MG"), "Southeast")
    .when(col("customer_state").isin("RS", "SC", "PR"), "South")
    .otherwise("Other"))
```

### 4. Gold Layer Processing

```python
# Create star schema
# Fact table
fact_orders = order_items.join(orders, "order_id") \
    .join(customers, "customer_id") \
    .join(products, "product_id")

# Dimension tables
dim_customers = customers.select("customer_id", "customer_city", "customer_state").distinct()
dim_products = products.select("product_id", "product_category_name_english").distinct()
```

## 🔧 Configuration

### Azure Data Factory Pipeline Parameters

| Parameter | Description | Default Value |
|-----------|-------------|---------------|
| `storageAccountName` | Azure Storage account name | `stbrazilianecommerce` |
| `containerName` | Container name | `brazilian-ecommerce-raw` |
| `bronzeDataPath` | Bronze layer path | `bronze` |
| `silverDataPath` | Silver layer path | `silver` |
| `goldDataPath` | Gold layer path | `gold` |
| `fabricLakehouseName` | Fabric lakehouse name | `BrazilianEcommerceLakehouse` |

### Databricks Cluster Configuration

```json
{
  "cluster_name": "brazilian-ecommerce-etl",
  "spark_version": "11.3.x-scala2.12",
  "node_type_id": "Standard_DS3_v2",
  "autotermination_minutes": 30,
  "autoscale": {
    "min_workers": 1,
    "max_workers": 4
  },
  "spark_conf": {
    "spark.sql.legacy.allowUntypedScalaUDF": "true",
    "spark.databricks.delta.preview.enabled": "true"
  }
}
```

## 📈 Monitoring and Alerting

### Azure Monitor Setup

```python
# Create Log Analytics workspace
workspace = monitor_client.workspaces.begin_create_or_update(
    resource_group_name="brazilian-ecommerce-rg",
    workspace_name="brazilian-ecommerce-law",
    workspace_parameters={
        'location': 'East US',
        'sku': {'name': 'PerGB2018'},
        'retention_in_days': 30
    }
)

# Create metric alerts
alert_rule = monitor_client.metric_alerts.create_or_update(
    resource_group_name="brazilian-ecommerce-rg",
    alert_name="ETL-Pipeline-High-Failure-Rate",
    alert={
        'description': 'Alert when ETL pipeline failure rate exceeds 5%',
        'severity': 2,
        'enabled': True,
        'scopes': [resource_id],
        'evaluation_frequency': 'PT1M',
        'window_size': 'PT5M',
        'criteria': {
            'all_of': [
                {
                    'field': 'PercentageFailedRuns',
                    'operator': 'GreaterThan',
                    'threshold': 5.0
                }
            ]
        }
    }
)
```

### Key Metrics to Monitor

- **Pipeline Success Rate**: Percentage of successful pipeline runs
- **Data Processing Duration**: Time taken for each transformation step
- **Resource Utilization**: CPU, memory usage of Databricks clusters
- **Data Quality**: Null percentages, duplicate records
- **Cost Monitoring**: Daily compute and storage costs
- **Error Rates**: Failed jobs, data validation errors

## 🔒 Security Implementation

### Key Vault Integration

```python
# Retrieve secrets from Key Vault
credential = DefaultAzureCredential()
key_client = SecretClient(
    vault_url="https://brazilian-ecommerce-kv.vault.azure.net",
    credential=credential
)

databricks_token = key_client.get_secret("databricks-token").value
kaggle_api_key = key_client.get_secret("kaggle-api-key").value
```

### Role-Based Access Control (RBAC)

| Role | Permissions | Assignment |
|------|-------------|------------|
| **Data Engineer** | Read/Write data, Run pipelines | AAD Group |
| **Data Analyst** | Read data, Query analytics | AAD Group |
| **DevOps** | Deploy infrastructure, CI/CD | Service Principal |
| **Monitoring** | Read logs, Set alerts | Service Principal |

### Network Security

```json
{
  "networkRules": {
    "bypass": ["AzureServices"],
    "defaultAction": "Deny",
    "ipRules": [
      {
        "value": "192.168.1.0/24",
        "action": "Allow"
      }
    ],
    "virtualNetworkRules": [
      {
        "id": "/subscriptions/xxx/resourceGroups/xxx/providers/Microsoft.Network/virtualNetworks/xxx/subnets/xxx",
        "action": "Allow"
      }
    ]
  }
}
```

## 🔄 CI/CD Pipeline

### Azure DevOps Pipeline Stages

1. **Validate**
   - Code formatting (Black)
   - Linting (Flake8)
   - Type checking (MyPy)
   - Unit tests (Pytest)
   - Security scans (Bandit)

2. **Build**
   - Package artifacts
   - Create deployment packages
   - Generate version tags

3. **Deploy Dev**
   - Deploy to development environment
   - Run integration tests
   - Validate data flow

4. **Deploy Staging**
   - Deploy to staging environment
   - Performance testing
   - User acceptance testing

5. **Deploy Production**
   - Deploy to production environment
   - Run smoke tests
   - Setup monitoring

### Pipeline Variables

```yaml
variables:
  - group: etl-pipeline-variables
  - name: pythonVersion
    value: '3.9'
  - name: workingDirectory
    value: 'azure_data_etl_pipeline'
```

## 📊 Analytics and Reporting

### Synapse Analytics Views

```sql
-- Sales by date view
CREATE VIEW gold.v_sales_by_date AS
SELECT 
    d.date_key,
    d.date,
    d.year,
    d.quarter,
    d.month,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    SUM(fo.order_item_total_value) AS total_revenue,
    AVG(fo.order_item_total_value) AS avg_order_value
FROM gold.fact_orders fo
INNER JOIN gold.dim_date d ON fo.date_key = d.date_key
WHERE fo.order_status = 'delivered'
GROUP BY d.date_key, d.date, d.year, d.quarter, d.month;
```

### Fabric Lakehouse Integration

```python
# Create Fabric lakehouse
fabric_client = FabricLakehouseManager(workspace_id, tenant_id, client_id, client_secret)

lakehouse_result = fabric_client.setup_brazilian_ecommerce_lakehouse()

# Create semantic model
semantic_model = fabric_client.create_semantic_model(
    lakehouse_id, 
    "BrazilianEcommerceModel"
)

# Create Power BI report
report = fabric_client.create_report(
    lakehouse_id,
    "BrazilianEcommerceDashboard"
)
```

## 🧪 Testing

### Unit Tests

```python
import pytest
from azure_data_etl_pipeline.data.download_kaggle_data import KaggleDataDownloader

class TestKaggleDataDownloader:
    def test_setup_directories(self):
        downloader = KaggleDataDownloader("olistbr/brazilian-ecommerce")
        downloader.setup_directories()
        assert downloader.local_data_path.exists()
    
    def test_validate_downloaded_files(self):
        # Mock file system and test validation logic
        pass
```

### Integration Tests

```python
def test_end_to_end_pipeline():
    # Test complete pipeline flow
    # 1. Data ingestion
    # 2. Bronze layer processing
    # 3. Silver layer processing
    # 4. Gold layer processing
    # 5. Data validation
    pass
```

### Data Quality Tests

```python
def test_data_quality():
    # Test data quality metrics
    # - Null percentage checks
    # - Duplicate record detection
    # - Schema validation
    # - Business rule validation
    pass
```

## 🚨 Troubleshooting

### Common Issues and Solutions

1. **Authentication Failures**
   ```bash
   # Check Azure CLI login
   az account show
   
   # Verify Key Vault access
   az keyvault secret show --vault-name brazilian-ecommerce-kv --name databricks-token
   ```

2. **Databricks Connection Issues**
   ```python
   # Test Databricks connectivity
   import requests
   response = requests.get(f"{databricks_host}/api/2.0/clusters/list", 
                          headers={'Authorization': f'Bearer {databricks_token}'})
   ```

3. **Data Lake Storage Access**
   ```python
   # Test storage access
   from azure.storage.filedatalake import DataLakeServiceClient
   client = DataLakeServiceClient(account_url, credential)
   filesystems = client.list_file_systems()
   ```

4. **Pipeline Failures**
   ```bash
   # Check ADF pipeline runs
   az datafactory pipeline run show \
     --resource-group brazilian-ecommerce-rg \
     --factory-name brazilian-ecommerce-adf \
     --run-id <run-id>
   ```

### Log Locations

- **ADF Pipeline Runs**: Azure Monitor → Log Analytics
- **Databricks Jobs**: Databricks workspace → Jobs → Run logs
- **Synapse Queries**: Synapse Studio → Monitoring → SQL requests
- **Storage Operations**: Azure Storage Analytics logs

## 💰 Cost Optimization

### Cost Management Strategies

1. **Auto-scaling Databricks Clusters**
   ```json
   {
     "autoscale": {
       "min_workers": 1,
       "max_workers": 4
     },
     "autotermination_minutes": 30
   }
   ```

2. **Storage Tiering**
   ```bash
   # Set up lifecycle policies
   az storage account management-policy create \
     --account-name stbrazilianecommerce \
     --resource-group brazilian-ecommerce-rg \
     --policy @lifecycle-policy.json
   ```

3. **Reserved Capacity**
   - Consider reserved instances for predictable workloads
   - Use spot instances for non-critical processing

### Estimated Monthly Costs (Production)

| Service | Configuration | Monthly Cost |
|---------|---------------|--------------|
| **Data Lake Storage** | 10 TB hot, 50 TB cool | $150-300 |
| **Databricks** | Standard_DS3_v2, auto-scale | $400-800 |
| **Synapse Analytics** | DW100c SQL pool | $200-400 |
| **Data Factory** | 1M pipeline runs | $50-100 |
| **Key Vault** | Standard tier | $6-10 |
| **Monitor** | Log Analytics | $20-50 |
| **Fabric** | Pro license | $20-50 |
| **Total** | | **$846-1710** |

## 📚 Additional Resources

### Documentation Links

- [Azure Data Factory Documentation](https://docs.microsoft.com/en-us/azure/data-factory/)
- [Azure Databricks Documentation](https://docs.microsoft.com/en-us/azure/databricks/)
- [Azure Synapse Analytics](https://docs.microsoft.com/en-us/azure/synapse-analytics/)
- [Microsoft Fabric](https://docs.microsoft.com/en-us/fabric/)
- [Azure Data Lake Storage](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction)

### Best Practices

- Use **Delta Lake** for ACID transactions and time travel
- Implement **data partitioning** by date for performance
- Use **auto-scaling** to optimize costs
- Monitor **data quality** metrics continuously
- Implement **proper error handling** and retry logic
- Use **managed identities** for authentication
- Set up **cost alerts** to monitor spending

---

**🔷 Built with Microsoft Azure - Enterprise-Grade Data Engineering**
