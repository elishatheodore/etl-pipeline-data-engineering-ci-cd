# ☁️ Cloud-Agnostic ETL Pipeline - Multi-Cloud Data Engineering

## 📋 Overview

This project implements a **production-grade, cloud-agnostic ETL pipeline** that can run on **Azure, AWS, or GCP**. It provides complete flexibility to switch between cloud providers by simply changing configuration parameters, making it ideal for multi-cloud strategies or cloud migration scenarios.

## 🌐 Multi-Cloud Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Kaggle API    │───▶│  Data Ingestion │───▶│  Cloud Storage  │
│                 │    │  (Python)       │    │  (Azure/AWS/GCP)│
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                                                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Orchestration  │───▶│  Data Processing│───▶│  Cloud Storage  │
│ (Airflow/       │    │ (Databricks/    │    │ (Bronze/Silver) │
│  Prefect/       │    │  Spark)         │    │                 │
│  Terraform)     │    └─────────────────┘    └─────────────────┘
└─────────────────┘                                │
        │                                         ▼
        ▼                                ┌─────────────────┐
┌─────────────────┐                        │  Cloud Storage  │
│  Monitoring     │                        │   (Gold Layer)  │
│ (Prometheus/    │                        │                 │
│  Grafana)       │                        └─────────────────┘
└─────────────────┘                                │
        ▼                                         ▼
┌─────────────────┐                        ┌─────────────────┐
│  CI/CD          │                        │   Analytics     │
│ (GitHub Actions)│                        │ (Parameterized) │
└─────────────────┘                        └─────────────────┘
```

## 🎯 Cloud Provider Support

| Feature | Azure | AWS | GCP |
|---------|-------|-----|-----|
| **Storage** | Data Lake Storage Gen2 | S3 | Cloud Storage |
| **Processing** | Databricks | Databricks/EMR | Databricks/Dataproc |
| **Orchestration** | Airflow/Prefect | Airflow/Prefect | Airflow/Prefect |
| **Infrastructure** | Terraform | Terraform | Terraform |
| **Monitoring** | Azure Monitor + Prometheus | CloudWatch + Prometheus | Cloud Monitoring + Prometheus |
| **CI/CD** | GitHub Actions | GitHub Actions | GitHub Actions |
| **Security** | Key Vault | Secrets Manager | Secret Manager |

## 📁 Project Structure

```
cloud_agnostic_data_etl_pipeline/
├── orchestration/
│   ├── airflow/
│   │   └── brazilian_ecommerce_dag.py          # Airflow DAG
│   ├── prefect/
│   │   └── brazilian_ecommerce_flow.py         # Prefect flow
│   └── terraform/
│       ├── main.tf                             # Infrastructure code
│       ├── variables.tf                        # Input variables
│       └── outputs.tf                          # Output values
├── databricks/
│   ├── notebooks/                               # Cloud-agnostic notebooks
│   └── jobs/                                   # Job definitions
├── data_lake/
│   ├── azure/                                  # Azure-specific setup
│   ├── aws/                                    # AWS-specific setup
│   └── gcp/                                    # GCP-specific setup
├── cicd/
│   └── github-actions.yml                      # CI/CD pipeline
├── monitoring/
│   ├── prometheus_grafana_setup.py             # Monitoring setup
│   ├── dashboards/                             # Grafana dashboards
│   └── alerts/                                 # Alert rules
├── data/
│   └── download_kaggle_data.py                 # Cloud-agnostic ingestion
└── README.md                                   # This file
```

## 🚀 Quick Start

### Prerequisites

- **Python 3.9+**
- **Terraform 1.5.0+**
- **Docker** (optional)
- **Cloud provider CLI tools**
- **Valid cloud provider credentials**

### 1. Choose Your Cloud Provider

```bash
# For Azure
export CLOUD_PROVIDER=azure
export AZURE_STORAGE_ACCOUNT_NAME="your-storage-account"

# For AWS
export CLOUD_PROVIDER=aws
export AWS_S3_BUCKET_NAME="your-bucket"
export AWS_REGION="us-east-1"

# For GCP
export CLOUD_PROVIDER=gcp
export GCP_BUCKET_NAME="your-bucket"
export GCP_PROJECT_ID="your-project-id"
```

### 2. Deploy Infrastructure with Terraform

```bash
cd orchestration/terraform

# Initialize Terraform
terraform init

# Plan infrastructure
terraform plan -var cloud_provider=$CLOUD_PROVIDER

# Apply infrastructure
terraform apply -var cloud_provider=$CLOUD_PROVIDER -auto-approve
```

### 3. Run Data Ingestion

```bash
cd ../../data

# Install dependencies
pip install -r ../../requirements.txt

# Run data ingestion
python download_kaggle_data.py
```

### 4. Setup Orchestration

#### Option A: Airflow

```bash
cd ../orchestration/airflow

# Install Airflow
pip install apache-airflow

# Set up Airflow
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__SQL_ALCHEMY_CONN=sqlite:////airflow/airflow.db
airflow db init

# Copy DAG file
cp brazilian_ecommerce_dag.py $AIRFLOW_HOME/dags/

# Start Airflow
airflow scheduler &
airflow webserver --port 8080

# Trigger DAG
airflow dags trigger brazilian_ecommerce_etl_pipeline
```

#### Option B: Prefect

```bash
cd ../prefect

# Install Prefect
pip install prefect

# Start Prefect server
prefect server start

# Deploy flow
python brazilian_ecommerce_flow.py

# Run flow
prefect deployment run brazilian-ecommerce-etl-production
```

### 5. Setup Monitoring

```bash
cd ../../monitoring

# Install monitoring tools
pip install prometheus-client grafana-api

# Run monitoring setup
python prometheus_grafana_setup.py

# Start Prometheus
prometheus --config.file=prometheus.yml

# Start Grafana
grafana-server --config=grafana.ini
```

## 🔧 Configuration

### Environment Variables

```bash
# Cloud Provider Configuration
export CLOUD_PROVIDER=azure          # azure, aws, or gcp
export AZURE_STORAGE_ACCOUNT_NAME=...
export AWS_S3_BUCKET_NAME=...
export GCP_BUCKET_NAME=...

# Databricks Configuration
export DATABRICKS_HOST=https://your-workforce.cloud.databricks.com
export DATABRICKS_TOKEN=your-token

# Dataset Configuration
export KAGGLE_DATASET=olistbr/brazilian-ecommerce
export CONTAINER_NAME=brazilian-ecommerce-raw

# Monitoring Configuration
export PROMETHEUS_URL=http://localhost:9090
export GRAFANA_URL=http://localhost:3000
export GRAFANA_TOKEN=your-grafana-token

# Notification Configuration
export SLACK_WEBHOOK_URL=your-slack-webhook
```

### Terraform Configuration

```hcl
# terraform.tfvars
cloud_provider = "azure"
environment     = "dev"

# Azure Configuration
azure_subscription_id = "your-subscription-id"
azure_tenant_id      = "your-tenant-id"
azure_client_id      = "your-client-id"
azure_client_secret  = "your-client-secret"

# AWS Configuration
aws_region      = "us-east-1"
aws_access_key  = "your-access-key"
aws_secret_key  = "your-secret-key"

# GCP Configuration
gcp_project_id  = "your-project-id"
gcp_credentials = "path/to/credentials.json"

# Databricks Configuration
databricks_host  = "https://your-workforce.cloud.databricks.com"
databricks_token = "your-databricks-token"
```

## 📊 Multi-Cloud Data Flow

### Cloud-Agnostic Data Ingestion

```python
# Abstract base class for cloud storage
class CloudStorageClient(ABC):
    @abstractmethod
    def authenticate(self) -> bool:
        pass
    
    @abstractmethod
    def upload_files(self, local_path: Path, container_name: str, remote_path: str) -> bool:
        pass

# Azure implementation
class AzureDataLakeClient(CloudStorageClient):
    def authenticate(self) -> bool:
        self.service_client = DataLakeServiceClient(
            account_url=f"https://{self.account_name}.dfs.core.windows.net",
            credential=DefaultAzureCredential()
        )

# AWS implementation
class S3Client(CloudStorageClient):
    def authenticate(self) -> bool:
        self.s3_client = boto3.client('s3', region_name=self.region)

# GCP implementation
class GCSClient(CloudStorageClient):
    def authenticate(self) -> bool:
        self.client = storage.Client()

# Factory pattern
def get_cloud_client(cloud_provider: str, **kwargs) -> CloudStorageClient:
    if cloud_provider.lower() == 'azure':
        return AzureDataLakeClient(kwargs['account_name'])
    elif cloud_provider.lower() == 'aws':
        return S3Client(kwargs['bucket_name'], kwargs.get('region', 'us-east-1'))
    elif cloud_provider.lower() == 'gcp':
        return GCSClient(kwargs['bucket_name'])
```

### Parameterized Databricks Notebooks

```python
# Cloud-agnostic notebook
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("container_name", "")
dbutils.widgets.text("cloud_provider", "azure")

storage_account = dbutils.widgets.get("storage_account")
container_name = dbutils.widgets.get("container_name")
cloud_provider = dbutils.widgets.get("cloud_provider")

# Construct cloud-agnostic paths
if cloud_provider.lower() == 'azure':
    base_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net"
elif cloud_provider.lower() == 'aws':
    base_path = f"s3://{container_name}"
elif cloud_provider.lower() == 'gcp':
    base_path = f"gs://{container_name}"

# Read data
df = spark.read.format("delta").load(f"{base_path}/bronze/customers")
```

## 🔄 Orchestration Options

### 1. Apache Airflow

**Features**:
- Rich UI for monitoring
- Extensive plugin ecosystem
- Mature and battle-tested
- Good for complex workflows

**DAG Structure**:
```python
with DAG('brazilian_ecommerce_etl_pipeline') as dag:
    download_task = PythonOperator(
        task_id='download_kaggle_data',
        python_callable=download_kaggle_data
    )
    
    bronze_task = DatabricksRunNowOperator(
        task_id='bronze_layer_transformation',
        notebook_path='/ETL/02_Bronze_Layer/bronze_transformation'
    )
    
    silver_task = DatabricksRunNowOperator(
        task_id='silver_layer_transformation',
        notebook_path='/ETL/03_Silver_Layer/silver_transformation'
    )
    
    gold_task = DatabricksRunNowOperator(
        task_id='gold_layer_transformation',
        notebook_path='/ETL/04_Gold_Layer/gold_transformation'
    )
    
    download_task >> bronze_task >> silver_task >> gold_task
```

### 2. Prefect

**Features**:
- Modern Python-native workflow engine
- Excellent type safety and IDE support
- Built-in scheduling and retries
- Great for data engineers who love Python

**Flow Structure**:
```python
@flow(name="Brazilian E-Commerce ETL Pipeline")
def brazilian_ecommerce_etl(cloud_provider: str = "azure"):
    metadata = download_kaggle_data.submit(config)
    
    bronze_result = run_databricks_job.submit(
        config, "/ETL/02_Bronze_Layer/bronze_transformation", "bronze", metadata
    )
    
    silver_result = run_databricks_job.submit(
        config, "/ETL/03_Silver_Layer/silver_transformation", "silver", metadata
    )
    
    gold_result = run_databricks_job.submit(
        config, "/ETL/04_Gold_Layer/gold_transformation", "gold", metadata
    )
    
    return {
        'bronze': bronze_result.result(),
        'silver': silver_result.result(),
        'gold': gold_result.result()
    }
```

### 3. Terraform

**Features**:
- Infrastructure as code
- Multi-cloud support
- State management
- Great for reproducible environments

**Resource Definition**:
```hcl
# Databricks jobs
resource "databricks_job" "bronze_transformation" {
  name = "${local.name_prefix}-bronze-transformation"
  
  new_cluster {
    spark_version = var.databricks_spark_version
    node_type_id  = var.databricks_node_type
    num_workers   = 2
  }
  
  notebook_task {
    notebook_path = "/ETL/02_Bronze_Layer/bronze_transformation"
    base_parameters = {
      storage_account = local.is_azure ? azurerm_storage_account.main[0].name : ""
      container_name  = "bronze"
    }
  }
}
```

## 📈 Monitoring and Alerting

### Prometheus + Grafana Setup

**Metrics Collection**:
```python
# Custom metrics for ETL pipeline
from prometheus_client import Counter, Histogram, Gauge

# Counters
pipeline_runs_total = Counter('etl_pipeline_runs_total', 'Total pipeline runs', ['status'])
records_processed_total = Counter('etl_records_processed_total', 'Total records processed', ['table'])

# Histograms
pipeline_duration_seconds = Histogram('etl_pipeline_duration_seconds', 'Pipeline duration')

# Gauges
data_quality_score = Gauge('etl_data_quality_score', 'Data quality score', ['table'])
```

**Alert Rules**:
```yaml
# prometheus_rules.yml
groups:
  - name: etl_pipeline
    rules:
      - alert: ETLPipelineHighFailureRate
        expr: rate(etl_pipeline_runs_total{status="failed"}[5m]) > 0.05
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "ETL pipeline failure rate is high"
      
      - alert: DataQualityIssues
        expr: etl_data_quality_score < 0.9
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Data quality score is below threshold"
```

### Cloud-Specific Monitoring

**Azure Monitor Integration**:
```python
# Azure Monitor metrics
from azure.monitor.query import LogsQueryClient

def send_metrics_to_azure_monitor(metrics: Dict[str, Any]):
    client = LogsQueryClient(credential)
    # Send custom metrics
    pass
```

**CloudWatch Integration**:
```python
# CloudWatch metrics
import boto3

def send_metrics_to_cloudwatch(metrics: Dict[str, Any]):
    cloudwatch = boto3.client('cloudwatch')
    for metric_name, value in metrics.items():
        cloudwatch.put_metric_data(
            Namespace='ETL-Pipeline',
            MetricData=[{
                'MetricName': metric_name,
                'Value': value,
                'Unit': 'Count'
            }]
        )
```

## 🔒 Multi-Cloud Security

### Cloud-Agnostic Authentication

```python
# Factory pattern for authentication
def get_auth_client(cloud_provider: str):
    if cloud_provider.lower() == 'azure':
        return DefaultAzureCredential()
    elif cloud_provider.lower() == 'aws':
        return boto3.Session()
    elif cloud_provider.lower() == 'gcp':
        return service_account.Credentials.from_service_account_file()
```

### Secrets Management

```python
# Cloud-agnostic secrets management
class SecretsManager:
    def __init__(self, cloud_provider: str):
        self.cloud_provider = cloud_provider
        self.client = self._get_client()
    
    def _get_client(self):
        if self.cloud_provider == 'azure':
            from azure.keyvault.secrets import SecretClient
            return SecretClient(vault_url, credential)
        elif self.cloud_provider == 'aws':
            import boto3
            return boto3.client('secretsmanager')
        elif self.cloud_provider == 'gcp':
            from google.cloud import secretmanager
            return secretmanager.SecretManagerServiceClient()
    
    def get_secret(self, secret_name: str) -> str:
        # Cloud-specific implementation
        pass
```

### Network Security

```terraform
# Network security for each cloud
resource "azurerm_network_security_group" "main" {
  count = local.is_azure ? 1 : 0
  
  security_rule {
    name                       = "allow_databricks"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "443"
    source_address_prefix      = "40.64.112.0/20"  # Databricks IPs
    destination_address_prefix = azurerm_subnet.main[0].address_prefix
  }
}

resource "aws_security_group" "main" {
  count = local.is_aws ? 1 : 0
  
  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["52.24.6.0/26"]  # Databricks IPs
  }
}
```

## 🔄 CI/CD Pipeline

### GitHub Actions Workflow

**Multi-Cloud Testing**:
```yaml
jobs:
  test:
    strategy:
      matrix:
        cloud_provider: [azure, aws, gcp]
    
    steps:
    - name: Test with ${{ matrix.cloud_provider }}
      run: |
        export CLOUD_PROVIDER=${{ matrix.cloud_provider }}
        python -m pytest tests/
```

**Infrastructure Deployment**:
```yaml
deploy:
  needs: test
  steps:
  - name: Deploy Infrastructure
    run: |
      cd orchestration/terraform
      terraform apply -var cloud_provider=${{ env.CLOUD_PROVIDER }} -auto-approve
```

### Environment-Specific Deployments

```yaml
environments:
  development:
    cloud_provider: azure
    storage_account: dev-storage
  staging:
    cloud_provider: aws
    s3_bucket: staging-bucket
  production:
    cloud_provider: gcp
    gcp_bucket: prod-bucket
```

## 🧪 Testing Strategy

### Multi-Cloud Testing

```python
# Test against all cloud providers
@pytest.mark.parametrize("cloud_provider", ["azure", "aws", "gcp"])
def test_data_ingestion(cloud_provider):
    # Mock cloud provider
    with patch('data.download_kaggle_data.get_cloud_client') as mock_client:
        mock_client.return_value = MockCloudClient()
        
        # Test ingestion
        result = download_kaggle_data()
        assert result['status'] == 'success'
```

### Integration Tests

```python
def test_end_to_end_pipeline():
    # Test complete pipeline flow
    for cloud_provider in ['azure', 'aws', 'gcp']:
        with TemporaryDirectory() as temp_dir:
            # Setup test environment
            setup_test_environment(cloud_provider, temp_dir)
            
            # Run pipeline
            result = run_pipeline(cloud_provider, temp_dir)
            
            # Validate results
            assert result['status'] == 'success'
            validate_data_quality(result['data'])
```

## 💰 Cost Comparison

### Monthly Cost Estimates (Production)

| Component | Azure | AWS | GCP |
|-----------|-------|-----|-----|
| **Storage (10TB)** | $180 | $195 | $170 |
| **Compute (Databricks)** | $600 | $650 | $580 |
| **Orchestration** | $70 | $60 | $65 |
| **Monitoring** | $40 | $35 | $38 |
| **Networking** | $25 | $30 | $28 |
| **Total** | **$915** | **$970** | **$881** |

### Cost Optimization Tips

1. **Auto-scaling**: Scale resources based on workload
2. **Spot Instances**: Use discounted compute (AWS/Azure)
3. **Storage Tiers**: Use appropriate storage classes
4. **Scheduling**: Stop resources during off-hours
5. **Cross-Cloud Arbitrage**: Choose cheapest provider for each service

## 🚨 Troubleshooting

### Cloud-Specific Issues

**Azure**:
```bash
# Check Azure CLI login
az account show

# Verify storage access
az storage container list --account-name $AZURE_STORAGE_ACCOUNT_NAME
```

**AWS**:
```bash
# Check AWS CLI configuration
aws sts get-caller-identity

# Verify S3 access
aws s3 ls s3://$AWS_S3_BUCKET_NAME
```

**GCP**:
```bash
# Check GCP authentication
gcloud auth list

# Verify storage access
gsutil ls gs://$GCP_BUCKET_NAME
```

### Common Solutions

1. **Authentication Failures**
   - Verify cloud provider credentials
   - Check service permissions
   - Ensure proper IAM roles

2. **Network Connectivity**
   - Check firewall rules
   - Verify VPC/subnet configuration
   - Test connectivity to cloud services

3. **Data Processing Issues**
   - Monitor resource utilization
   - Check Databricks cluster status
   - Review transformation logs

## 📚 Additional Resources

### Documentation

- [Multi-Cloud Strategy Guide](docs/multi-cloud-strategy.md)
- [Cloud Migration Guide](docs/cloud-migration.md)
- [Cost Optimization Guide](docs/cost-optimization.md)
- [Security Best Practices](docs/security-best-practices.md)

### Tools and Libraries

- [Terraform Provider Documentation](https://registry.terraform.io/)
- [Databricks API Reference](https://docs.databricks.com/dev-api/index.html)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Prefect Documentation](https://docs.prefect.io/)

### Community

- [GitHub Discussions](https://github.com/your-org/etl-pipeline-data-engineering-ci-cd/discussions)
- [Slack Community](https://data-engineering-community.slack.com/)
- [Stack Overflow](https://stackoverflow.com/questions/tagged/etl-pipeline)

---

**☁️ Cloud-Agnostic - Deploy Anywhere, Scale Everywhere**
