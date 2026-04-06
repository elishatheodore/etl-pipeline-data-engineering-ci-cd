# 🏗️ Brazilian E-Commerce ETL Pipeline - Production-Grade Data Engineering

## 📋 Overview

This repository contains **two production-grade, end-to-end data engineering projects** for processing the Brazilian E-Commerce dataset from Olist. Both projects follow enterprise standards, are fully modular, parameterized, and ready for deployment with comprehensive CI/CD, monitoring, and infrastructure best practices.

## 🎯 Projects

### 1. Azure Data ETL Pipeline (`/azure_data_etl_pipeline`)
- **Cloud**: Azure-specific implementation
- **Orchestration**: Azure Data Factory
- **Processing**: Azure Databricks
- **Storage**: Azure Data Lake Storage (Medallion Architecture)
- **Analytics**: Azure Synapse Analytics + Microsoft Fabric Lakehouse
- **Security**: Azure Key Vault

### 2. Cloud-Agnostic Data ETL Pipeline (`/cloud_agnostic_data_etl_pipeline`)
- **Cloud**: Multi-cloud support (Azure, AWS, GCP)
- **Orchestration**: Airflow, Prefect, or Terraform
- **Processing**: Databricks/Spark (parameterized)
- **Storage**: Generic data lake/object storage
- **Analytics**: Parameterized star schema
- **Security**: Environment variables/secrets management

## 📊 Dataset

**Source**: [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

**Tables**:
- `olist_customers_dataset.csv`
- `olist_geolocation_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_order_reviews_dataset.csv`
- `olist_orders_dataset.csv`
- `olist_products_dataset.csv`
- `olist_sellers_dataset.csv`
- `product_category_name_translation.csv`

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Kaggle API    │───▶│  Data Ingestion │───▶│   Raw Storage   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                                                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Analytics     │◀───│   Gold Layer    │◀───│  Silver Layer   │
│   (Fabric/      │    │  (Star Schema)  │    │ (Curated Data)  │
│    Synapse)     │    └─────────────────┘    └─────────────────┘
└─────────────────┘                                │
                                                   ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Monitoring    │◀───│   Orchestration │◀───│   Bronze Layer  │
│ (Azure Monitor/ │    │   (ADF/         │    │ (Delta Tables)  │
│  Prometheus)    │    │   Airflow)       │    └─────────────────┘
└─────────────────┘    └─────────────────┘
```

## 📁 Repository Structure

```
etl-pipeline-data-engineering-ci-cd/
├── azure_data_etl_pipeline/
│   ├── adf/                     # Azure Data Factory pipelines
│   ├── databricks/              # Databricks notebooks (Bronze/Silver/Gold)
│   ├── synapse/                 # Synapse SQL scripts
│   ├── fabric/                  # Fabric lakehouse setup
│   ├── keyvault/                # Key Vault configuration
│   ├── cicd/                    # Azure DevOps pipeline
│   ├── monitoring/              # Azure Monitor setup
│   └── data/                    # Data ingestion scripts
├── cloud_agnostic_data_etl_pipeline/
│   ├── orchestration/           # Airflow/Prefect/Terraform
│   ├── databricks/              # Cloud-agnostic notebooks
│   ├── data_lake/               # Multi-cloud storage setup
│   ├── cicd/                    # GitHub Actions
│   ├── monitoring/              # Prometheus/Grafana
│   └── data/                    # Cloud-agnostic ingestion
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

## 🚀 Quick Start

### Prerequisites

- **Python 3.9+**
- **Terraform 1.5.0+**
- **Docker** (optional)
- **Cloud provider CLI tools**
- **Valid cloud provider credentials**

### Azure Pipeline Setup

1. **Configure Azure Credentials**
   ```bash
   export AZURE_SUBSCRIPTION_ID="your-subscription-id"
   export AZURE_TENANT_ID="your-tenant-id"
   export AZURE_CLIENT_ID="your-client-id"
   export AZURE_CLIENT_SECRET="your-client-secret"
   ```

2. **Deploy Infrastructure**
   ```bash
   cd azure_data_etl_pipeline
   # Use Azure Portal or Azure CLI to deploy resources
   az group create --name brazilian-ecommerce-rg --location "East US"
   ```

3. **Run Data Ingestion**
   ```bash
   cd data
   python download_kaggle_data.py
   ```

4. **Deploy ADF Pipeline**
   ```bash
   cd ../adf
   az datafactory pipeline create --resource-group brazilian-ecommerce-rg --factory-name your-adf-name --name Brazilian_Ecommerce_ETL_Pipeline --pipeline-file pipeline_brazilian_ecommerce.json
   ```

### Cloud-Agnostic Pipeline Setup

1. **Configure Cloud Provider**
   ```bash
   # For Azure
   export CLOUD_PROVIDER=azure
   export AZURE_STORAGE_ACCOUNT_NAME="your-storage-account"
   
   # For AWS
   export CLOUD_PROVIDER=aws
   export AWS_S3_BUCKET_NAME="your-bucket"
   
   # For GCP
   export CLOUD_PROVIDER=gcp
   export GCP_BUCKET_NAME="your-bucket"
   ```

2. **Deploy Infrastructure with Terraform**
   ```bash
   cd cloud_agnostic_data_etl_pipeline/orchestration/terraform
   terraform init
   terraform plan -var cloud_provider=azure
   terraform apply -var cloud_provider=azure -auto-approve
   ```

3. **Run Data Ingestion**
   ```bash
   cd ../../data
   python download_kaggle_data.py
   ```

4. **Setup Orchestration**
   ```bash
   # Airflow
   cd ../orchestration/airflow
   airflow dags trigger brazilian_ecommerce_etl_pipeline
   
   # OR Prefect
   cd ../prefect
   prefect deployment run brazilian-ecommerce-etl-production
   ```

## 🏗️ Data Architecture

### Medallion Architecture

```
┌─────────────────┐
│   Raw Layer     │ ← CSV files from Kaggle
│   (bronze)      │ └─ Schema validation
│                 │   └─ Data quality checks
└─────────────────┘
        │
        ▼
┌─────────────────┐
│  Silver Layer   │ ← Business logic applied
│  (silver)       │ └─ Data enrichment
│                 │   └─ Derived columns
└─────────────────┘
        │
        ▼
┌─────────────────┐
│   Gold Layer    │ ← Star schema for analytics
│   (gold)        │ └─ Fact and dimension tables
│                 │   └─ Optimized for queries
└─────────────────┘
```

### Star Schema

```
                    ┌─────────────────┐
                    │   fact_orders   │
                    │                 │
                    │ • order_id      │
                    │ • customer_id   │
                    │ • product_id    │
                    │ • seller_id     │
                    │ • date_key      │
                    │ • revenue       │
                    │ • quantity      │
                    └─────────────────┘
                           │
            ┌──────────────┼──────────────┐
            ▼              ▼              ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│  dim_customers  │ │  dim_products  │ │   dim_sellers  │
│                 │ │                 │ │                 │
│ • customer_key  │ │ • product_key   │ │ • seller_key    │
│ • customer_id   │ │ • product_id    │ │ • seller_id     │
│ • city          │ │ • category      │ │ • city          │
│ • state         │ │ • weight        │ │ • state         │
└─────────────────┘ └─────────────────┘ └─────────────────┘
```

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `CLOUD_PROVIDER` | Cloud provider (azure/aws/gcp) | `azure` |
| `AZURE_STORAGE_ACCOUNT_NAME` | Azure storage account | - |
| `AWS_S3_BUCKET_NAME` | AWS S3 bucket | - |
| `GCP_BUCKET_NAME` | GCP bucket | - |
| `DATABRICKS_HOST` | Databricks workspace URL | - |
| `DATABRICKS_TOKEN` | Databricks API token | - |
| `KAGGLE_DATASET` | Kaggle dataset name | `olistbr/brazilian-ecommerce` |
| `SLACK_WEBHOOK_URL` | Slack webhook for notifications | - |

### Terraform Variables

```hcl
# Cloud Provider
cloud_provider = "azure"
environment    = "dev"

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
databricks_host  = "https://your-workspace.cloud.databricks.com"
databricks_token = "your-databricks-token"
```

## 📊 Monitoring and Logging

### Azure Pipeline Monitoring

- **Azure Monitor**: Metrics, logs, and alerts
- **Log Analytics Workspace**: Centralized logging
- **Application Insights**: Application performance monitoring
- **Azure Monitor Dashboards**: Custom dashboards

**Key Metrics**:
- Pipeline success/failure rate
- Data processing duration
- Resource utilization
- Cost monitoring
- Data quality metrics

### Cloud-Agnostic Monitoring

- **Prometheus**: Metrics collection
- **Grafana**: Visualization and alerting
- **CloudWatch/Azure Monitor/GCP Cloud Logging**: Cloud-specific metrics

**Alert Rules**:
- Pipeline failure rate > 5%
- Pipeline execution > 1 hour
- Data quality issues
- High compute/storage costs
- Resource availability

## 🔄 CI/CD Pipelines

### Azure DevOps Pipeline (`azure-pipelines.yml`)

**Stages**:
1. **Validate**: Code quality, linting, testing
2. **Build**: Package artifacts
3. **Deploy Dev**: Deploy to development environment
4. **Deploy Staging**: Deploy to staging with integration tests
5. **Deploy Production**: Deploy to production with smoke tests
6. **Monitor**: Setup monitoring and alerts

### GitHub Actions (`github-actions.yml`)

**Jobs**:
1. **Test**: Code quality, security scans, unit tests
2. **Terraform Validate**: Infrastructure validation
3. **Build**: Package artifacts
4. **Deploy**: Environment-specific deployments
5. **Monitoring**: Setup monitoring dashboards

## 🔒 Security

### Azure Pipeline Security

- **Azure Key Vault**: Centralized secret management
- **Managed Identities**: Azure AD authentication
- **Network Security Groups**: Network isolation
- **Storage Encryption**: Data at rest encryption
- **Role-Based Access Control**: Least privilege access

### Cloud-Agnostic Security

- **Environment Variables**: Sensitive data
- **IAM Roles**: Cloud provider roles
- **Network Security**: VPC/subnet isolation
- **Encryption**: Storage and transit encryption
- **Secrets Management**: Cloud provider solutions

## 📈 Performance Optimization

### Data Processing

- **Delta Lake**: ACID transactions, time travel
- **Partitioning**: Date-based partitioning
- **Caching**: Databricks caching
- **Optimization**: Z-ordering, compaction
- **Auto-scaling**: Dynamic resource allocation

### Storage Optimization

- **Compression**: Snappy/Zstandard
- **File Formats**: Parquet/Delta
- **Tiered Storage**: Hot/cold data separation
- **Lifecycle Policies**: Automated data retention

## 💰 Cost Management

### Cost Optimization Strategies

1. **Auto-scaling**: Scale resources based on demand
2. **Spot Instances**: Use discounted compute (AWS/Azure)
3. **Storage Tiers**: Use appropriate storage classes
4. **Resource Scheduling**: Stop resources when not needed
5. **Monitoring**: Track and alert on cost anomalies

### Estimated Monthly Costs (Production)

| Service | Cost Range |
|---------|------------|
| Storage | $50-200 |
| Compute | $200-800 |
| Networking | $20-50 |
| Monitoring | $10-30 |
| **Total** | **$280-1080** |

## 🧪 Testing

### Test Types

1. **Unit Tests**: Component testing
2. **Integration Tests**: End-to-end pipeline testing
3. **Data Quality Tests**: Schema validation, null checks
4. **Performance Tests**: Load testing, benchmarking
5. **Security Tests**: Vulnerability scanning

### Running Tests

```bash
# Unit tests
pytest tests/unit/ --cov=azure_data_etl_pipeline

# Integration tests
pytest tests/integration/ --junitxml=test-results.xml

# Data quality tests
python tests/data_quality/validate_data.py

# Security scans
bandit -r . -f json
safety check
```

## 🚨 Troubleshooting

### Common Issues

1. **Authentication Failures**
   - Verify cloud provider credentials
   - Check service permissions
   - Ensure Key Vault/Secrets Manager access

2. **Data Quality Issues**
   - Check source data integrity
   - Validate schema changes
   - Review transformation logic

3. **Performance Issues**
   - Monitor resource utilization
   - Check query optimization
   - Review partitioning strategy

4. **Pipeline Failures**
   - Check logs for error details
   - Verify data availability
   - Review resource limits

### Log Locations

- **Azure**: Azure Monitor Log Analytics
- **AWS**: CloudWatch Logs
- **GCP**: Cloud Logging
- **Local**: `/logs` directory

## 📚 Documentation

### Additional Documentation

- **[Azure Pipeline Details](azure_data_etl_pipeline/README.md)**
- **[Cloud-Agnostic Pipeline Details](cloud_agnostic_data_etl_pipeline/README.md)**
- **[API Documentation](docs/api/)**
- **[Architecture Diagrams](docs/architecture/)**
- **[Deployment Guide](docs/deployment/)**
- **[Troubleshooting Guide](docs/troubleshooting/)**

## 🤝 Contributing

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/amazing-feature`)
3. **Commit** your changes (`git commit -m 'Add amazing feature'`)
4. **Push** to the branch (`git push origin feature/amazing-feature`)
5. **Open** a Pull Request

### Development Guidelines

- Follow PEP 8 Python style guide
- Add unit tests for new features
- Update documentation
- Use semantic versioning
- Ensure CI/CD pipeline passes

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **Olist** for providing the Brazilian E-Commerce dataset
- **Kaggle** for dataset hosting
- **Azure**, **AWS**, **GCP** for cloud platforms
- **Databricks** for unified analytics platform
- **Apache Airflow** and **Prefect** for workflow orchestration

## 📞 Support

For support and questions:

- **Issues**: [GitHub Issues](https://github.com/your-org/etl-pipeline-data-engineering-ci-cd/issues)
- **Discussions**: [GitHub Discussions](https://github.com/your-org/etl-pipeline-data-engineering-ci-cd/discussions)
- **Email**: data-engineering@your-company.com

---

**🚀 Built with ❤️ by the Data Engineering Team**
