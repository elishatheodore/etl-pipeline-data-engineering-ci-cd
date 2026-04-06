# =================================================================
# Cloud-Agnostic ETL Pipeline Infrastructure
# Brazilian E-Commerce Data Engineering
# =================================================================

terraform {
  required_version = ">= 1.5.0"
  
  required_providers {
    # Cloud providers
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    
    # Data and analytics
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
    
    # Monitoring and logging
    datadog = {
      source  = "datadog/datadog"
      version = "~> 3.0"
    }
    grafana = {
      source  = "grafana/grafana"
      version = "~> 2.0"
    }
    
    # Utilities
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.0"
    }
  }
}

# =================================================================
# Provider Configuration
# =================================================================

# Determine cloud provider from variable
locals {
  cloud_provider = var.cloud_provider
  
  # Cloud-specific configuration
  is_azure   = local.cloud_provider == "azure"
  is_aws     = local.cloud_provider == "aws"
  is_gcp     = local.cloud_provider == "gcp"
}

# Azure Provider
provider "azurerm" {
  features {}
  skip_provider_registration = true
  
  dynamic "subscription_id" {
    for_each = local.is_azure ? [1] : []
    content {
      value = var.azure_subscription_id
    }
  }
  
  dynamic "tenant_id" {
    for_each = local.is_azure ? [1] : []
    content {
      value = var.azure_tenant_id
    }
  }
  
  dynamic "client_id" {
    for_each = local.is_azure ? [1] : []
    content {
      value = var.azure_client_id
    }
  }
  
  dynamic "client_secret" {
    for_each = local.is_azure ? [1] : []
    content {
      value = var.azure_client_secret
    }
  }
}

# AWS Provider
provider "aws" {
  region = var.aws_region
  
  dynamic "access_key" {
    for_each = local.is_aws ? [1] : []
    content {
      value = var.aws_access_key
    }
  }
  
  dynamic "secret_key" {
    for_each = local.is_aws ? [1] : []
    content {
      value = var.aws_secret_key
    }
  }
}

# Google Cloud Provider
provider "google" {
  project = var.gcp_project_id
  region  = var.gcp_region
  
  dynamic "credentials" {
    for_each = local.is_gcp ? [1] : []
    content {
      value = var.gcp_credentials
    }
  }
}

# Databricks Provider (cloud-agnostic)
provider "databricks" {
  host = var.databricks_host
  
  dynamic "token" {
    for_each = var.databricks_token != "" ? [1] : []
    content {
      value = var.databricks_token
    }
  }
}

# =================================================================
# Local Variables and Naming
# =================================================================

locals {
  project_name = "brazilian-ecommerce"
  environment  = var.environment
  
  # Naming convention
  name_prefix = "${local.project_name}-${local.environment}"
  
  # Tags for all resources
  common_tags = {
    Project     = local.project_name
    Environment = local.environment
    ManagedBy   = "terraform"
    Owner       = "data-engineering"
    CreatedAt   = timestamp()
  }
  
  # Resource suffix for uniqueness
  resource_suffix = substr(md5(local.name_prefix), 0, 8)
}

# =================================================================
# Azure Resources (if selected)
# =================================================================

# Resource Group
resource "azurerm_resource_group" "main" {
  count = local.is_azure ? 1 : 0
  
  name     = "${local.name_prefix}-rg"
  location = var.azure_location
  
  tags = local.common_tags
}

# Storage Account (Data Lake Gen2)
resource "azurerm_storage_account" "main" {
  count = local.is_azure ? 1 : 0
  
  name                     = "${replace(local.name_prefix, "-", "")}st${local.resource_suffix}"
  resource_group_name      = azurerm_resource_group.main[0].name
  location                = azurerm_resource_group.main[0].location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  
  # Enable hierarchical namespace for Data Lake
  is_hns_enabled = true
  
  # Enable data protection
  min_tls_version = "TLS1_2"
  
  tags = local.common_tags
}

# Storage Containers
resource "azurerm_storage_container" "raw" {
  count = local.is_azure ? 1 : 0
  
  name                  = "raw"
  storage_account_name  = azurerm_storage_account.main[0].name
  container_access_type = "private"
}

resource "azurerm_storage_container" "bronze" {
  count = local.is_azure ? 1 : 0
  
  name                  = "bronze"
  storage_account_name  = azurerm_storage_account.main[0].name
  container_access_type = "private"
}

resource "azurerm_storage_container" "silver" {
  count = local.is_azure ? 1 : 0
  
  name                  = "silver"
  storage_account_name  = azurerm_storage_account.main[0].name
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold" {
  count = local.is_azure ? 1 : 0
  
  name                  = "gold"
  storage_account_name  = azurerm_storage_account.main[0].name
  container_access_type = "private"
}

# Key Vault for secrets
resource "azurerm_key_vault" "main" {
  count = local.is_azure ? 1 : 0
  
  name                = "${replace(local.name_prefix, "-", "")}-kv-${local.resource_suffix}"
  resource_group_name = azurerm_resource_group.main[0].name
  location            = azurerm_resource_group.main[0].location
  tenant_id           = var.azure_tenant_id
  sku_name            = "standard"
  
  soft_delete_retention_days = 7
  purge_protection_enabled  = false
  
  access_policy {
    tenant_id = var.azure_tenant_id
    object_id = var.azure_object_id
    
    key_permissions = [
      "Get", "List", "Create", "Delete", "Update"
    ]
    
    secret_permissions = [
      "Get", "List", "Set", "Delete"
    ]
  }
  
  tags = local.common_tags
}

# =================================================================
# AWS Resources (if selected)
# =================================================================

# S3 Buckets
resource "aws_s3_bucket" "raw" {
  count = local.is_aws ? 1 : 0
  
  bucket = "${local.name_prefix}-raw-${local.resource_suffix}"
  
  tags = local.common_tags
}

resource "aws_s3_bucket_versioning" "raw" {
  count = local.is_aws ? 1 : 0
  
  bucket = aws_s3_bucket.raw[0].id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket" "bronze" {
  count = local.is_aws ? 1 : 0
  
  bucket = "${local.name_prefix}-bronze-${local.resource_suffix}"
  
  tags = local.common_tags
}

resource "aws_s3_bucket" "silver" {
  count = local.is_aws ? 1 : 0
  
  bucket = "${local.name_prefix}-silver-${local.resource_suffix}"
  
  tags = local.common_tags
}

resource "aws_s3_bucket" "gold" {
  count = local.is_aws ? 1 : 0
  
  bucket = "${local.name_prefix}-gold-${local.resource_suffix}"
  
  tags = local.common_tags
}

# AWS Secrets Manager
resource "aws_secretsmanager_secret" "main" {
  count = local.is_aws ? 1 : 0
  
  name = "${local.name_prefix}-secrets"
  
  tags = local.common_tags
}

# =================================================================
# GCP Resources (if selected)
# =================================================================

# GCS Buckets
resource "google_storage_bucket" "raw" {
  count = local.is_gcp ? 1 : 0
  
  name          = "${local.name_prefix}-raw-${local.resource_suffix}"
  location      = var.gcp_location
  storage_class = "STANDARD"
  
  uniform_bucket_level_access = true
  
  labels = local.common_tags
}

resource "google_storage_bucket" "bronze" {
  count = local.is_gcp ? 1 : 0
  
  name          = "${local.name_prefix}-bronze-${local.resource_suffix}"
  location      = var.gcp_location
  storage_class = "STANDARD"
  
  uniform_bucket_level_access = true
  
  labels = local.common_tags
}

resource "google_storage_bucket" "silver" {
  count = local.is_gcp ? 1 : 0
  
  name          = "${local.name_prefix}-silver-${local.resource_suffix}"
  location      = var.gcp_location
  storage_class = "STANDARD"
  
  uniform_bucket_level_access = true
  
  labels = local.common_tags
}

resource "google_storage_bucket" "gold" {
  count = local.is_gcp ? 1 : 0
  
  name          = "${local.name_prefix}-gold-${local.resource_suffix}"
  location      = var.gcp_location
  storage_class = "STANDARD"
  
  uniform_bucket_level_access = true
  
  labels = local.common_tags
}

# =================================================================
# Databricks Resources (Cloud-Agnostic)
# =================================================================

# Databricks Workspace (if not using existing)
resource "databricks_workspace" "main" {
  count = var.create_databricks_workspace ? 1 : 0
  
  name        = "${local.name_prefix}-workspace"
  sku         = var.databricks_sku
  
  dynamic "aws_location" {
    for_each = local.is_aws ? [1] : []
    content {
      region = var.aws_region
    }
  }
  
  dynamic "azure_location" {
    for_each = local.is_azure ? [1] : []
    content {
      resource_group = azurerm_resource_group.main[0].name
      location       = azurerm_resource_group.main[0].location
    }
  }
  
  dynamic "gcp_location" {
    for_each = local.is_gcp ? [1] : []
    content {
      region = var.gcp_region
    }
  }
  
  tags = local.common_tags
}

# Databricks Cluster
resource "databricks_cluster" "etl_cluster" {
  cluster_name = "${local.name_prefix}-etl-cluster"
  
  spark_version = var.databricks_spark_version
  
  node_type_id = var.databricks_node_type
  
  autoscale {
    min_workers = var.databricks_min_workers
    max_workers = var.databricks_max_workers
  }
  
  autotermination_minutes = var.databricks_autotermination_minutes
  
  spark_conf = {
    "spark.sql.legacy.allowUntypedScalaUDF" = "true"
    "spark.databricks.delta.preview.enabled" = "true"
  }
  
  custom_tags = local.common_tags
}

# Databricks Jobs
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
      source_path     = "raw"
      bronze_path     = "bronze"
    }
  }
}

resource "databricks_job" "silver_transformation" {
  name = "${local.name_prefix}-silver-transformation"
  
  new_cluster {
    spark_version = var.databricks_spark_version
    node_type_id  = var.databricks_node_type
    num_workers   = 2
  }
  
  notebook_task {
    notebook_path = "/ETL/03_Silver_Layer/silver_transformation"
    base_parameters = {
      storage_account = local.is_azure ? azurerm_storage_account.main[0].name : ""
      container_name  = "silver"
      bronze_path     = "bronze"
      silver_path     = "silver"
    }
  }
}

resource "databricks_job" "gold_transformation" {
  name = "${local.name_prefix}-gold-transformation"
  
  new_cluster {
    spark_version = var.databricks_spark_version
    node_type_id  = var.databricks_node_type
    num_workers   = 2
  }
  
  notebook_task {
    notebook_path = "/ETL/04_Gold_Layer/gold_transformation"
    base_parameters = {
      storage_account = local.is_azure ? azurerm_storage_account.main[0].name : ""
      container_name  = "gold"
      silver_path     = "silver"
      gold_path       = "gold"
    }
  }
}

# =================================================================
# Monitoring and Logging
# =================================================================

# CloudWatch Log Group (AWS)
resource "aws_cloudwatch_log_group" "etl_logs" {
  count = local.is_aws ? 1 : 0
  
  name              = "/aws/batch/${local.name_prefix}"
  retention_in_days = 30
  
  tags = local.common_tags
}

# Azure Monitor Log Analytics Workspace
resource "azurerm_log_analytics_workspace" "main" {
  count = local.is_azure ? 1 : 0
  
  name                = "${replace(local.name_prefix, "-", "")}-law"
  location            = azurerm_resource_group.main[0].location
  resource_group_name = azurerm_resource_group.main[0].name
  sku                 = "PerGB2018"
  retention_in_days   = 30
  
  tags = local.common_tags
}

# =================================================================
# Output Values
# =================================================================

output "storage_info" {
  description = "Storage configuration information"
  value = {
    cloud_provider = local.cloud_provider
    
    # Azure
    azure = local.is_azure ? {
      storage_account_name = azurerm_storage_account.main[0].name
      resource_group_name  = azurerm_resource_group.main[0].name
      key_vault_name       = azurerm_key_vault.main[0].name
      containers           = ["raw", "bronze", "silver", "gold"]
    } : null
    
    # AWS
    aws = local.is_aws ? {
      buckets = {
        raw    = aws_s3_bucket.raw[0].bucket
        bronze = aws_s3_bucket.bronze[0].bucket
        silver = aws_s3_bucket.silver[0].bucket
        gold   = aws_s3_bucket.gold[0].bucket
      }
    } : null
    
    # GCP
    gcp = local.is_gcp ? {
      buckets = {
        raw    = google_storage_bucket.raw[0].name
        bronze = google_storage_bucket.bronze[0].name
        silver = google_storage_bucket.silver[0].name
        gold   = google_storage_bucket.gold[0].name
      }
    } : null
  }
}

output "databricks_info" {
  description = "Databricks configuration information"
  value = {
    workspace_url = var.databricks_host
    cluster_id    = databricks_cluster.etl_cluster.id
    job_ids = {
      bronze_transformation = databricks_job.bronze_transformation.id
      silver_transformation = databricks_job.silver_transformation.id
      gold_transformation   = databricks_job.gold_transformation.id
    }
  }
}

output "monitoring_info" {
  description = "Monitoring configuration information"
  value = {
    cloud_provider = local.cloud_provider
    
    # Azure
    azure = local.is_azure ? {
      log_analytics_workspace_id = azurerm_log_analytics_workspace.main[0].id
      log_analytics_workspace_name = azurerm_log_analytics_workspace.main[0].name
    } : null
    
    # AWS
    aws = local.is_aws ? {
      cloudwatch_log_group = aws_cloudwatch_log_group.etl_logs[0].name
    } : null
  }
}

output "configuration_commands" {
  description = "Configuration commands for the pipeline"
  value = {
    # Environment setup commands
    setup_commands = [
      "# Set environment variables",
      local.is_azure ? "export CLOUD_PROVIDER=azure" : "",
      local.is_azure ? "export AZURE_STORAGE_ACCOUNT_NAME=${azurerm_storage_account.main[0].name}" : "",
      local.is_aws ? "export CLOUD_PROVIDER=aws" : "",
      local.is_aws ? "export AWS_S3_BUCKET_NAME=${aws_s3_bucket.raw[0].bucket}" : "",
      local.is_gcp ? "export CLOUD_PROVIDER=gcp" : "",
      local.is_gcp ? "export GCP_BUCKET_NAME=${google_storage_bucket.raw[0].name}" : "",
      "",
      "# Run data ingestion",
      "python data/download_kaggle_data.py",
      "",
      "# Run orchestration (choose one):",
      "# Airflow: airflow dags trigger brazilian_ecommerce_etl_pipeline",
      "# Prefect: prefect deployment run brazilian-ecommerce-etl-production"
    ]
  }
}
