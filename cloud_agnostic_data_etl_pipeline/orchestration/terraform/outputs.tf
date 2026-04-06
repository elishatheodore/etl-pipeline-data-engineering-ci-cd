# =================================================================
# Output Values for Cloud-Agnostic ETL Pipeline Infrastructure
# =================================================================

output "project_name" {
  description = "Project name"
  value       = local.project_name
}

output "environment" {
  description = "Environment name"
  value       = local.environment
}

output "cloud_provider" {
  description = "Selected cloud provider"
  value       = local.cloud_provider
}

# =================================================================
# Storage Outputs
# =================================================================

output "storage_configuration" {
  description = "Storage configuration for the selected cloud provider"
  value = {
    # Azure Storage
    azure = local.is_azure ? {
      storage_account_name = azurerm_storage_account.main[0].name
      resource_group_name  = azurerm_resource_group.main[0].name
      primary_endpoint     = azurerm_storage_account.main[0].primary_blob_endpoint
      containers = {
        raw    = azurerm_storage_container.raw[0].name
        bronze = azurerm_storage_container.bronze[0].name
        silver = azurerm_storage_container.silver[0].name
        gold   = azurerm_storage_container.gold[0].name
      }
      key_vault_name = azurerm_key_vault.main[0].name
      key_vault_uri  = azurerm_key_vault.main[0].vault_uri
    } : null
    
    # AWS S3
    aws = local.is_aws ? {
      buckets = {
        raw    = aws_s3_bucket.raw[0].bucket
        bronze = aws_s3_bucket.bronze[0].bucket
        silver = aws_s3_bucket.silver[0].bucket
        gold   = aws_s3_bucket.gold[0].bucket
      }
      bucket_arns = {
        raw    = aws_s3_bucket.raw[0].arn
        bronze = aws_s3_bucket.bronze[0].arn
        silver = aws_s3_bucket.silver[0].arn
        gold   = aws_s3_bucket.gold[0].arn
      }
      bucket_domains = {
        raw    = aws_s3_bucket.raw[0].bucket_domain_name
        bronze = aws_s3_bucket.bronze[0].bucket_domain_name
        silver = aws_s3_bucket.silver[0].bucket_domain_name
        gold   = aws_s3_bucket.gold[0].bucket_domain_name
      }
      secrets_manager_arn = aws_secretsmanager_secret.main[0].arn
    } : null
    
    # GCP Storage
    gcp = local.is_gcp ? {
      buckets = {
        raw    = google_storage_bucket.raw[0].name
        bronze = google_storage_bucket.bronze[0].name
        silver = google_storage_bucket.silver[0].name
        gold   = google_storage_bucket.gold[0].name
      }
      bucket_urls = {
        raw    = google_storage_bucket.raw[0].url
        bronze = google_storage_bucket.bronze[0].url
        silver = google_storage_bucket.silver[0].url
        gold   = google_storage_bucket.gold[0].url
      }
      self_links = {
        raw    = google_storage_bucket.raw[0].self_link
        bronze = google_storage_bucket.bronze[0].self_link
        silver = google_storage_bucket.silver[0].self_link
        gold   = google_storage_bucket.gold[0].self_link
      }
    } : null
  }
}

# =================================================================
# Databricks Outputs
# =================================================================

output "databricks_configuration" {
  description = "Databricks workspace and cluster configuration"
  value = {
    workspace_url = var.databricks_host
    cluster_id    = databricks_cluster.etl_cluster.id
    cluster_name  = databricks_cluster.etl_cluster.cluster_name
    spark_version = databricks_cluster.etl_cluster.spark_version
    node_type_id  = databricks_cluster.etl_cluster.node_type_id
    
    jobs = {
      bronze_transformation = {
        job_id   = databricks_job.bronze_transformation.id
        job_name = databricks_job.bronze_transformation.name
      }
      silver_transformation = {
        job_id   = databricks_job.silver_transformation.id
        job_name = databricks_job.silver_transformation.name
      }
      gold_transformation = {
        job_id   = databricks_job.gold_transformation.id
        job_name = databricks_job.gold_transformation.name
      }
    }
    
    cluster_autoscale = {
      min_workers = databricks_cluster.etl_cluster.autoscale[0].min_workers
      max_workers = databricks_cluster.etl_cluster.autoscale[0].max_workers
    }
    
    autotermination_minutes = databricks_cluster.etl_cluster.autotermination_minutes
  }
}

# =================================================================
# Monitoring Outputs
# =================================================================

output "monitoring_configuration" {
  description = "Monitoring and logging configuration"
  value = {
    # Azure Monitor
    azure = local.is_azure ? {
      log_analytics_workspace_id   = azurerm_log_analytics_workspace.main[0].id
      log_analytics_workspace_name = azurerm_log_analytics_workspace.main[0].name
      workspace_id                = azurerm_log_analytics_workspace.main[0].workspace_id
      primary_shared_key          = azurerm_log_analytics_workspace.main[0].primary_shared_key
      secondary_shared_key        = azurerm_log_analytics_workspace.main[0].secondary_shared_key
    } : null
    
    # AWS CloudWatch
    aws = local.is_aws ? {
      cloudwatch_log_group_name = aws_cloudwatch_log_group.etl_logs[0].name
      cloudwatch_log_group_arn  = aws_cloudwatch_log_group.etl_logs[0].arn
      region                   = var.aws_region
    } : null
    
    # GCP Cloud Logging
    gcp = local.is_gcp ? {
      project_id = var.gcp_project_id
      region     = var.gcp_region
    } : null
  }
}

# =================================================================
# Network Outputs
# =================================================================

output "network_configuration" {
  description = "Network configuration"
  value = {
    vpc_cidr = var.vpc_cidr
    
    public_subnets = var.public_subnet_cidrs
    private_subnets = var.private_subnet_cidrs
    
    # Additional network details would be added here if VPCs are created
  }
}

# =================================================================
# Security Outputs
# =================================================================

output "security_configuration" {
  description = "Security and encryption configuration"
  value = {
    encryption_enabled = var.enable_encryption
    versioning_enabled = var.enable_versioning
    
    # Azure Key Vault
    azure_key_vault = local.is_azure ? {
      name = azurerm_key_vault.main[0].name
      uri  = azurerm_key_vault.main[0].vault_uri
      id   = azurerm_key_vault.main[0].id
    } : null
    
    # AWS Secrets Manager
    aws_secrets_manager = local.is_aws ? {
      secret_arn = aws_secretsmanager_secret.main[0].arn
      secret_id  = aws_secretsmanager_secret.main[0].id
    } : null
  }
}

# =================================================================
# Cost and Billing Outputs
# =================================================================

output "cost_information" {
  description = "Cost and billing information"
  value = {
    cost_center = var.cost_center
    budget_alert_amount = var.budget_alert_amount
    
    # Resource counts by type
    resource_counts = {
      storage_containers = local.is_azure ? 4 : (local.is_aws ? 4 : (local.is_gcp ? 4 : 0))
      databricks_clusters = 1
      databricks_jobs = 3
      monitoring_resources = var.enable_monitoring ? 1 : 0
    }
    
    # Estimated monthly costs (placeholder - would need actual pricing)
    estimated_monthly_costs = {
      storage = "Varies by usage and provider"
      compute = "Varies by Databricks usage"
      monitoring = var.enable_monitoring ? "Minimal" : "None"
    }
  }
}

# =================================================================
# Environment Setup Commands
# =================================================================

output "setup_commands" {
  description = "Commands to set up the environment for the ETL pipeline"
  value = {
    # Environment variables setup
    environment_setup = [
      "# Set cloud provider",
      local.is_azure ? "export CLOUD_PROVIDER=azure" : "",
      local.is_azure ? "export AZURE_STORAGE_ACCOUNT_NAME=${azurerm_storage_account.main[0].name}" : "",
      local.is_azure ? "export AZURE_TENANT_ID=${var.azure_tenant_id}" : "",
      local.is_azure ? "export AZURE_CLIENT_ID=${var.azure_client_id}" : "",
      
      local.is_aws ? "export CLOUD_PROVIDER=aws" : "",
      local.is_aws ? "export AWS_S3_BUCKET_NAME=${aws_s3_bucket.raw[0].bucket}" : "",
      local.is_aws ? "export AWS_REGION=${var.aws_region}" : "",
      
      local.is_gcp ? "export CLOUD_PROVIDER=gcp" : "",
      local.is_gcp ? "export GCP_BUCKET_NAME=${google_storage_bucket.raw[0].name}" : "",
      local.is_gcp ? "export GCP_PROJECT_ID=${var.gcp_project_id}" : "",
      
      "# Databricks configuration",
      "export DATABRICKS_HOST=${var.databricks_host}",
      "export DATABRICKS_TOKEN=${var.databricks_token}",
      
      "# Dataset configuration",
      "export KAGGLE_DATASET=${var.kaggle_dataset}",
      "export CONTAINER_NAME=brazilian-ecommerce"
    ]
    
    # Data ingestion commands
    data_ingestion = [
      "# Install dependencies",
      "pip install -r requirements.txt",
      "",
      "# Run data ingestion",
      "python cloud_agnostic_data_etl_pipeline/data/download_kaggle_data.py",
      "",
      "# Verify data upload",
      "# Check storage containers/buckets for uploaded files"
    ]
    
    # Orchestration commands
    orchestration_setup = [
      "# Airflow setup (if using Airflow)",
      "export AIRFLOW__CORE__EXECUTOR=LocalExecutor",
      "export AIRFLOW__CORE__SQL_ALCHEMY_CONN=sqlite:////airflow/airflow.db",
      "airflow db init",
      "airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com",
      "",
      "# Copy DAG file",
      "cp cloud_agnostic_data_etl_pipeline/orchestration/airflow/brazilian_ecommerce_dag.py $AIRFLOW_HOME/dags/",
      "",
      "# Start Airflow",
      "airflow scheduler &",
      "airflow webserver --port 8080",
      "",
      "# Or Prefect setup (if using Prefect)",
      "pip install prefect",
      "prefect config set PREFECT_API_URL=http://localhost:4200/api",
      "prefect server start",
      "",
      "# Deploy Prefect flow",
      "python cloud_agnostic_data_etl_pipeline/orchestration/prefect/brazilian_ecommerce_flow.py"
    ]
    
    # Terraform commands
    terraform_commands = [
      "# Initialize Terraform",
      "cd cloud_agnostic_data_etl_pipeline/orchestration/terraform",
      "terraform init",
      "",
      "# Plan infrastructure",
      "terraform plan -var cloud_provider=${local.cloud_provider}",
      "",
      "# Apply infrastructure",
      "terraform apply -var cloud_provider=${local.cloud_provider} -auto-approve",
      "",
      "# Destroy infrastructure (when needed)",
      "terraform destroy -var cloud_provider=${local.cloud_provider} -auto-approve"
    ]
  }
}

# =================================================================
# Quick Start Guide
# =================================================================

output "quick_start_guide" {
  description = "Quick start guide for the ETL pipeline"
  value = {
    steps = [
      "1. Configure cloud provider credentials",
      "2. Run Terraform to provision infrastructure",
      "3. Set up environment variables",
      "4. Run data ingestion script",
      "5. Configure orchestration (Airflow or Prefect)",
      "6. Start the ETL pipeline",
      "7. Monitor pipeline execution"
    ]
    
    prerequisites = [
      "Terraform >= 1.5.0",
      "Python >= 3.8",
      "Docker (optional)",
      "Cloud provider CLI tools",
      "Valid cloud provider credentials"
    ]
    
    verification_steps = [
      "Check storage containers/buckets for data",
      "Verify Databricks jobs are created",
      "Test orchestration DAG/flow",
      "Check monitoring dashboards",
      "Validate data quality results"
    ]
  }
}

# =================================================================
# Troubleshooting Information
# =================================================================

output "troubleshooting" {
  description = "Troubleshooting information and common issues"
  value = {
    common_issues = {
      authentication = [
        "Ensure cloud provider credentials are properly configured",
        "Check service principal/role permissions",
        "Verify Key Vault/Secrets Manager access"
      ]
      networking = [
        "Check VPC/subnet configurations",
        "Verify firewall rules",
        "Ensure connectivity between services"
      ]
      storage = [
        "Verify container/bucket permissions",
        "Check storage account limits",
        "Ensure proper encryption settings"
      ]
      databricks = [
        "Check workspace connectivity",
        "Verify cluster permissions",
        "Ensure notebook paths are correct"
      ]
    }
    
    log_locations = {
      azure = "Azure Monitor Log Analytics Workspace"
      aws   = "AWS CloudWatch Logs"
      gcp   = "Google Cloud Logging"
      local = "Pipeline logs in /logs directory"
    }
    
    support_contacts = [
      "Data Engineering Team",
      "Cloud Platform Team",
      "Infrastructure Support"
    ]
  }
}
