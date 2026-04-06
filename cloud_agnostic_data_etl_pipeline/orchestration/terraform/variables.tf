# =================================================================
# Variables for Cloud-Agnostic ETL Pipeline Infrastructure
# =================================================================

variable "cloud_provider" {
  description = "Cloud provider to use (azure, aws, gcp)"
  type        = string
  default     = "azure"
  
  validation {
    condition     = contains(["azure", "aws", "gcp"], var.cloud_provider)
    error_message = "Cloud provider must be one of: azure, aws, gcp."
  }
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
  
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

# =================================================================
# Azure Configuration
# =================================================================

variable "azure_subscription_id" {
  description = "Azure subscription ID"
  type        = string
  default     = ""
}

variable "azure_tenant_id" {
  description = "Azure tenant ID"
  type        = string
  default     = ""
}

variable "azure_client_id" {
  description = "Azure client ID (service principal)"
  type        = string
  default     = ""
}

variable "azure_client_secret" {
  description = "Azure client secret"
  type        = string
  default     = ""
  sensitive   = true
}

variable "azure_object_id" {
  description = "Azure object ID for Key Vault access"
  type        = string
  default     = ""
}

variable "azure_location" {
  description = "Azure region for resources"
  type        = string
  default     = "East US"
  
  validation {
    condition = contains([
      "East US", "West US", "Central US", "North Europe", 
      "West Europe", "East Asia", "Southeast Asia"
    ], var.azure_location)
    error_message = "Azure location must be a valid Azure region."
  }
}

# =================================================================
# AWS Configuration
# =================================================================

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
  
  validation {
    condition = contains([
      "us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"
    ], var.aws_region)
    error_message = "AWS region must be a valid AWS region."
  }
}

variable "aws_access_key" {
  description = "AWS access key"
  type        = string
  default     = ""
  sensitive   = true
}

variable "aws_secret_key" {
  description = "AWS secret key"
  type        = string
  default     = ""
  sensitive   = true
}

# =================================================================
# GCP Configuration
# =================================================================

variable "gcp_project_id" {
  description = "GCP project ID"
  type        = string
  default     = ""
}

variable "gcp_region" {
  description = "GCP region for resources"
  type        = string
  default     = "us-central1"
  
  validation {
    condition = contains([
      "us-central1", "us-east1", "us-west1", "europe-west1", "asia-southeast1"
    ], var.gcp_region)
    error_message = "GCP region must be a valid GCP region."
  }
}

variable "gcp_location" {
  description = "GCP location for storage buckets"
  type        = string
  default     = "US"
  
  validation {
    condition = contains(["US", "EU", "ASIA"], var.gcp_location)
    error_message = "GCP location must be one of: US, EU, ASIA."
  }
}

variable "gcp_credentials" {
  description = "GCP service account credentials (JSON)"
  type        = string
  default     = ""
  sensitive   = true
}

# =================================================================
# Databricks Configuration
# =================================================================

variable "databricks_host" {
  description = "Databricks workspace URL"
  type        = string
  default     = ""
}

variable "databricks_token" {
  description = "Databricks API token"
  type        = string
  default     = ""
  sensitive   = true
}

variable "create_databricks_workspace" {
  description = "Whether to create a new Databricks workspace"
  type        = bool
  default     = false
}

variable "databricks_sku" {
  description = "Databricks workspace SKU"
  type        = string
  default     = "standard"
  
  validation {
    condition = contains(["standard", "premium"], var.databricks_sku)
    error_message = "Databricks SKU must be either standard or premium."
  }
}

variable "databricks_spark_version" {
  description = "Databricks Spark version"
  type        = string
  default     = "11.3.x-scala2.12"
}

variable "databricks_node_type" {
  description = "Databricks cluster node type"
  type        = string
  default     = "Standard_DS3_v2"
}

variable "databricks_min_workers" {
  description = "Minimum number of workers for Databricks cluster"
  type        = number
  default     = 1
  
  validation {
    condition     = var.databricks_min_workers >= 0
    error_message = "Minimum workers must be >= 0."
  }
}

variable "databricks_max_workers" {
  description = "Maximum number of workers for Databricks cluster"
  type        = number
  default     = 4
  
  validation {
    condition     = var.databricks_max_workers >= var.databricks_min_workers
    error_message = "Maximum workers must be >= minimum workers."
  }
}

variable "databricks_autotermination_minutes" {
  description = "Auto-termination time for Databricks cluster in minutes"
  type        = number
  default     = 30
  
  validation {
    condition     = var.databricks_autotermination_minutes >= 10
    error_message = "Auto-termination must be at least 10 minutes."
  }
}

# =================================================================
# Monitoring Configuration
# =================================================================

variable "enable_monitoring" {
  description = "Whether to enable monitoring and logging"
  type        = bool
  default     = true
}

variable "log_retention_days" {
  description = "Number of days to retain logs"
  type        = number
  default     = 30
  
  validation {
    condition     = var.log_retention_days >= 1 && var.log_retention_days <= 365
    error_message = "Log retention must be between 1 and 365 days."
  }
}

# =================================================================
# Network Configuration
# =================================================================

variable "vpc_cidr" {
  description = "CIDR block for VPC (AWS/GCP)"
  type        = string
  default     = "10.0.0.0/16"
  
  validation {
    condition     = can(cidrhost(var.vpc_cidr, 0))
    error_message = "VPC CIDR must be a valid CIDR block."
  }
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24"]
  
  validation {
    condition     = alltrue([for cidr in var.public_subnet_cidrs : can(cidrhost(cidr, 0))])
    error_message = "All public subnet CIDRs must be valid CIDR blocks."
  }
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for private subnets"
  type        = list(string)
  default     = ["10.0.10.0/24", "10.0.20.0/24"]
  
  validation {
    condition     = alltrue([for cidr in var.private_subnet_cidrs : can(cidrhost(cidr, 0))])
    error_message = "All private subnet CIDRs must be valid CIDR blocks."
  }
}

# =================================================================
# Security Configuration
# =================================================================

variable "enable_encryption" {
  description = "Whether to enable encryption for storage"
  type        = bool
  default     = true
}

variable "encryption_key_arn" {
  description = "ARN of custom encryption key (optional)"
  type        = string
  default     = ""
}

variable "enable_versioning" {
  description = "Whether to enable versioning for storage buckets"
  type        = bool
  default     = true
}

variable "enable_mfa_delete" {
  description = "Whether to enable MFA delete for S3 buckets"
  type        = bool
  default     = false
}

# =================================================================
# Cost Management
# =================================================================

variable "cost_center" {
  description = "Cost center for resource tagging"
  type        = string
  default     = "data-engineering"
}

variable "budget_alert_amount" {
  description = "Budget alert amount in USD"
  type        = number
  default     = 1000
  
  validation {
    condition     = var.budget_alert_amount > 0
    error_message = "Budget alert amount must be greater than 0."
  }
}

variable "budget_alert_emails" {
  description = "Email addresses for budget alerts"
  type        = list(string)
  default     = []
}

# =================================================================
# Data Pipeline Configuration
# =================================================================

variable "kaggle_dataset" {
  description = "Kaggle dataset name"
  type        = string
  default     = "olistbr/brazilian-ecommerce"
}

variable "data_retention_days" {
  description = "Number of days to retain raw data"
  type        = number
  default     = 90
  
  validation {
    condition     = var.data_retention_days >= 1
    error_message = "Data retention must be at least 1 day."
  }
}

variable "enable_data_quality_checks" {
  description = "Whether to enable data quality checks"
  type        = bool
  default     = true
}

variable "enable_anomaly_detection" {
  description = "Whether to enable anomaly detection"
  type        = bool
  default     = false
}

# =================================================================
# Notification Configuration
# =================================================================

variable "slack_webhook_url" {
  description = "Slack webhook URL for notifications"
  type        = string
  default     = ""
  sensitive   = true
}

variable "notification_emails" {
  description = "Email addresses for pipeline notifications"
  type        = list(string)
  default     = []
}

variable "enable_success_notifications" {
  description = "Whether to send success notifications"
  type        = bool
  default     = true
}

variable "enable_failure_notifications" {
  description = "Whether to send failure notifications"
  type        = bool
  default     = true
}

# =================================================================
# Advanced Configuration
# =================================================================

variable "tags" {
  description = "Additional tags for resources"
  type        = map(string)
  default     = {}
}

variable "enable_terraform_state_locking" {
  description = "Whether to enable Terraform state locking"
  type        = bool
  default     = true
}

variable "terraform_state_bucket" {
  description = "S3 bucket for Terraform state (AWS)"
  type        = string
  default     = ""
}

variable "terraform_state_container" {
  description = "Storage container for Terraform state (Azure)"
  type        = string
  default     = ""
}

variable "terraform_state_bucket_gcp" {
  description = "GCS bucket for Terraform state (GCP)"
  type        = string
  default     = ""
}

variable "enable_remote_backend" {
  description = "Whether to use remote Terraform backend"
  type        = bool
  default     = false
}
