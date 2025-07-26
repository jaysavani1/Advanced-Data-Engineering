# Olympic Analytics Platform - Infrastructure as Code
# This Terraform configuration creates all necessary Azure resources

terraform {
  required_version = ">= 1.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
  
  backend "azurerm" {
    # Configure backend storage for Terraform state
    # resource_group_name  = "terraform-state-rg"
    # storage_account_name = "tfstateolympic"
    # container_name       = "tfstate"
    # key                  = "olympic-analytics.terraform.tfstate"
  }
}

provider "azurerm" {
  features {}
}

# Variables
variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "East US"
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "olympic"
}

# Local variables
locals {
  resource_group_name = "${var.project_name}-analytics-${var.environment}-rg"
  storage_account_name = "${var.project_name}storage${var.environment}"
  databricks_workspace_name = "${var.project_name}-db-${var.environment}"
  synapse_workspace_name = "${var.project_name}-synapse-${var.environment}"
  event_hub_namespace = "${var.project_name}-events-${var.environment}"
  key_vault_name = "${var.project_name}-kv-${var.environment}"
  data_factory_name = "${var.project_name}-adf-${var.environment}"
  
  tags = {
    Environment = var.environment
    Project     = "Olympic Analytics"
    ManagedBy   = "Terraform"
    Owner       = "Data Team"
  }
}

# Resource Group
resource "azurerm_resource_group" "main" {
  name     = local.resource_group_name
  location = var.location
  
  tags = local.tags
}

# Key Vault
resource "azurerm_key_vault" "main" {
  name                        = local.key_vault_name
  location                    = azurerm_resource_group.main.location
  resource_group_name         = azurerm_resource_group.main.name
  enabled_for_disk_encryption = true
  tenant_id                   = data.azurerm_client_config.current.tenant_id
  soft_delete_retention_days  = 7
  purge_protection_enabled    = false
  
  sku_name = "standard"
  
  tags = local.tags
}

# Key Vault Access Policy
resource "azurerm_key_vault_access_policy" "terraform" {
  key_vault_id = azurerm_key_vault.main.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = data.azurerm_client_config.current.object_id
  
  key_permissions = [
    "Get", "List", "Create", "Delete", "Update"
  ]
  
  secret_permissions = [
    "Get", "List", "Set", "Delete"
  ]
  
  certificate_permissions = [
    "Get", "List", "Create", "Delete", "Update"
  ]
}

# Storage Account
resource "azurerm_storage_account" "main" {
  name                     = local.storage_account_name
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  
  is_hns_enabled = true  # Enable hierarchical namespace for Data Lake
  
  tags = local.tags
}

# Storage Containers
resource "azurerm_storage_data_lake_gen2_filesystem" "raw" {
  name               = "raw-data"
  storage_account_id = azurerm_storage_account.main.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "processed" {
  name               = "processed-data"
  storage_account_id = azurerm_storage_account.main.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "curated" {
  name               = "curated-data"
  storage_account_id = azurerm_storage_account.main.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "checkpoints" {
  name               = "checkpoints"
  storage_account_id = azurerm_storage_account.main.id
}

# Event Hubs Namespace
resource "azurerm_eventhub_namespace" "main" {
  name                = local.event_hub_namespace
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "Standard"
  capacity            = 1
  
  auto_inflate_enabled     = true
  maximum_throughput_units = 10
  
  tags = local.tags
}

# Event Hub
resource "azurerm_eventhub" "olympics_data" {
  name                = "olympics-data"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.main.name
  partition_count     = 4
  message_retention   = 7
}

resource "azurerm_eventhub" "real_time_events" {
  name                = "real-time-events"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.main.name
  partition_count     = 8
  message_retention   = 7
}

# Databricks Workspace
resource "azurerm_databricks_workspace" "main" {
  name                = local.databricks_workspace_name
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = "standard"
  
  tags = local.tags
}

# Synapse Analytics Workspace
resource "azurerm_synapse_workspace" "main" {
  name                                 = local.synapse_workspace_name
  resource_group_name                  = azurerm_resource_group.main.name
  location                             = azurerm_resource_group.main.location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.curated.id
  
  sql_administrator_login          = "sqladminuser"
  sql_administrator_login_password = random_password.sql_password.result
  
  identity {
    type = "SystemAssigned"
  }
  
  tags = local.tags
}

# Synapse SQL Pool
resource "azurerm_synapse_sql_pool" "main" {
  name                 = "olympic-sql-pool"
  synapse_workspace_id = azurerm_synapse_workspace.main.id
  sku_name             = "DW100c"
  create_mode          = "Default"
  
  tags = local.tags
}

# Synapse Spark Pool
resource "azurerm_synapse_spark_pool" "main" {
  name                 = "olympic-spark-pool"
  synapse_workspace_id = azurerm_synapse_workspace.main.id
  node_size_family     = "MemoryOptimized"
  node_size            = "Small"
  
  auto_scale {
    max_node_count = 3
    min_node_count = 1
  }
  
  auto_pause {
    delay_in_minutes = 15
  }
  
  tags = local.tags
}

# Data Factory
resource "azurerm_data_factory" "main" {
  name                = local.data_factory_name
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  
  identity {
    type = "SystemAssigned"
  }
  
  tags = local.tags
}

# Application Insights
resource "azurerm_application_insights" "main" {
  name                = "${var.project_name}-appinsights-${var.environment}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  application_type    = "web"
  
  tags = local.tags
}

# Random password for SQL admin
resource "random_password" "sql_password" {
  length  = 16
  special = true
}

# Store secrets in Key Vault
resource "azurerm_key_vault_secret" "sql_password" {
  name         = "synapse-sql-password"
  value        = random_password.sql_password.result
  key_vault_id = azurerm_key_vault.main.id
  
  depends_on = [azurerm_key_vault_access_policy.terraform]
}

resource "azurerm_key_vault_secret" "storage_account_key" {
  name         = "storage-account-key"
  value        = azurerm_storage_account.main.primary_access_key
  key_vault_id = azurerm_key_vault.main.id
  
  depends_on = [azurerm_key_vault_access_policy.terraform]
}

# Data sources
data "azurerm_client_config" "current" {}

# Outputs
output "resource_group_name" {
  value = azurerm_resource_group.main.name
}

output "storage_account_name" {
  value = azurerm_storage_account.main.name
}

output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.main.workspace_url
}

output "synapse_workspace_name" {
  value = azurerm_synapse_workspace.main.name
}

output "event_hub_namespace" {
  value = azurerm_eventhub_namespace.main.name
}

output "key_vault_name" {
  value = azurerm_key_vault.main.name
}

output "application_insights_connection_string" {
  value = azurerm_application_insights.main.connection_string
  sensitive = true
} 
