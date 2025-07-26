"""
Configuration management utility for Olympic Analytics Platform.
Handles loading and validation of configuration from YAML files and environment variables.
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass
import logging
from cerberus import Validator

logger = logging.getLogger(__name__)


@dataclass
class AzureConfig:
    """Azure configuration settings."""
    subscription_id: str
    tenant_id: str
    resource_group: str
    location: str
    key_vault_name: str
    key_vault_url: str


@dataclass
class StorageConfig:
    """Storage configuration settings."""
    account_name: str
    account_key: str
    containers: Dict[str, str]
    paths: Dict[str, str]


@dataclass
class EventHubsConfig:
    """Event Hubs configuration settings."""
    namespace: str
    connection_string: str
    hubs: Dict[str, Dict[str, Any]]


@dataclass
class DatabricksConfig:
    """Databricks configuration settings."""
    workspace_url: str
    access_token: str
    cluster: Dict[str, Any]
    notebooks: Dict[str, str]


@dataclass
class DataQualityConfig:
    """Data quality configuration settings."""
    rules: Dict[str, Any]
    thresholds: Dict[str, float]
    alerts: Dict[str, Any]


class ConfigManager:
    """Manages configuration loading and validation."""
    
    def __init__(self, config_path: Optional[str] = None, environment: Optional[str] = None):
        """
        Initialize configuration manager.
        
        Args:
            config_path: Path to configuration file
            environment: Environment name (dev, staging, prod)
        """
        self.config_path = config_path or self._get_default_config_path(environment)
        self.environment = environment or self._get_environment()
        self.config = {}
        self._load_config()
        self._validate_config()
    
    def _get_default_config_path(self, environment: Optional[str] = None) -> str:
        """Get default configuration file path."""
        env = environment or self._get_environment()
        return f"config/environments/{env}.yaml"
    
    def _get_environment(self) -> str:
        """Get environment from environment variable or default to dev."""
        return os.getenv("ENVIRONMENT", "dev")
    
    def _load_config(self) -> None:
        """Load configuration from YAML file and environment variables."""
        try:
            # Load YAML configuration
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r') as file:
                    self.config = yaml.safe_load(file)
            else:
                logger.warning(f"Configuration file not found: {self.config_path}")
                self.config = {}
            
            # Override with environment variables
            self._override_from_env()
            
            # Resolve template variables
            self._resolve_template_variables()
            
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise
    
    def _override_from_env(self) -> None:
        """Override configuration values with environment variables."""
        env_mappings = {
            "AZURE_SUBSCRIPTION_ID": ["azure", "subscription_id"],
            "AZURE_TENANT_ID": ["azure", "tenant_id"],
            "STORAGE_ACCOUNT_KEY": ["storage", "account_key"],
            "EVENT_HUB_CONNECTION_STRING": ["event_hubs", "connection_string"],
            "DATABRICKS_ACCESS_TOKEN": ["databricks", "access_token"],
            "SYNAPSE_USERNAME": ["synapse", "username"],
            "SYNAPSE_PASSWORD": ["synapse", "password"],
            "SLACK_WEBHOOK_URL": ["data_quality", "alerts", "slack_webhook"],
            "APPLICATION_INSIGHTS_CONNECTION_STRING": ["logging", "handlers", "azure_monitor", "connection_string"],
            "REDIS_URL": ["app", "cache", "redis_url"],
            "OLYMPIC_API_KEY": ["external_apis", "olympic_api", "api_key"],
            "SPORTS_API_KEY": ["external_apis", "sports_api", "api_key"],
        }
        
        for env_var, config_path in env_mappings.items():
            env_value = os.getenv(env_var)
            if env_value:
                self._set_nested_value(self.config, config_path, env_value)
    
    def _set_nested_value(self, config: Dict[str, Any], path: list, value: Any) -> None:
        """Set a nested value in configuration dictionary."""
        current = config
        for key in path[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        current[path[-1]] = value
    
    def _resolve_template_variables(self) -> None:
        """Resolve template variables in configuration."""
        config_str = yaml.dump(self.config)
        
        # Replace environment variables
        config_str = config_str.replace("${ENVIRONMENT}", self.environment)
        
        # Replace account name placeholders
        if "storage" in self.config:
            account_name = self.config["storage"]["account_name"]
            config_str = config_str.replace("@{account_name}", account_name)
        
        # Reload configuration
        self.config = yaml.safe_load(config_str)
    
    def _validate_config(self) -> None:
        """Validate configuration using schema."""
        schema = {
            'environment': {'type': 'string', 'allowed': ['dev', 'staging', 'prod']},
            'version': {'type': 'string'},
            'azure': {
                'type': 'dict',
                'schema': {
                    'subscription_id': {'type': 'string', 'required': True},
                    'tenant_id': {'type': 'string', 'required': True},
                    'resource_group': {'type': 'string', 'required': True},
                    'location': {'type': 'string', 'required': True},
                    'key_vault': {
                        'type': 'dict',
                        'schema': {
                            'name': {'type': 'string', 'required': True},
                            'url': {'type': 'string', 'required': True}
                        }
                    }
                }
            },
            'storage': {
                'type': 'dict',
                'schema': {
                    'account_name': {'type': 'string', 'required': True},
                    'account_key': {'type': 'string', 'required': True},
                    'containers': {'type': 'dict', 'required': True},
                    'paths': {'type': 'dict', 'required': True}
                }
            },
            'event_hubs': {
                'type': 'dict',
                'schema': {
                    'namespace': {'type': 'string', 'required': True},
                    'connection_string': {'type': 'string', 'required': True},
                    'hubs': {'type': 'dict', 'required': True}
                }
            },
            'databricks': {
                'type': 'dict',
                'schema': {
                    'workspace_url': {'type': 'string', 'required': True},
                    'access_token': {'type': 'string', 'required': True},
                    'cluster': {'type': 'dict', 'required': True},
                    'notebooks': {'type': 'dict', 'required': True}
                }
            }
        }
        
        validator = Validator(schema)
        if not validator.validate(self.config):
            logger.error(f"Configuration validation failed: {validator.errors}")
            raise ValueError(f"Invalid configuration: {validator.errors}")
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key (supports dot notation)."""
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def get_azure_config(self) -> AzureConfig:
        """Get Azure configuration."""
        azure_config = self.config.get('azure', {})
        return AzureConfig(
            subscription_id=azure_config.get('subscription_id'),
            tenant_id=azure_config.get('tenant_id'),
            resource_group=azure_config.get('resource_group'),
            location=azure_config.get('location'),
            key_vault_name=azure_config.get('key_vault', {}).get('name'),
            key_vault_url=azure_config.get('key_vault', {}).get('url')
        )
    
    def get_storage_config(self) -> StorageConfig:
        """Get storage configuration."""
        storage_config = self.config.get('storage', {})
        return StorageConfig(
            account_name=storage_config.get('account_name'),
            account_key=storage_config.get('account_key'),
            containers=storage_config.get('containers', {}),
            paths=storage_config.get('paths', {})
        )
    
    def get_event_hubs_config(self) -> EventHubsConfig:
        """Get Event Hubs configuration."""
        event_hubs_config = self.config.get('event_hubs', {})
        return EventHubsConfig(
            namespace=event_hubs_config.get('namespace'),
            connection_string=event_hubs_config.get('connection_string'),
            hubs=event_hubs_config.get('hubs', {})
        )
    
    def get_databricks_config(self) -> DatabricksConfig:
        """Get Databricks configuration."""
        databricks_config = self.config.get('databricks', {})
        return DatabricksConfig(
            workspace_url=databricks_config.get('workspace_url'),
            access_token=databricks_config.get('access_token'),
            cluster=databricks_config.get('cluster', {}),
            notebooks=databricks_config.get('notebooks', {})
        )
    
    def get_data_quality_config(self) -> DataQualityConfig:
        """Get data quality configuration."""
        dq_config = self.config.get('data_quality', {})
        return DataQualityConfig(
            rules=dq_config.get('rules', {}),
            thresholds=dq_config.get('thresholds', {}),
            alerts=dq_config.get('alerts', {})
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Return configuration as dictionary."""
        return self.config.copy()


# Global configuration instance
_config_instance: Optional[ConfigManager] = None


def get_config(config_path: Optional[str] = None, environment: Optional[str] = None) -> ConfigManager:
    """
    Get global configuration instance.
    
    Args:
        config_path: Path to configuration file
        environment: Environment name
        
    Returns:
        ConfigManager instance
    """
    global _config_instance
    
    if _config_instance is None:
        _config_instance = ConfigManager(config_path, environment)
    
    return _config_instance


def reload_config(config_path: Optional[str] = None, environment: Optional[str] = None) -> ConfigManager:
    """
    Reload configuration from file.
    
    Args:
        config_path: Path to configuration file
        environment: Environment name
        
    Returns:
        ConfigManager instance
    """
    global _config_instance
    _config_instance = ConfigManager(config_path, environment)
    return _config_instance 
