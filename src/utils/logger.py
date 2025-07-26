"""
Logging utility for Olympic Analytics Platform.
Provides structured logging with multiple output formats and destinations.
"""

import logging
import logging.handlers
import sys
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
import structlog
from structlog.stdlib import LoggerFactory


class StructuredFormatter(logging.Formatter):
    """Custom formatter for structured logging."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as structured JSON."""
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # Add exception info if present
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)
        
        # Add extra fields
        if hasattr(record, 'extra_fields'):
            log_entry.update(record.extra_fields)
        
        return json.dumps(log_entry)


class ContextFilter(logging.Filter):
    """Filter to add context information to log records."""
    
    def __init__(self, context: Optional[Dict[str, Any]] = None):
        super().__init__()
        self.context = context or {}
    
    def filter(self, record: logging.LogRecord) -> bool:
        """Add context to log record."""
        record.extra_fields = self.context.copy()
        return True


class LoggerManager:
    """Manages logging configuration and provides logger instances."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize logger manager.
        
        Args:
            config: Logging configuration dictionary
        """
        self.config = config
        self.loggers = {}
        self._setup_logging()
    
    def _setup_logging(self) -> None:
        """Set up logging configuration."""
        # Configure structlog
        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.UnicodeDecoder(),
                structlog.processors.JSONRenderer()
            ],
            context_class=dict,
            logger_factory=LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )
        
        # Create logs directory
        log_path = Path(self.config.get('handlers', {}).get('file', {}).get('path', 'logs'))
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, self.config.get('level', 'INFO')))
        
        # Clear existing handlers
        root_logger.handlers.clear()
        
        # Add handlers based on configuration
        self._add_console_handler(root_logger)
        self._add_file_handler(root_logger)
        self._add_azure_monitor_handler(root_logger)
    
    def _add_console_handler(self, logger: logging.Logger) -> None:
        """Add console handler."""
        console_config = self.config.get('handlers', {}).get('console', {})
        if console_config.get('enabled', True):
            handler = logging.StreamHandler(sys.stdout)
            handler.setLevel(getattr(logging, console_config.get('level', 'INFO')))
            
            if self.config.get('format') == 'json':
                handler.setFormatter(StructuredFormatter())
            else:
                formatter = logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                )
                handler.setFormatter(formatter)
            
            logger.addHandler(handler)
    
    def _add_file_handler(self, logger: logging.Logger) -> None:
        """Add file handler with rotation."""
        file_config = self.config.get('handlers', {}).get('file', {})
        if file_config.get('enabled', False):
            log_path = Path(file_config.get('path', 'logs/olympic-analytics.log'))
            max_size = file_config.get('max_size', '100MB')
            backup_count = file_config.get('backup_count', 5)
            
            # Convert max_size to bytes
            if isinstance(max_size, str):
                if max_size.endswith('MB'):
                    max_size = int(max_size[:-2]) * 1024 * 1024
                elif max_size.endswith('GB'):
                    max_size = int(max_size[:-2]) * 1024 * 1024 * 1024
                else:
                    max_size = int(max_size)
            
            handler = logging.handlers.RotatingFileHandler(
                log_path,
                maxBytes=max_size,
                backupCount=backup_count
            )
            handler.setLevel(getattr(logging, file_config.get('level', 'DEBUG')))
            
            if self.config.get('format') == 'json':
                handler.setFormatter(StructuredFormatter())
            else:
                formatter = logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                )
                handler.setFormatter(formatter)
            
            logger.addHandler(handler)
    
    def _add_azure_monitor_handler(self, logger: logging.Logger) -> None:
        """Add Azure Monitor handler."""
        azure_config = self.config.get('handlers', {}).get('azure_monitor', {})
        if azure_config.get('enabled', False):
            try:
                from opencensus.ext.azure.log_exporter import AzureLogHandler
                connection_string = azure_config.get('connection_string')
                if connection_string:
                    handler = AzureLogHandler(connection_string=connection_string)
                    handler.setLevel(logging.INFO)
                    logger.addHandler(handler)
            except ImportError:
                print("Warning: opencensus-ext-azure not installed. Azure Monitor logging disabled.")
    
    def get_logger(self, name: str, context: Optional[Dict[str, Any]] = None) -> structlog.stdlib.BoundLogger:
        """
        Get a logger instance with optional context.
        
        Args:
            name: Logger name
            context: Optional context dictionary
            
        Returns:
            Structured logger instance
        """
        if name not in self.loggers:
            logger = structlog.get_logger(name)
            
            # Add context filter if provided
            if context:
                filter_obj = ContextFilter(context)
                logger.addFilter(filter_obj)
            
            self.loggers[name] = logger
        
        return self.loggers[name]


class PipelineLogger:
    """Specialized logger for data pipeline operations."""
    
    def __init__(self, pipeline_name: str, config: Dict[str, Any]):
        """
        Initialize pipeline logger.
        
        Args:
            pipeline_name: Name of the pipeline
            config: Logging configuration
        """
        self.pipeline_name = pipeline_name
        self.logger_manager = LoggerManager(config)
        self.logger = self.logger_manager.get_logger(
            f"pipeline.{pipeline_name}",
            context={'pipeline': pipeline_name}
        )
    
    def start_pipeline(self, **kwargs) -> None:
        """Log pipeline start."""
        self.logger.info(
            "Pipeline started",
            pipeline_name=self.pipeline_name,
            **kwargs
        )
    
    def end_pipeline(self, duration: float, **kwargs) -> None:
        """Log pipeline completion."""
        self.logger.info(
            "Pipeline completed",
            pipeline_name=self.pipeline_name,
            duration_seconds=duration,
            **kwargs
        )
    
    def pipeline_error(self, error: Exception, **kwargs) -> None:
        """Log pipeline error."""
        self.logger.error(
            "Pipeline failed",
            pipeline_name=self.pipeline_name,
            error=str(error),
            error_type=type(error).__name__,
            **kwargs
        )
    
    def data_processed(self, record_count: int, **kwargs) -> None:
        """Log data processing metrics."""
        self.logger.info(
            "Data processed",
            pipeline_name=self.pipeline_name,
            record_count=record_count,
            **kwargs
        )
    
    def data_quality_issue(self, issue_type: str, details: str, **kwargs) -> None:
        """Log data quality issues."""
        self.logger.warning(
            "Data quality issue detected",
            pipeline_name=self.pipeline_name,
            issue_type=issue_type,
            details=details,
            **kwargs
        )


class DataQualityLogger:
    """Specialized logger for data quality operations."""
    
    def __init__(self, dataset_name: str, config: Dict[str, Any]):
        """
        Initialize data quality logger.
        
        Args:
            dataset_name: Name of the dataset
            config: Logging configuration
        """
        self.dataset_name = dataset_name
        self.logger_manager = LoggerManager(config)
        self.logger = self.logger_manager.get_logger(
            f"data_quality.{dataset_name}",
            context={'dataset': dataset_name}
        )
    
    def validation_start(self, **kwargs) -> None:
        """Log validation start."""
        self.logger.info(
            "Data validation started",
            dataset_name=self.dataset_name,
            **kwargs
        )
    
    def validation_complete(self, results: Dict[str, Any], **kwargs) -> None:
        """Log validation completion."""
        self.logger.info(
            "Data validation completed",
            dataset_name=self.dataset_name,
            validation_results=results,
            **kwargs
        )
    
    def validation_failed(self, rule_name: str, details: str, **kwargs) -> None:
        """Log validation failure."""
        self.logger.error(
            "Data validation failed",
            dataset_name=self.dataset_name,
            rule_name=rule_name,
            details=details,
            **kwargs
        )
    
    def quality_metrics(self, metrics: Dict[str, Any], **kwargs) -> None:
        """Log quality metrics."""
        self.logger.info(
            "Quality metrics calculated",
            dataset_name=self.dataset_name,
            metrics=metrics,
            **kwargs
        )


def setup_logging(config: Dict[str, Any]) -> LoggerManager:
    """
    Set up logging for the application.
    
    Args:
        config: Logging configuration dictionary
        
    Returns:
        LoggerManager instance
    """
    return LoggerManager(config)


def get_logger(name: str, config: Optional[Dict[str, Any]] = None) -> structlog.stdlib.BoundLogger:
    """
    Get a logger instance.
    
    Args:
        name: Logger name
        config: Optional logging configuration
        
    Returns:
        Structured logger instance
    """
    if config is None:
        # Use default configuration
        config = {
            'level': 'INFO',
            'format': 'json',
            'handlers': {
                'console': {'enabled': True, 'level': 'INFO'},
                'file': {'enabled': False}
            }
        }
    
    logger_manager = LoggerManager(config)
    return logger_manager.get_logger(name)


def log_function_call(func):
    """Decorator to log function calls."""
    def wrapper(*args, **kwargs):
        logger = get_logger(func.__module__)
        logger.info(
            "Function called",
            function=func.__name__,
            args=args,
            kwargs=kwargs
        )
        try:
            result = func(*args, **kwargs)
            logger.info(
                "Function completed",
                function=func.__name__,
                success=True
            )
            return result
        except Exception as e:
            logger.error(
                "Function failed",
                function=func.__name__,
                error=str(e),
                success=False
            )
            raise
    return wrapper 