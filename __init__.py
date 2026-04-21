"""
Redis Cache Export - A query cache and export plugin

Features:
- Cache query conditions (not data) for efficient memory usage
- Async export with progress tracking
- Support CSV, Excel, JSON formats
- Compatible with Flask, FastAPI, Django
- Stream processing for large datasets
"""

__version__ = "1.0.0"
__author__ = "Your Name"
__email__ = "your.email@example.com"

from .core import CacheExportManager, QueryCache, ProgressTracker, ExportExecutor
from .decorators import cache_query, setup_manager
from .formatters import CSVFormatter, ExcelFormatter, JSONFormatter
from .adapters import FlaskAdapter, FastAPIAdapter, DjangoAdapter, BaseAdapter
from .exceptions import (
    CacheExportError,
    QueryNotFoundError,
    QueryCacheExpiredError,
    TaskNotFoundError,
    TaskCancelledError,
    TaskFailedError,
    UnsupportedFormatError,
)
from .logger import get_logger, configure_logging, LogManager
from .config import Config, default_config

__all__ = [
    # Core
    'CacheExportManager',
    'QueryCache',
    'ProgressTracker',
    'ExportExecutor',
    
    # Decorators
    'cache_query',
    'setup_manager',
    
    # Formatters
    'CSVFormatter',
    'ExcelFormatter',
    'JSONFormatter',
    
    # Adapters
    'BaseAdapter',
    'FlaskAdapter',
    'FastAPIAdapter',
    'DjangoAdapter',
    
    # Exceptions
    'CacheExportError',
    'QueryNotFoundError',
    'QueryCacheExpiredError',
    'TaskNotFoundError',
    'TaskCancelledError',
    'TaskFailedError',
    'UnsupportedFormatError',
    
    # Logger
    'get_logger',
    'configure_logging',
    'LogManager',
    
    # Config
    'Config',
    'default_config',
]
