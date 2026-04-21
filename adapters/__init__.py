"""
框架适配器
"""

from .base import BaseAdapter, RequestInfo, ResponseInfo
from .flask_adapter import FlaskAdapter
from .fastapi_adapter import FastAPIAdapter, FastAPIExportHandlers
from .django_adapter import DjangoAdapter

__all__ = [
    'BaseAdapter',
    'RequestInfo',
    'ResponseInfo',
    'FlaskAdapter',
    'FastAPIAdapter',
    'FastAPIExportHandlers',
    'DjangoAdapter',
]
