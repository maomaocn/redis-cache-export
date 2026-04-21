"""
任务队列模块
"""

from .base import BaseTaskQueue, TaskResult
from .memory_queue import MemoryTaskQueue
from .celery_queue import CeleryTaskQueue

__all__ = [
    'BaseTaskQueue',
    'TaskResult',
    'MemoryTaskQueue',
    'CeleryTaskQueue',
]
