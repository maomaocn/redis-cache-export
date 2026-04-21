"""
任务队列基类
"""

from abc import ABC, abstractmethod
from typing import Any, Callable, Optional, Dict
from dataclasses import dataclass
from enum import Enum


class TaskState(str, Enum):
    """任务状态"""
    PENDING = "pending"
    STARTED = "started"
    SUCCESS = "success"
    FAILURE = "failure"
    RETRY = "retry"
    REVOKED = "revoked"


@dataclass
class TaskResult:
    """任务执行结果"""
    task_id: str
    state: TaskState
    result: Any = None
    error: Optional[str] = None
    traceback: Optional[str] = None
    meta: Optional[Dict[str, Any]] = None


class BaseTaskQueue(ABC):
    """
    任务队列基类
    
    所有任务队列实现需要继承此类
    """
    
    def __init__(self, manager=None):
        """
        初始化任务队列
        
        Args:
            manager: CacheExportManager 实例
        """
        self.manager = manager
        self._tasks: Dict[str, Callable] = {}
    
    def register_task(self, name: str, func: Callable) -> None:
        """
        注册任务函数
        
        Args:
            name: 任务名称
            func: 任务函数
        """
        self._tasks[name] = func
    
    def get_task(self, name: str) -> Optional[Callable]:
        """
        获取已注册的任务函数
        
        Args:
            name: 任务名称
        
        Returns:
            任务函数，不存在则返回 None
        """
        return self._tasks.get(name)
    
    @abstractmethod
    def submit(self, task_name: str, *args, **kwargs) -> str:
        """
        提交任务到队列
        
        Args:
            task_name: 任务名称
            *args: 位置参数
            **kwargs: 关键字参数
        
        Returns:
            任务ID
        """
        pass
    
    @abstractmethod
    def get_result(self, task_id: str, timeout: Optional[float] = None) -> TaskResult:
        """
        获取任务结果
        
        Args:
            task_id: 任务ID
            timeout: 超时时间（秒）
        
        Returns:
            任务结果
        """
        pass
    
    @abstractmethod
    def revoke(self, task_id: str, terminate: bool = False) -> bool:
        """
        撤销/取消任务
        
        Args:
            task_id: 任务ID
            terminate: 是否终止正在执行的任务
        
        Returns:
            是否成功
        """
        pass
    
    @abstractmethod
    def get_status(self, task_id: str) -> TaskState:
        """
        获取任务状态
        
        Args:
            task_id: 任务ID
        
        Returns:
            任务状态
        """
        pass
