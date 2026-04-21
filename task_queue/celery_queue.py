"""
Celery 任务队列适配器
适用于生产环境、分布式场景
"""

import time
from typing import Any, Callable, Optional

from .base import BaseTaskQueue, TaskResult, TaskState

# 尝试导入 Celery
try:
    from celery import Celery, shared_task
    from celery.result import AsyncResult
    CELERY_AVAILABLE = True
except ImportError:
    CELERY_AVAILABLE = False
    Celery = None
    shared_task = None
    AsyncResult = None


class CeleryTaskQueue(BaseTaskQueue):
    """
    Celery 任务队列
    
    特点：
    1. 支持分布式部署
    2. 支持任务重试
    3. 支持任务优先级
    4. 适用于生产环境
    """
    
    def __init__(
        self,
        manager=None,
        celery_app: Optional['Celery'] = None,
        broker_url: Optional[str] = None,
        result_backend: Optional[str] = None
    ):
        """
        初始化 Celery 任务队列
        
        Args:
            manager: CacheExportManager 实例
            celery_app: 现有的 Celery 应用实例
            broker_url: 消息代理 URL（如 redis://localhost:6379/1）
            result_backend: 结果后端 URL
        """
        if not CELERY_AVAILABLE:
            raise ImportError(
                "Celery library is required. "
                "Install it with: pip install celery"
            )
        
        super().__init__(manager)
        
        # 使用现有 Celery 应用或创建新的
        if celery_app:
            self.celery_app = celery_app
        else:
            self.celery_app = Celery(
                'export_tasks',
                broker=broker_url or 'redis://localhost:6379/1',
                backend=result_backend or 'redis://localhost:6379/2'
            )
            
            # 默认配置
            self.celery_app.conf.update(
                task_serializer='json',
                accept_content=['json'],
                result_serializer='json',
                timezone='Asia/Shanghai',
                enable_utc=True,
                task_track_started=True,
                task_time_limit=3600,  # 1小时超时
                task_soft_time_limit=3300,  # 55分钟软超时
            )
    
    def register_task(self, name: str, func: Callable) -> None:
        """
        注册任务函数
        
        Args:
            name: 任务名称
            func: 任务函数
        """
        super().register_task(name, func)
        
        # 将函数注册为 Celery 任务
        self.celery_app.task(func, name=name)
    
    def submit(self, task_name: str, *args, **kwargs) -> str:
        """
        提交任务
        
        Args:
            task_name: 任务名称
            *args: 位置参数
            **kwargs: 关键字参数
        
        Returns:
            任务ID
        """
        task_func = self.get_task(task_name)
        if task_func is None:
            raise ValueError(f"Task not found: {task_name}")
        
        # 获取 Celery 任务
        celery_task = self.celery_app.tasks.get(task_name)
        if celery_task is None:
            raise ValueError(f"Celery task not registered: {task_name}")
        
        # 异步执行
        result = celery_task.apply_async(args=args, kwargs=kwargs)
        
        return result.id
    
    def get_result(self, task_id: str, timeout: Optional[float] = None) -> TaskResult:
        """
        获取任务结果
        
        Args:
            task_id: 任务ID
            timeout: 超时时间（秒）
        
        Returns:
            任务结果
        """
        async_result = AsyncResult(task_id, app=self.celery_app)
        
        # 映射 Celery 状态到内部状态
        state_mapping = {
            'PENDING': TaskState.PENDING,
            'STARTED': TaskState.STARTED,
            'SUCCESS': TaskState.SUCCESS,
            'FAILURE': TaskState.FAILURE,
            'RETRY': TaskState.RETRY,
            'REVOKED': TaskState.REVOKED,
        }
        
        state = state_mapping.get(async_result.state, TaskState.PENDING)
        
        # 获取结果或错误
        result = None
        error = None
        traceback_str = None
        
        if state == TaskState.SUCCESS:
            try:
                result = async_result.result
            except Exception:
                pass
        elif state == TaskState.FAILURE:
            try:
                error = str(async_result.result)
                traceback_str = async_result.traceback
            except Exception:
                pass
        
        return TaskResult(
            task_id=task_id,
            state=state,
            result=result,
            error=error,
            traceback=traceback_str
        )
    
    def revoke(self, task_id: str, terminate: bool = False) -> bool:
        """
        撤销任务
        
        Args:
            task_id: 任务ID
            terminate: 是否终止正在执行的任务
        
        Returns:
            是否成功
        """
        try:
            self.celery_app.control.revoke(task_id, terminate=terminate)
            return True
        except Exception:
            return False
    
    def get_status(self, task_id: str) -> TaskState:
        """
        获取任务状态
        
        Args:
            task_id: 任务ID
        
        Returns:
            任务状态
        """
        async_result = AsyncResult(task_id, app=self.celery_app)
        
        state_mapping = {
            'PENDING': TaskState.PENDING,
            'STARTED': TaskState.STARTED,
            'SUCCESS': TaskState.SUCCESS,
            'FAILURE': TaskState.FAILURE,
            'RETRY': TaskState.RETRY,
            'REVOKED': TaskState.REVOKED,
        }
        
        return state_mapping.get(async_result.state, TaskState.PENDING)
    
    def get_active_tasks(self) -> list:
        """
        获取活跃任务列表
        
        Returns:
            活跃任务列表
        """
        inspect = self.celery_app.control.inspect()
        active = inspect.active()
        
        if active:
            # 合并所有 worker 的活跃任务
            tasks = []
            for worker, worker_tasks in active.items():
                tasks.extend(worker_tasks)
            return tasks
        
        return []


def create_celery_export_task(celery_app: 'Celery', manager_factory: Callable):
    """
    创建导出任务的便捷函数
    
    Args:
        celery_app: Celery 应用实例
        manager_factory: 创建 CacheExportManager 的工厂函数
    
    Returns:
        任务函数
    """
    @celery_app.task(bind=True, name='export.execute_export')
    def execute_export_task(self, task_id: str, query_func_name: str, **kwargs):
        """执行导出任务的 Celery 任务"""
        manager = manager_factory()
        
        # 获取查询函数（需要在 manager 中注册）
        query_func = manager.task_runners.get(query_func_name)
        
        if query_func is None:
            raise ValueError(f"Query function not found: {query_func_name}")
        
        # 执行导出
        return manager.execute_task_sync(
            task_id=task_id,
            query_func=query_func,
            **kwargs
        )
    
    return execute_export_task
