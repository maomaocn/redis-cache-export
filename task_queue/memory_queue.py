"""
内存任务队列（轻量级实现）
适用于单进程场景，无需额外依赖
"""

import threading
import queue
import time
import uuid
from typing import Any, Callable, Optional, Dict
from concurrent.futures import ThreadPoolExecutor, Future

from .base import BaseTaskQueue, TaskResult, TaskState


class MemoryTaskQueue(BaseTaskQueue):
    """
    内存任务队列
    
    特点：
    1. 使用线程池执行任务
    2. 无需额外依赖（不需要 Celery）
    3. 适用于单进程、轻量级场景
    4. 任务结果存储在内存中
    """
    
    def __init__(
        self,
        manager=None,
        max_workers: int = 4,
        result_ttl: int = 3600
    ):
        """
        初始化内存任务队列
        
        Args:
            manager: CacheExportManager 实例
            max_workers: 最大工作线程数
            result_ttl: 结果保存时间（秒）
        """
        super().__init__(manager)
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.result_ttl = result_ttl
        
        # 存储任务状态和结果
        self._results: Dict[str, TaskResult] = {}
        self._futures: Dict[str, Future] = {}
        
        # 清理线程
        self._cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self._cleanup_thread.start()
    
    def _generate_task_id(self) -> str:
        """生成唯一的任务ID"""
        return str(uuid.uuid4())
    
    def _execute_task(self, task_id: str, task_func: Callable, args: tuple, kwargs: dict) -> Any:
        """
        执行任务
        
        Args:
            task_id: 任务ID
            task_func: 任务函数
            args: 位置参数
            kwargs: 关键字参数
        
        Returns:
            执行结果
        """
        try:
            # 更新状态为执行中
            self._results[task_id] = TaskResult(
                task_id=task_id,
                state=TaskState.STARTED,
                meta={'started_at': time.time()}
            )
            
            # 执行任务
            result = task_func(*args, **kwargs)
            
            # 存储成功结果
            self._results[task_id] = TaskResult(
                task_id=task_id,
                state=TaskState.SUCCESS,
                result=result,
                meta={'completed_at': time.time()}
            )
            
            return result
            
        except Exception as e:
            # 存储失败结果
            import traceback
            self._results[task_id] = TaskResult(
                task_id=task_id,
                state=TaskState.FAILURE,
                error=str(e),
                traceback=traceback.format_exc(),
                meta={'failed_at': time.time()}
            )
            raise
    
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
        
        task_id = self._generate_task_id()
        
        # 初始化任务状态
        self._results[task_id] = TaskResult(
            task_id=task_id,
            state=TaskState.PENDING,
            meta={'submitted_at': time.time()}
        )
        
        # 提交到线程池
        future = self.executor.submit(
            self._execute_task,
            task_id,
            task_func,
            args,
            kwargs
        )
        self._futures[task_id] = future
        
        return task_id
    
    def get_result(self, task_id: str, timeout: Optional[float] = None) -> TaskResult:
        """
        获取任务结果
        
        Args:
            task_id: 任务ID
            timeout: 超时时间（秒）
        
        Returns:
            任务结果
        """
        future = self._futures.get(task_id)
        
        if future:
            try:
                future.result(timeout=timeout)
            except Exception:
                pass
        
        return self._results.get(task_id, TaskResult(
            task_id=task_id,
            state=TaskState.PENDING
        ))
    
    def revoke(self, task_id: str, terminate: bool = False) -> bool:
        """
        撤销任务
        
        Args:
            task_id: 任务ID
            terminate: 是否终止（内存队列不支持强制终止）
        
        Returns:
            是否成功
        """
        future = self._futures.get(task_id)
        
        if future:
            cancelled = future.cancel()
            
            if cancelled:
                self._results[task_id] = TaskResult(
                    task_id=task_id,
                    state=TaskState.REVOKED,
                    meta={'revoked_at': time.time()}
                )
            
            return cancelled
        
        return False
    
    def get_status(self, task_id: str) -> TaskState:
        """
        获取任务状态
        
        Args:
            task_id: 任务ID
        
        Returns:
            任务状态
        """
        result = self._results.get(task_id)
        return result.state if result else TaskState.PENDING
    
    def _cleanup_loop(self):
        """定期清理过期结果"""
        while True:
            time.sleep(60)  # 每分钟清理一次
            
            current_time = time.time()
            expired_tasks = []
            
            for task_id, result in self._results.items():
                meta = result.meta or {}
                completed_at = meta.get('completed_at') or meta.get('failed_at') or meta.get('revoked_at')
                
                if completed_at and (current_time - completed_at) > self.result_ttl:
                    expired_tasks.append(task_id)
            
            # 清理过期数据
            for task_id in expired_tasks:
                self._results.pop(task_id, None)
                self._futures.pop(task_id, None)
    
    def shutdown(self, wait: bool = True):
        """
        关闭任务队列
        
        Args:
            wait: 是否等待任务完成
        """
        self.executor.shutdown(wait=wait)
