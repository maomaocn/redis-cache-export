"""
核心管理器模块
整合：查询缓存、进度追踪、导出执行
"""

import time
import uuid
from typing import Any, Dict, Optional, Callable
import redis

from .query_cache import QueryCache
from .progress_tracker import ProgressTracker, TaskStatus
from .export_executor import ExportExecutor


class CacheExportManager:
    """
    缓存导出管理器
    
    核心入口类，整合所有功能：
    1. 查询条件缓存
    2. 导出任务提交
    3. 进度查询
    4. 文件下载
    """
    
    def __init__(
        self,
        redis_client: Optional[redis.Redis] = None,
        redis_url: str = 'redis://localhost:6379/0',
        storage_path: str = '/tmp/exports',
        batch_size: int = 1000,
        query_ttl: int = 3600,
        progress_ttl: int = 86400
    ):
        """
        初始化管理器
        
        Args:
            redis_client: Redis 客户端实例（优先使用）
            redis_url: Redis 连接 URL（如果没有提供 redis_client）
            storage_path: 文件存储路径
            batch_size: 分批查询大小
            query_ttl: 查询条件缓存时间（秒）
            progress_ttl: 进度数据保存时间（秒）
        """
        # 创建 Redis 客户端
        if redis_client:
            self.redis = redis_client
        else:
            self.redis = redis.from_url(redis_url)
        
        # 初始化子模块
        self.query_cache = QueryCache(self.redis)
        self.progress_tracker = ProgressTracker(self.redis)
        self.export_executor = ExportExecutor(
            self.redis,
            self.query_cache,
            self.progress_tracker,
            storage_path,
            batch_size
        )
        
        # 配置
        self.query_ttl = query_ttl
        self.progress_ttl = progress_ttl
        
        # 任务执行器（外部注入）
        self.task_runners: Dict[str, Callable] = {}
    
    # ==================== 查询缓存相关 ====================
    
    def cache_query(
        self,
        func_name: str,
        params: Dict[str, Any],
        user_id: Optional[str] = None,
        exclude_params: Optional[set] = None,
        extra_data: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        缓存查询条件
        
        Args:
            func_name: 查询函数名称
            params: 查询参数
            user_id: 用户 ID
            exclude_params: 需排除的参数
            extra_data: 额外数据（如 total）
        
        Returns:
            query_id
        """
        return self.query_cache.cache_query(
            func_name=func_name,
            params=params,
            user_id=user_id,
            exclude_params=exclude_params,
            ttl=self.query_ttl,
            extra_data=extra_data
        )
    
    def get_query(self, query_id: str) -> Optional[Dict[str, Any]]:
        """
        获取缓存的查询条件
        
        Args:
            query_id: 查询 ID
        
        Returns:
            查询数据
        """
        return self.query_cache.get_query(query_id)
    
    def extend_query_ttl(self, query_id: str, additional_seconds: int = 1800) -> bool:
        """
        延长查询缓存时间
        
        Args:
            query_id: 查询 ID
            additional_seconds: 额外秒数
        
        Returns:
            是否成功
        """
        return self.query_cache.extend_ttl(query_id, additional_seconds)
    
    # ==================== 导出任务相关 ====================
    
    def submit_export_task(
        self,
        query_id: str,
        fields: list,
        export_format: str = 'csv',
        user_id: Optional[str] = None,
        total_count: Optional[int] = None
    ) -> str:
        """
        提交导出任务
        
        Args:
            query_id: 查询 ID
            fields: 导出字段
            export_format: 导出格式（csv/excel/json）
            user_id: 用户 ID
            total_count: 总数量
        
        Returns:
            task_id
        """
        return self.export_executor.submit_task(
            query_id=query_id,
            fields=fields,
            export_format=export_format,
            user_id=user_id,
            total_count=total_count
        )
    
    def get_task_progress(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        获取任务进度
        
        Args:
            task_id: 任务 ID
        
        Returns:
            进度数据
        """
        return self.progress_tracker.get_progress(task_id)
    
    def cancel_task(self, task_id: str) -> bool:
        """
        取消任务
        
        Args:
            task_id: 任务 ID
        
        Returns:
            是否成功
        """
        return self.progress_tracker.cancel_task(task_id)
    
    def get_user_tasks(
        self,
        user_id: str,
        status: Optional[str] = None,
        limit: int = 10
    ) -> list:
        """
        获取用户的任务列表
        
        Args:
            user_id: 用户 ID
            status: 过滤状态
            limit: 返回数量
        
        Returns:
            任务列表
        """
        return self.progress_tracker.get_user_tasks(user_id, status, limit)
    
    def get_export_file(self, task_id: str) -> Optional[tuple]:
        """
        获取导出文件
        
        Args:
            task_id: 任务 ID
        
        Returns:
            (文件路径, 文件名, MIME类型) 或 None
        """
        return self.export_executor.get_file(task_id)
    
    # ==================== 执行器相关 ====================
    
    def register_formatter(self, format_name: str, formatter_class: type):
        """
        注册格式化器
        
        Args:
            format_name: 格式名称
            formatter_class: 格式化器类
        """
        self.export_executor.register_formatter(format_name, formatter_class)
    
    def register_task_runner(self, name: str, runner: Callable):
        """
        注册任务执行器
        
        Args:
            name: 执行器名称
            runner: 执行函数
        """
        self.task_runners[name] = runner
    
    async def execute_task_async(
        self,
        task_id: str,
        runner_name: str,
        **kwargs
    ) -> Dict[str, Any]:
        """
        异步执行任务
        
        Args:
            task_id: 任务 ID
            runner_name: 执行器名称
            **kwargs: 额外参数
        
        Returns:
            执行结果
        """
        runner = self.task_runners.get(runner_name)
        if runner is None:
            return {'success': False, 'error': f'Runner not found: {runner_name}'}
        
        return runner(task_id=task_id, **kwargs)
    
    def execute_task_sync(
        self,
        task_id: str,
        query_func: Callable,
        total_func: Optional[Callable] = None
    ) -> Dict[str, Any]:
        """
        同步执行任务
        
        Args:
            task_id: 任务 ID
            query_func: 查询函数
            total_func: 总数查询函数
        
        Returns:
            执行结果
        """
        return self.export_executor.execute_export(
            task_id=task_id,
            query_func=query_func,
            formatter_factory=None,  # 使用已注册的格式化器
            total_func=total_func
        )
    
    # ==================== 清理相关 ====================
    
    def cleanup_task(self, task_id: str, delete_file: bool = True):
        """
        清理任务
        
        Args:
            task_id: 任务 ID
            delete_file: 是否删除文件
        """
        self.export_executor.cleanup_task(task_id, delete_file)
