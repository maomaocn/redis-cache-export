"""
缓存导出装饰器
"""

import functools
import time
import uuid
from typing import Any, Dict, Optional, Callable, List
from .core import CacheExportManager
from .formatters import CSVFormatter, ExcelFormatter, JSONFormatter


def cache_query(
    manager: CacheExportManager,
    func_name: Optional[str] = None,
    exclude_params: Optional[set] = None,
    ttl: int = 3600,
    auto_count: bool = True
):
    """
    缓存查询装饰器
    
    用法：
        @cache_query(manager=manager)
        def get_devices(**params):
            return Device.query.filter(**params)
    
    Args:
        manager: CacheExportManager 实例
        func_name: 函数名称（可选，默认使用被装饰函数的名称）
        exclude_params: 排除的参数（分页参数）
        ttl: 缓存时间（秒）
        auto_count: 是否自动统计总数
    
    Returns:
        装饰器函数
    """
    
    def decorator(func: Callable) -> Callable:
        # 获取函数名称
        _func_name = func_name or func.__name__
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # 合并位置参数和关键字参数
            params = kwargs.copy()
            if args:
                # 尝试将位置参数转换为关键字参数
                # 这需要知道函数的参数名，这里简化处理
                pass
            
            # 缓存查询条件
            query_id = manager.cache_query(
                func_name=_func_name,
                params=params,
                exclude_params=exclude_params,
                extra_data={'total': kwargs.get('total', 0)} if 'total' in kwargs else None
            )
            
            # 执行查询
            result = func(*args, **kwargs)
            
            # 如果返回结果是字典，添加 query_id
            if isinstance(result, dict):
                result['query_id'] = query_id
            elif isinstance(result, tuple) and len(result) >= 2:
                # 假设返回的是 (data, total) 格式
                data, total = result[0], result[1]
                if isinstance(data, list):
                    # 更新缓存的 total
                    manager.query_cache.update_query(
                        query_id, 
                        {'total': total}
                    )
                    result = {
                        'data': data,
                        'total': total,
                        'query_id': query_id
                    }
            
            return result
        
        # 添加额外的方法
        wrapper.cache_export = lambda: get_export_handler(manager, _func_name)
        
        return wrapper
    
    return decorator


def get_export_handler(manager: CacheExportManager, func_name: str):
    """
    获取导出处理器
    
    Args:
        manager: 管理器实例
        func_name: 函数名称
    
    Returns:
        导出处理器
    """
    
    class ExportHandler:
        def __init__(self, manager: CacheExportManager, func_name: str):
            self.manager = manager
            self.func_name = func_name
        
        def submit(
            self,
            query_id: str,
            fields: List[str],
            export_format: str = 'csv',
            user_id: Optional[str] = None,
            total_count: Optional[int] = None
        ) -> str:
            """
            提交导出任务
            
            Args:
                query_id: 查询 ID
                fields: 导出字段
                export_format: 导出格式
                user_id: 用户 ID
                total_count: 总数量
            
            Returns:
                任务 ID
            """
            return self.manager.submit_export_task(
                query_id=query_id,
                fields=fields,
                export_format=export_format,
                user_id=user_id,
                total_count=total_count
            )
        
        def progress(self, task_id: str) -> Dict[str, Any]:
            """
            查询任务进度
            
            Args:
                task_id: 任务 ID
            
            Returns:
                进度信息
            """
            return self.manager.get_task_progress(task_id)
        
        def download(self, task_id: str) -> Optional[tuple]:
            """
            获取导出文件
            
            Args:
                task_id: 任务 ID
            
            Returns:
                (文件路径, 文件名, MIME类型) 或 None
            """
            return self.manager.get_export_file(task_id)
        
        def cancel(self, task_id: str) -> bool:
            """
            取消任务
            
            Args:
                task_id: 任务 ID
            
            Returns:
                是否成功
            """
            return self.manager.cancel_task(task_id)
    
    return ExportHandler(manager, func_name)


# 便捷函数

def setup_manager(
    redis_url: str = 'redis://localhost:6379/0',
    storage_path: str = '/tmp/exports',
    batch_size: int = 1000,
    query_ttl: int = 3600,
    progress_ttl: int = 86400
) -> CacheExportManager:
    """
    创建并配置管理器
    
    Args:
        redis_url: Redis 连接 URL
        storage_path: 文件存储路径
        batch_size: 分批查询大小
        query_ttl: 查询缓存时间（秒）
        progress_ttl: 进度保存时间（秒）
    
    Returns:
        CacheExportManager 实例
    """
    manager = CacheExportManager(
        redis_url=redis_url,
        storage_path=storage_path,
        batch_size=batch_size,
        query_ttl=query_ttl,
        progress_ttl=progress_ttl
    )
    
    # 注册默认格式化器
    manager.register_formatter('csv', CSVFormatter)
    manager.register_formatter('excel', ExcelFormatter)
    manager.register_formatter('json', JSONFormatter)
    
    return manager
