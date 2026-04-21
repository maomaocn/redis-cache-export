"""
导出执行器模块
负责：执行导出任务，分批查询数据，生成文件
"""

import os
import time
import uuid
from typing import Any, Dict, Optional, Callable, TYPE_CHECKING
import redis

if TYPE_CHECKING:
    from .query_cache import QueryCache
    from .progress_tracker import ProgressTracker, TaskStatus


class ExportExecutor:
    """
    导出执行器
    
    功能：
    1. 从缓存获取查询条件
    2. 调用查询函数获取数据（分批）
    3. 使用格式化器生成文件
    4. 更新导出进度
    5. 处理取消请求
    """
    
    def __init__(
        self,
        redis_client: redis.Redis,
        query_cache: 'QueryCache',
        progress_tracker: 'ProgressTracker',
        storage_path: str = '/tmp/exports',
        batch_size: int = 1000
    ):
        """
        初始化导出执行器
        
        Args:
            redis_client: Redis 客户端实例
            query_cache: 查询缓存管理器
            progress_tracker: 进度追踪器
            storage_path: 文件存储路径
            batch_size: 分批查询大小
        """
        self.redis = redis_client
        self.query_cache = query_cache
        self.progress_tracker = progress_tracker
        self.storage_path = storage_path
        self.batch_size = batch_size
        
        # 确保存储目录存在
        os.makedirs(storage_path, exist_ok=True)
        
        # 格式化器注册表（后续会从 formatters 模块导入）
        self.formatters = {}
    
    def register_formatter(self, format_name: str, formatter_class: type):
        """
        注册格式化器
        
        Args:
            format_name: 格式名称（csv, excel, json）
            formatter_class: 格式化器类
        """
        self.formatters[format_name] = formatter_class
    
    def generate_task_id(self) -> str:
        """
        生成唯一的任务 ID
        
        Returns:
            任务 ID
        """
        return f"task_{uuid.uuid4().hex[:16]}"
    
    def generate_file_path(self, task_id: str, export_format: str) -> str:
        """
        生成文件存储路径
        
        Args:
            task_id: 任务 ID
            export_format: 导出格式
        
        Returns:
            文件路径
        """
        # 根据格式确定扩展名
        extensions = {
            'csv': '.csv',
            'excel': '.xlsx',
            'json': '.json'
        }
        ext = extensions.get(export_format, '.txt')
        
        filename = f"{task_id}{ext}"
        return os.path.join(self.storage_path, filename)
    
    def submit_task(
        self,
        query_id: str,
        fields: list,
        export_format: str,
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
            total_count: 总数据量（可选，如果不提供会自动统计）
        
        Returns:
            任务 ID
        """
        # 检查查询条件是否存在
        query_data = self.query_cache.get_query(query_id)
        if query_data is None:
            raise ValueError(f"Query cache not found: {query_id}")
        
        # 生成任务 ID
        task_id = self.generate_task_id()
        
        # 如果没有提供总数，使用缓存的 extra 中的 total
        if total_count is None:
            total_count = query_data.get('extra', {}).get('total', 0)
        
        # 初始化进度
        self.progress_tracker.init_progress(
            task_id=task_id,
            total=total_count,
            query_id=query_id,
            export_format=export_format,
            fields=fields,
            user_id=user_id or query_data.get('user_id')
        )
        
        return task_id
    
    def execute_export(
        self,
        task_id: str,
        query_func: Callable,
        formatter_factory: Callable,
        total_func: Optional[Callable] = None
    ) -> Dict[str, Any]:
        """
        执行导出任务
        
        Args:
            task_id: 任务 ID
            query_func: 查询函数，接收 (params, offset, limit) 返回数据列表
            formatter_factory: 格式化器工厂函数
            total_func: 总数查询函数（可选）
        
        Returns:
            导出结果
        """
        from .progress_tracker import TaskStatus
        
        # 获取进度数据
        progress = self.progress_tracker.get_progress(task_id)
        if progress is None:
            return {'success': False, 'error': 'Task not found'}
        
        # 检查是否已取消
        if self.progress_tracker.is_cancelled(task_id):
            return {'success': False, 'error': 'Task cancelled'}
        
        # 获取缓存的查询条件
        query_data = self.query_cache.get_query(progress['query_id'])
        if query_data is None:
            self.progress_tracker.update_progress(
                task_id, 
                status=TaskStatus.FAILED.value,
                error_message='Query cache expired'
            )
            return {'success': False, 'error': 'Query cache expired'}
        
        # 更新状态为处理中
        self.progress_tracker.update_progress(task_id, status=TaskStatus.PROCESSING.value)
        
        try:
            params = query_data['params']
            fields = progress['fields']
            export_format = progress['export_format']
            total = progress['total']
            
            # 如果没有提供总数，尝试查询
            if total == 0 and total_func:
                total = total_func(params)
                progress['total'] = total
            
            # 检查格式化器
            if export_format not in self.formatters:
                raise ValueError(f"Unsupported format: {export_format}")
            
            # 生成文件路径
            file_path = self.generate_file_path(task_id, export_format)
            
            # 创建格式化器
            formatter = self.formatters[export_format](fields)
            formatter.open(file_path)
            
            # 分批查询并写入
            processed = 0
            offset = 0
            
            while True:
                # 检查是否取消
                if self.progress_tracker.is_cancelled(task_id):
                    formatter.close()
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    return {'success': False, 'error': 'Task cancelled'}
                
                # 查询一批数据
                batch = query_func(params, offset, self.batch_size)
                
                if not batch:
                    break
                
                # 写入文件
                formatter.write_batch(batch)
                
                processed += len(batch)
                offset += self.batch_size
                
                # 更新进度
                self.progress_tracker.update_progress(
                    task_id,
                    processed=processed
                )
                
                # 如果已知总数，判断是否完成
                if total > 0 and processed >= total:
                    break
            
            # 关闭文件
            formatter.close()
            
            # 获取文件大小
            file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
            
            # 更新完成状态
            self.progress_tracker.update_progress(
                task_id,
                status=TaskStatus.COMPLETED.value,
                file_path=file_path,
                file_size=file_size,
                processed=processed
            )
            
            return {
                'success': True,
                'task_id': task_id,
                'file_path': file_path,
                'file_size': file_size,
                'total_records': processed
            }
            
        except Exception as e:
            # 更新失败状态
            self.progress_tracker.update_progress(
                task_id,
                status=TaskStatus.FAILED.value,
                error_message=str(e)
            )
            
            # 清理文件
            if 'file_path' in locals() and os.path.exists(file_path):
                os.remove(file_path)
            
            return {'success': False, 'error': str(e)}
    
    def get_file(self, task_id: str) -> Optional[tuple]:
        """
        获取导出文件
        
        Args:
            task_id: 任务 ID
        
        Returns:
            (文件路径, 文件名, MIME类型) 或 None
        """
        progress = self.progress_tracker.get_progress(task_id)
        
        if progress is None:
            return None
        
        if progress.get('status') != 'completed':
            return None
        
        file_path = progress.get('file_path')
        if not file_path or not os.path.exists(file_path):
            return None
        
        # 确定 MIME 类型
        export_format = progress.get('export_format', 'csv')
        mime_types = {
            'csv': 'text/csv',
            'excel': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'json': 'application/json'
        }
        mime_type = mime_types.get(export_format, 'application/octet-stream')
        
        # 生成文件名
        filename = os.path.basename(file_path)
        
        return (file_path, filename, mime_type)
    
    def cleanup_task(self, task_id: str, delete_file: bool = True):
        """
        清理任务
        
        Args:
            task_id: 任务 ID
            delete_file: 是否删除文件
        """
        progress = self.progress_tracker.get_progress(task_id)
        
        if progress:
            file_path = progress.get('file_path')
            if delete_file and file_path and os.path.exists(file_path):
                os.remove(file_path)
        
        self.progress_tracker.delete_progress(task_id)
