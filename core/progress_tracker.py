"""
进度追踪模块
负责：追踪导出任务进度，支持实时查询
"""

import json
import time
from typing import Optional, Dict, Any
from enum import Enum
import redis


class TaskStatus(str, Enum):
    """任务状态"""
    PENDING = "pending"         # 等待处理
    PROCESSING = "processing"   # 处理中
    COMPLETED = "completed"     # 已完成
    FAILED = "failed"           # 失败
    CANCELLED = "cancelled"     # 已取消


class ProgressTracker:
    """
    导出进度追踪器
    
    功能：
    1. 记录任务进度（已处理条数、总条数）
    2. 支持实时查询进度
    3. 预估剩余时间
    4. 支持取消任务
    """
    
    def __init__(self, redis_client: redis.Redis, prefix: str = 'export_progress'):
        """
        初始化进度追踪器
        
        Args:
            redis_client: Redis 客户端实例
            prefix: Redis key 前缀
        """
        self.redis = redis_client
        self.prefix = prefix
    
    def _get_progress_key(self, task_id: str) -> str:
        """获取进度存储的完整 key"""
        return f"{self.prefix}:{task_id}"
    
    def init_progress(
        self,
        task_id: str,
        total: int,
        query_id: str,
        export_format: str,
        fields: list,
        user_id: Optional[str] = None,
        ttl: int = 86400  # 默认保存1天
    ) -> Dict[str, Any]:
        """
        初始化任务进度
        
        Args:
            task_id: 任务ID
            total: 总数据条数
            query_id: 查询ID
            export_format: 导出格式
            fields: 导出字段
            user_id: 用户ID
            ttl: 进度数据保存时间
        
        Returns:
            初始进度数据
        """
        progress_data = {
            'task_id': task_id,
            'query_id': query_id,
            'user_id': user_id,
            'status': TaskStatus.PENDING.value,
            'total': total,
            'processed': 0,
            'failed': 0,
            'export_format': export_format,
            'fields': fields,
            'file_path': None,
            'file_size': 0,
            'error_message': None,
            'started_at': None,
            'completed_at': None,
            'created_at': time.time()
        }
        
        progress_key = self._get_progress_key(task_id)
        self.redis.setex(
            progress_key,
            ttl,
            json.dumps(progress_data, ensure_ascii=False)
        )
        
        return progress_data
    
    def update_progress(
        self,
        task_id: str,
        processed: Optional[int] = None,
        failed: Optional[int] = None,
        status: Optional[str] = None,
        file_path: Optional[str] = None,
        file_size: Optional[int] = None,
        error_message: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        更新任务进度
        
        Args:
            task_id: 任务ID
            processed: 已处理条数
            failed: 失败条数
            status: 任务状态
            file_path: 文件路径
            file_size: 文件大小
            error_message: 错误信息
        
        Returns:
            更新后的进度数据，如果任务不存在返回 None
        """
        progress_key = self._get_progress_key(task_id)
        data = self.redis.get(progress_key)
        
        if data is None:
            return None
        
        try:
            progress_data = json.loads(data.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None
        
        # 更新字段
        if processed is not None:
            progress_data['processed'] = processed
        
        if failed is not None:
            progress_data['failed'] = failed
        
        if status is not None:
            progress_data['status'] = status
            
            # 记录时间戳
            if status == TaskStatus.PROCESSING.value and progress_data.get('started_at') is None:
                progress_data['started_at'] = time.time()
            
            if status in (TaskStatus.COMPLETED.value, TaskStatus.FAILED.value, TaskStatus.CANCELLED.value):
                progress_data['completed_at'] = time.time()
        
        if file_path is not None:
            progress_data['file_path'] = file_path
        
        if file_size is not None:
            progress_data['file_size'] = file_size
        
        if error_message is not None:
            progress_data['error_message'] = error_message
        
        # 保存更新
        ttl = self.redis.ttl(progress_key)
        if ttl <= 0:
            ttl = 86400  # 默认1天
        
        self.redis.setex(
            progress_key,
            ttl,
            json.dumps(progress_data, ensure_ascii=False)
        )
        
        return progress_data
    
    def get_progress(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        获取任务进度
        
        Args:
            task_id: 任务ID
        
        Returns:
            进度数据，如果不存在返回 None
        """
        progress_key = self._get_progress_key(task_id)
        data = self.redis.get(progress_key)
        
        if data is None:
            return None
        
        try:
            progress_data = json.loads(data.decode('utf-8'))
            
            # 计算百分比
            total = progress_data.get('total', 0)
            processed = progress_data.get('processed', 0)
            
            if total > 0:
                progress_data['percentage'] = round((processed / total) * 100, 2)
            else:
                progress_data['percentage'] = 0
            
            # 计算预估剩余时间
            started_at = progress_data.get('started_at')
            if started_at and processed > 0 and total > processed:
                elapsed = time.time() - started_at
                speed = processed / elapsed if elapsed > 0 else 0
                remaining_items = total - processed
                estimated_seconds = remaining_items / speed if speed > 0 else 0
                progress_data['estimated_remaining_seconds'] = round(estimated_seconds)
            else:
                progress_data['estimated_remaining_seconds'] = None
            
            return progress_data
            
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None
    
    def cancel_task(self, task_id: str) -> bool:
        """
        取消任务
        
        Args:
            task_id: 任务ID
        
        Returns:
            是否取消成功
        """
        progress_data = self.update_progress(task_id, status=TaskStatus.CANCELLED.value)
        return progress_data is not None
    
    def is_cancelled(self, task_id: str) -> bool:
        """
        检查任务是否被取消
        
        Args:
            task_id: 任务ID
        
        Returns:
            是否已取消
        """
        progress = self.get_progress(task_id)
        if progress is None:
            return False
        return progress.get('status') == TaskStatus.CANCELLED.value
    
    def delete_progress(self, task_id: str) -> bool:
        """
        删除任务进度记录
        
        Args:
            task_id: 任务ID
        
        Returns:
            是否删除成功
        """
        progress_key = self._get_progress_key(task_id)
        return bool(self.redis.delete(progress_key))
    
    def get_user_tasks(
        self,
        user_id: str,
        status: Optional[str] = None,
        limit: int = 10
    ) -> list:
        """
        获取用户的任务列表
        
        Args:
            user_id: 用户ID
            status: 过滤状态（可选）
            limit: 返回数量限制
        
        Returns:
            任务列表
        """
        # 扫描所有进度 key
        pattern = f"{self.prefix}:*"
        keys = self.redis.keys(pattern)
        
        tasks = []
        for key in keys[:limit * 10]:  # 扫描更多key来过滤
            try:
                data = self.redis.get(key)
                if data:
                    progress_data = json.loads(data.decode('utf-8'))
                    
                    # 过滤用户
                    if progress_data.get('user_id') != user_id:
                        continue
                    
                    # 过滤状态
                    if status and progress_data.get('status') != status:
                        continue
                    
                    tasks.append(progress_data)
                    
                    if len(tasks) >= limit:
                        break
            except Exception:
                continue
        
        # 按创建时间倒序
        tasks.sort(key=lambda x: x.get('created_at', 0), reverse=True)
        
        return tasks[:limit]
