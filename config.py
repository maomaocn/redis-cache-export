"""
配置管理模块
"""

import os
from typing import Optional
from dataclasses import dataclass, field


@dataclass
class RedisConfig:
    """Redis 配置"""
    url: str = 'redis://localhost:6379/0'
    prefix: str = 'export_cache'
    socket_timeout: int = 5
    socket_connect_timeout: int = 5
    
    @classmethod
    def from_env(cls) -> 'RedisConfig':
        """从环境变量创建配置"""
        return cls(
            url=os.getenv('REDIS_URL', 'redis://localhost:6379/0'),
            prefix=os.getenv('EXPORT_REDIS_PREFIX', 'export_cache'),
            socket_timeout=int(os.getenv('REDIS_SOCKET_TIMEOUT', '5')),
            socket_connect_timeout=int(os.getenv('REDIS_CONNECT_TIMEOUT', '5'))
        )


@dataclass
class StorageConfig:
    """存储配置"""
    path: str = '/tmp/exports'
    cleanup_after_download: bool = False
    max_file_size: int = 1024 * 1024 * 1024  # 1GB
    
    @classmethod
    def from_env(cls) -> 'StorageConfig':
        """从环境变量创建配置"""
        return cls(
            path=os.getenv('EXPORT_STORAGE_PATH', '/tmp/exports'),
            cleanup_after_download=os.getenv('EXPORT_CLEANUP_AFTER_DOWNLOAD', 'false').lower() == 'true',
            max_file_size=int(os.getenv('EXPORT_MAX_FILE_SIZE', str(1024 * 1024 * 1024)))
        )


@dataclass
class ExportConfig:
    """导出配置"""
    batch_size: int = 1000
    query_ttl: int = 3600  # 1小时
    progress_ttl: int = 86400  # 1天
    max_concurrent_exports: int = 10
    
    @classmethod
    def from_env(cls) -> 'ExportConfig':
        """从环境变量创建配置"""
        return cls(
            batch_size=int(os.getenv('EXPORT_BATCH_SIZE', '1000')),
            query_ttl=int(os.getenv('EXPORT_QUERY_TTL', '3600')),
            progress_ttl=int(os.getenv('EXPORT_PROGRESS_TTL', '86400')),
            max_concurrent_exports=int(os.getenv('EXPORT_MAX_CONCURRENT', '10'))
        )


@dataclass
class TaskQueueConfig:
    """任务队列配置"""
    backend: str = 'memory'  # 'memory' 或 'celery'
    max_workers: int = 4
    celery_broker_url: Optional[str] = None
    celery_result_backend: Optional[str] = None
    
    @classmethod
    def from_env(cls) -> 'TaskQueueConfig':
        """从环境变量创建配置"""
        backend = os.getenv('EXPORT_TASK_BACKEND', 'memory')
        return cls(
            backend=backend,
            max_workers=int(os.getenv('EXPORT_MAX_WORKERS', '4')),
            celery_broker_url=os.getenv('CELERY_BROKER_URL'),
            celery_result_backend=os.getenv('CELERY_RESULT_BACKEND')
        )


@dataclass
class Config:
    """主配置类"""
    redis: RedisConfig = field(default_factory=RedisConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    export: ExportConfig = field(default_factory=ExportConfig)
    task_queue: TaskQueueConfig = field(default_factory=TaskQueueConfig)
    debug: bool = False
    
    @classmethod
    def from_env(cls) -> 'Config':
        """从环境变量创建完整配置"""
        return cls(
            redis=RedisConfig.from_env(),
            storage=StorageConfig.from_env(),
            export=ExportConfig.from_env(),
            task_queue=TaskQueueConfig.from_env(),
            debug=os.getenv('EXPORT_DEBUG', 'false').lower() == 'true'
        )
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Config':
        """从字典创建配置"""
        redis_data = data.get('redis', {})
        storage_data = data.get('storage', {})
        export_data = data.get('export', {})
        task_data = data.get('task_queue', {})
        
        return cls(
            redis=RedisConfig(**redis_data) if redis_data else RedisConfig(),
            storage=StorageConfig(**storage_data) if storage_data else StorageConfig(),
            export=ExportConfig(**export_data) if export_data else ExportConfig(),
            task_queue=TaskQueueConfig(**task_data) if task_data else TaskQueueConfig(),
            debug=data.get('debug', False)
        )
    
    def to_dict(self) -> dict:
        """转换为字典"""
        from dataclasses import asdict
        return {
            'redis': asdict(self.redis),
            'storage': asdict(self.storage),
            'export': asdict(self.export),
            'task_queue': asdict(self.task_queue),
            'debug': self.debug
        }


# 默认配置实例
default_config = Config.from_env()
