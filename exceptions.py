"""
自定义异常类
提供清晰的异常层级和详细的错误信息
"""


class CacheExportError(Exception):
    """
    缓存导出基础异常
    所有自定义异常的基类
    """
    
    def __init__(self, message: str, code: str = None, details: dict = None):
        """
        初始化异常
        
        Args:
            message: 错误消息
            code: 错误代码
            details: 错误详情
        """
        self.message = message
        self.code = code or 'UNKNOWN_ERROR'
        self.details = details or {}
        super().__init__(self.message)
    
    def to_dict(self) -> dict:
        """转换为字典，用于 API 响应"""
        return {
            'success': False,
            'error': {
                'code': self.code,
                'message': self.message,
                'details': self.details
            }
        }
    
    def __str__(self):
        return f"[{self.code}] {self.message}"


# ==================== 查询缓存相关异常 ====================

class QueryCacheError(CacheExportError):
    """查询缓存异常基类"""
    pass


class QueryNotFoundError(QueryCacheError):
    """查询条件不存在或已过期"""
    
    def __init__(self, query_id: str):
        super().__init__(
            message=f"Query cache not found or expired: {query_id}",
            code='QUERY_NOT_FOUND',
            details={'query_id': query_id}
        )


class QueryCacheExpiredError(QueryCacheError):
    """查询条件已过期"""
    
    def __init__(self, query_id: str, expired_at: str = None):
        details = {'query_id': query_id}
        if expired_at:
            details['expired_at'] = expired_at
        
        super().__init__(
            message=f"Query cache has expired: {query_id}",
            code='QUERY_EXPIRED',
            details=details
        )


class InvalidQueryParamsError(QueryCacheError):
    """无效的查询参数"""
    
    def __init__(self, reason: str, params: dict = None):
        details = {'reason': reason}
        if params:
            details['params'] = params
        
        super().__init__(
            message=f"Invalid query parameters: {reason}",
            code='INVALID_QUERY_PARAMS',
            details=details
        )


# ==================== 导出任务相关异常 ====================

class ExportTaskError(CacheExportError):
    """导出任务异常基类"""
    pass


class TaskNotFoundError(ExportTaskError):
    """任务不存在"""
    
    def __init__(self, task_id: str):
        super().__init__(
            message=f"Export task not found: {task_id}",
            code='TASK_NOT_FOUND',
            details={'task_id': task_id}
        )


class TaskAlreadyExistsError(ExportTaskError):
    """任务已存在"""
    
    def __init__(self, task_id: str):
        super().__init__(
            message=f"Export task already exists: {task_id}",
            code='TASK_ALREADY_EXISTS',
            details={'task_id': task_id}
        )


class TaskCancelledError(ExportTaskError):
    """任务已被取消"""
    
    def __init__(self, task_id: str, cancelled_at: str = None):
        details = {'task_id': task_id}
        if cancelled_at:
            details['cancelled_at'] = cancelled_at
        
        super().__init__(
            message=f"Export task was cancelled: {task_id}",
            code='TASK_CANCELLED',
            details=details
        )


class TaskFailedError(ExportTaskError):
    """任务执行失败"""
    
    def __init__(self, task_id: str, reason: str, traceback: str = None):
        details = {'task_id': task_id, 'reason': reason}
        if traceback:
            details['traceback'] = traceback
        
        super().__init__(
            message=f"Export task failed: {reason}",
            code='TASK_FAILED',
            details=details
        )


class TaskTimeoutError(ExportTaskError):
    """任务执行超时"""
    
    def __init__(self, task_id: str, timeout: int):
        super().__init__(
            message=f"Export task timed out after {timeout} seconds",
            code='TASK_TIMEOUT',
            details={'task_id': task_id, 'timeout': timeout}
        )


class TaskStillRunningError(ExportTaskError):
    """任务仍在运行中"""
    
    def __init__(self, task_id: str, progress: int = None):
        details = {'task_id': task_id}
        if progress is not None:
            details['progress'] = progress
        
        super().__init__(
            message=f"Export task is still running: {task_id}",
            code='TASK_RUNNING',
            details=details
        )


# ==================== 格式化相关异常 ====================

class FormatterError(CacheExportError):
    """格式化异常基类"""
    pass


class UnsupportedFormatError(FormatterError):
    """不支持的导出格式"""
    
    def __init__(self, format_name: str, supported_formats: list = None):
        details = {'format': format_name}
        if supported_formats:
            details['supported_formats'] = supported_formats
        
        super().__init__(
            message=f"Unsupported export format: {format_name}",
            code='UNSUPPORTED_FORMAT',
            details=details
        )


class FileWriteError(FormatterError):
    """文件写入失败"""
    
    def __init__(self, file_path: str, reason: str):
        super().__init__(
            message=f"Failed to write file: {reason}",
            code='FILE_WRITE_ERROR',
            details={'file_path': file_path, 'reason': reason}
        )


class FileNotFoundError(FormatterError):
    """文件不存在"""
    
    def __init__(self, file_path: str):
        super().__init__(
            message=f"File not found: {file_path}",
            code='FILE_NOT_FOUND',
            details={'file_path': file_path}
        )


# ==================== 存储相关异常 ====================

class StorageError(CacheExportError):
    """存储异常基类"""
    pass


class StorageFullError(StorageError):
    """存储空间不足"""
    
    def __init__(self, required_space: int = None, available_space: int = None):
        details = {}
        if required_space:
            details['required_space'] = required_space
        if available_space:
            details['available_space'] = available_space
        
        super().__init__(
            message="Storage is full",
            code='STORAGE_FULL',
            details=details
        )


class StoragePermissionError(StorageError):
    """存储权限不足"""
    
    def __init__(self, path: str, required_permission: str = None):
        details = {'path': path}
        if required_permission:
            details['required_permission'] = required_permission
        
        super().__init__(
            message=f"Permission denied for storage path: {path}",
            code='STORAGE_PERMISSION_DENIED',
            details=details
        )


# ==================== Redis 相关异常 ====================

class RedisConnectionError(CacheExportError):
    """Redis 连接异常"""
    
    def __init__(self, redis_url: str, reason: str = None):
        details = {'redis_url': redis_url}
        if reason:
            details['reason'] = reason
        
        super().__init__(
            message=f"Failed to connect to Redis: {reason or 'Unknown reason'}",
            code='REDIS_CONNECTION_ERROR',
            details=details
        )


class RedisOperationError(CacheExportError):
    """Redis 操作异常"""
    
    def __init__(self, operation: str, key: str = None, reason: str = None):
        details = {'operation': operation}
        if key:
            details['key'] = key
        if reason:
            details['reason'] = reason
        
        super().__init__(
            message=f"Redis operation failed: {operation}",
            code='REDIS_OPERATION_ERROR',
            details=details
        )


# ==================== 数据相关异常 ====================

class DataError(CacheExportError):
    """数据异常基类"""
    pass


class EmptyDataError(DataError):
    """数据为空"""
    
    def __init__(self, message: str = "No data to export"):
        super().__init__(
            message=message,
            code='EMPTY_DATA'
        )


class InvalidFieldsError(DataError):
    """无效的字段"""
    
    def __init__(self, invalid_fields: list, available_fields: list = None):
        details = {'invalid_fields': invalid_fields}
        if available_fields:
            details['available_fields'] = available_fields
        
        super().__init__(
            message=f"Invalid fields specified: {invalid_fields}",
            code='INVALID_FIELDS',
            details=details
        )


class DataTooLargeError(DataError):
    """数据量过大"""
    
    def __init__(self, data_size: int, max_size: int):
        super().__init__(
            message=f"Data size ({data_size}) exceeds maximum limit ({max_size})",
            code='DATA_TOO_LARGE',
            details={'data_size': data_size, 'max_size': max_size}
        )


# ==================== 配置相关异常 ====================

class ConfigurationError(CacheExportError):
    """配置异常"""
    
    def __init__(self, config_key: str, reason: str):
        super().__init__(
            message=f"Configuration error for '{config_key}': {reason}",
            code='CONFIGURATION_ERROR',
            details={'config_key': config_key, 'reason': reason}
        )
