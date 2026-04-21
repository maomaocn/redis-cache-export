"""
日志管理模块
提供统一的日志配置和管理
"""

import logging
import sys
import os
from datetime import datetime
from typing import Optional, Dict, Any
from pathlib import Path
import json


# 日志格式
DEFAULT_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DETAILED_FORMAT = (
    "%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d | "
    "%(funcName)s | %(message)s"
)

# 日志颜色（终端输出）
COLORS = {
    'DEBUG': '\033[36m',     # 青色
    'INFO': '\033[32m',      # 绿色
    'WARNING': '\033[33m',   # 黄色
    'ERROR': '\033[31m',     # 红色
    'CRITICAL': '\033[35m',  # 紫色
    'RESET': '\033[0m'       # 重置
}


class ColoredFormatter(logging.Formatter):
    """
    彩色日志格式化器
    为终端输出添加颜色
    """
    
    def format(self, record):
        # 获取颜色
        color = COLORS.get(record.levelname, COLORS['RESET'])
        reset = COLORS['RESET']
        
        # 格式化消息
        message = super().format(record)
        
        # 添加颜色
        return f"{color}{message}{reset}"


class JSONFormatter(logging.Formatter):
    """
    JSON 日志格式化器
    用于结构化日志输出
    """
    
    def format(self, record):
        log_data = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # 添加额外字段
        if hasattr(record, 'extra_data'):
            log_data['extra'] = record.extra_data
        
        # 添加异常信息
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)
        
        return json.dumps(log_data, ensure_ascii=False)


class LoggerAdapter(logging.LoggerAdapter):
    """
    日志适配器
    支持添加额外上下文信息
    """
    
    def process(self, msg, kwargs):
        # 合并额外信息
        extra_data = kwargs.pop('extra_data', {})
        extra_data.update(self.extra or {})
        
        if extra_data:
            kwargs['extra'] = {'extra_data': extra_data}
        
        return msg, kwargs


class LogManager:
    """
    日志管理器
    提供统一的日志配置和获取接口
    """
    
    _instance = None
    _loggers: Dict[str, logging.Logger] = {}
    _configured = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    @classmethod
    def configure(
        cls,
        level: str = 'INFO',
        log_dir: Optional[str] = None,
        max_file_size: int = 10 * 1024 * 1024,  # 10MB
        backup_count: int = 5,
        format_type: str = 'default',  # 'default', 'detailed', 'json'
        console_output: bool = True,
        console_colors: bool = True
    ):
        """
        配置日志系统
        
        Args:
            level: 日志级别 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_dir: 日志文件目录
            max_file_size: 单个日志文件最大大小
            backup_count: 保留的日志文件数量
            format_type: 日志格式类型
            console_output: 是否输出到控制台
            console_colors: 控制台输出是否使用颜色
        """
        if cls._configured:
            return
        
        # 获取根日志器
        root_logger = logging.getLogger('redis_cache_export')
        root_logger.setLevel(getattr(logging, level.upper()))
        
        # 清除现有处理器
        root_logger.handlers.clear()
        
        # 选择格式
        if format_type == 'json':
            formatter = JSONFormatter()
        elif format_type == 'detailed':
            formatter = logging.Formatter(DETAILED_FORMAT)
        else:
            formatter = logging.Formatter(DEFAULT_FORMAT)
        
        # 控制台处理器
        if console_output:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(getattr(logging, level.upper()))
            
            if console_colors and format_type != 'json':
                console_handler.setFormatter(ColoredFormatter(
                    DETAILED_FORMAT if format_type == 'detailed' else DEFAULT_FORMAT
                ))
            else:
                console_handler.setFormatter(formatter)
            
            root_logger.addHandler(console_handler)
        
        # 文件处理器
        if log_dir:
            log_path = Path(log_dir)
            log_path.mkdir(parents=True, exist_ok=True)
            
            log_file = log_path / 'export.log'
            
            # 使用 RotatingFileHandler 实现日志轮转
            from logging.handlers import RotatingFileHandler
            
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=max_file_size,
                backupCount=backup_count,
                encoding='utf-8'
            )
            file_handler.setLevel(getattr(logging, level.upper()))
            file_handler.setFormatter(formatter)
            
            root_logger.addHandler(file_handler)
            
            # 错误日志单独文件
            error_log_file = log_path / 'error.log'
            error_handler = RotatingFileHandler(
                error_log_file,
                maxBytes=max_file_size,
                backupCount=backup_count,
                encoding='utf-8'
            )
            error_handler.setLevel(logging.ERROR)
            error_handler.setFormatter(formatter)
            
            root_logger.addHandler(error_handler)
        
        cls._configured = True
    
    @classmethod
    def get_logger(
        cls,
        name: str = 'redis_cache_export',
        extra: Optional[Dict[str, Any]] = None
    ) -> LoggerAdapter:
        """
        获取日志器
        
        Args:
            name: 日志器名称
            extra: 额外的上下文信息
        
        Returns:
            LoggerAdapter 实例
        """
        # 确保名称以 redis_cache_export 开头
        if not name.startswith('redis_cache_export'):
            name = f'redis_cache_export.{name}'
        
        if name not in cls._loggers:
            logger = logging.getLogger(name)
            cls._loggers[name] = logger
        
        return LoggerAdapter(cls._loggers[name], extra or {})
    
    @classmethod
    def set_level(cls, level: str):
        """
        设置日志级别
        
        Args:
            level: 日志级别
        """
        root_logger = logging.getLogger('redis_cache_export')
        root_logger.setLevel(getattr(logging, level.upper()))
        
        for handler in root_logger.handlers:
            handler.setLevel(getattr(logging, level.upper()))


# ==================== 便捷函数 ====================

def get_logger(name: str = 'redis_cache_export') -> LoggerAdapter:
    """
    获取日志器
    
    Args:
        name: 日志器名称
    
    Returns:
        LoggerAdapter 实例
    """
    return LogManager.get_logger(name)


def configure_logging(**kwargs):
    """
    配置日志系统
    
    Args:
        **kwargs: LogManager.configure 的参数
    """
    LogManager.configure(**kwargs)


# ==================== 装饰器 ====================

def log_function_call(logger: Optional[LoggerAdapter] = None):
    """
    函数调用日志装饰器
    
    用法:
        @log_function_call()
        def my_function(arg1, arg2):
            pass
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            nonlocal logger
            if logger is None:
                logger = get_logger(func.__module__)
            
            logger.debug(
                f"Calling {func.__name__}",
                extra_data={'args': str(args)[:200], 'kwargs': str(kwargs)[:200]}
            )
            
            try:
                result = func(*args, **kwargs)
                logger.debug(
                    f"Completed {func.__name__}",
                    extra_data={'result_type': type(result).__name__}
                )
                return result
            except Exception as e:
                logger.error(
                    f"Error in {func.__name__}: {str(e)}",
                    extra_data={'exception_type': type(e).__name__}
                )
                raise
        
        return wrapper
    return decorator


def log_execution_time(logger: Optional[LoggerAdapter] = None):
    """
    执行时间日志装饰器
    
    用法:
        @log_execution_time()
        def my_function():
            pass
    """
    import time
    
    def decorator(func):
        def wrapper(*args, **kwargs):
            nonlocal logger
            if logger is None:
                logger = get_logger(func.__module__)
            
            start_time = time.time()
            
            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time
                
                logger.info(
                    f"Executed {func.__name__} in {execution_time:.3f}s",
                    extra_data={
                        'function': func.__name__,
                        'execution_time': execution_time
                    }
                )
                
                return result
            except Exception as e:
                execution_time = time.time() - start_time
                
                logger.error(
                    f"Failed {func.__name__} after {execution_time:.3f}s: {str(e)}",
                    extra_data={
                        'function': func.__name__,
                        'execution_time': execution_time,
                        'exception': str(e)
                    }
                )
                raise
        
        return wrapper
    return decorator


# 默认配置
def init_default_logging():
    """初始化默认日志配置"""
    LogManager.configure(
        level=os.getenv('EXPORT_LOG_LEVEL', 'INFO'),
        console_output=True,
        console_colors=True,
        format_type='default'
    )


# 自动初始化（如果环境变量中设置了日志级别）
if os.getenv('EXPORT_LOG_LEVEL'):
    init_default_logging()
