"""
工具函数模块
提供常用的工具函数
"""

import functools
import time
import traceback
from typing import Any, Callable, Optional, Type, Union
from contextlib import contextmanager

from .logger import get_logger
from .exceptions import CacheExportError


def deprecated(message: str):
    """
    标记函数已废弃
    
    Args:
        message: 废弃提示信息
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = get_logger(func.__module__)
            logger.warning(
                f"Function {func.__name__} is deprecated: {message}",
                extra_data={'function': func.__name__, 'deprecated_message': message}
            )
            return func(*args, **kwargs)
        return wrapper
    return decorator


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,),
    logger: Optional[Any] = None
):
    """
    重试装饰器
    
    Args:
        max_attempts: 最大重试次数
        delay: 初始延迟时间（秒）
        backoff: 延迟时间增长因子
        exceptions: 需要重试的异常类型
        logger: 日志器（可选）
    
    用法:
        @retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError,))
        def my_function():
            pass
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal logger
            if logger is None:
                logger = get_logger(func.__module__)
            
            last_exception = None
            current_delay = delay
            
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == max_attempts:
                        logger.error(
                            f"Function {func.__name__} failed after {max_attempts} attempts: {str(e)}",
                            extra_data={
                                'function': func.__name__,
                                'attempt': attempt,
                                'max_attempts': max_attempts,
                                'exception': str(e)
                            }
                        )
                        raise
                    
                    logger.warning(
                        f"Function {func.__name__} failed on attempt {attempt}/{max_attempts}, "
                        f"retrying in {current_delay}s: {str(e)}",
                        extra_data={
                            'function': func.__name__,
                            'attempt': attempt,
                            'delay': current_delay,
                            'exception': str(e)
                        }
                    )
                    
                    time.sleep(current_delay)
                    current_delay *= backoff
            
            raise last_exception
        
        return wrapper
    return decorator


def handle_exceptions(
    *exception_handlers: tuple,
    default_return: Any = None,
    reraise: bool = False,
    logger: Optional[Any] = None
):
    """
    统一异常处理装饰器
    
    Args:
        exception_handlers: (异常类型, 处理函数) 元组
        default_return: 默认返回值
        reraise: 是否重新抛出异常
        logger: 日志器（可选）
    
    用法:
        @handle_exceptions(
            (ValueError, lambda e: print(f"Value error: {e}")),
            (KeyError, lambda e: print(f"Key error: {e}")),
            reraise=False,
            default_return=None
        )
        def my_function():
            pass
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal logger
            if logger is None:
                logger = get_logger(func.__module__)
            
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # 查找匹配的处理器
                for exc_type, handler in exception_handlers:
                    if isinstance(e, exc_type):
                        try:
                            handler(e)
                        except Exception as handler_error:
                            logger.error(
                                f"Exception handler failed: {str(handler_error)}",
                                extra_data={
                                    'function': func.__name__,
                                    'original_exception': str(e),
                                    'handler_exception': str(handler_error)
                                }
                            )
                        break
                else:
                    # 没有匹配的处理器
                    logger.error(
                        f"Unhandled exception in {func.__name__}: {str(e)}\n"
                        f"{traceback.format_exc()}",
                        extra_data={
                            'function': func.__name__,
                            'exception_type': type(e).__name__,
                            'exception_message': str(e)
                        }
                    )
                
                if reraise:
                    raise
                
                return default_return
        
        return wrapper
    return decorator


@contextmanager
def error_context(
    operation: str,
    logger: Optional[Any] = None,
    reraise: bool = True,
    default_return: Any = None
):
    """
    错误处理上下文管理器
    
    Args:
        operation: 操作名称
        logger: 日志器（可选）
        reraise: 是否重新抛出异常
        default_return: 默认返回值
    
    用法:
        with error_context("database query"):
            result = db.query(...)
    """
    nonlocal logger
    if logger is None:
        logger = get_logger()
    
    try:
        yield
    except Exception as e:
        logger.error(
            f"Error during {operation}: {str(e)}\n{traceback.format_exc()}",
            extra_data={
                'operation': operation,
                'exception_type': type(e).__name__,
                'exception_message': str(e)
            }
        )
        
        if reraise:
            raise
        
        return default_return


def safe_execute(
    func: Callable,
    *args,
    default=None,
    log_errors: bool = True,
    **kwargs
) -> Any:
    """
    安全执行函数
    
    Args:
        func: 要执行的函数
        *args: 位置参数
        default: 出错时的默认返回值
        log_errors: 是否记录错误
        **kwargs: 关键字参数
    
    Returns:
        函数执行结果或默认值
    """
    try:
        return func(*args, **kwargs)
    except Exception as e:
        if log_errors:
            logger = get_logger(func.__module__ if hasattr(func, '__module__') else __name__)
            logger.error(
                f"Safe execute failed: {str(e)}",
                extra_data={
                    'function': func.__name__ if hasattr(func, '__name__') else str(func),
                    'exception_type': type(e).__name__
                }
            )
        return default


class Singleton:
    """
    单例模式基类
    
    用法:
        class MyManager(Singleton):
            pass
    """
    _instances = {}
    
    def __new__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__new__(cls)
        return cls._instances[cls]


class ContextVar:
    """
    上下文变量管理器
    
    用于存储请求级别的上下文信息
    """
    
    def __init__(self):
        self._data = {}
    
    def set(self, key: str, value: Any):
        """设置上下文变量"""
        self._data[key] = value
    
    def get(self, key: str, default: Any = None) -> Any:
        """获取上下文变量"""
        return self._data.get(key, default)
    
    def delete(self, key: str):
        """删除上下文变量"""
        self._data.pop(key, None)
    
    def clear(self):
        """清空所有上下文变量"""
        self._data.clear()
    
    def get_all(self) -> dict:
        """获取所有上下文变量"""
        return self._data.copy()


# 全局上下文变量
_global_context = ContextVar()


def get_context() -> ContextVar:
    """获取全局上下文变量"""
    return _global_context
