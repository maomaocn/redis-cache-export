"""
框架适配器基类
定义所有框架适配器需要实现的接口
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Callable
from dataclasses import dataclass


@dataclass
class RequestInfo:
    """
    请求信息封装
    
    用于在不同框架间统一请求信息
    """
    params: Dict[str, Any]      # 请求参数
    headers: Dict[str, str]     # 请求头
    user_id: Optional[str]       # 用户ID
    request_id: Optional[str]    # 请求ID


@dataclass
class ResponseInfo:
    """
    响应信息封装
    
    用于在不同框架间统一响应格式
    """
    data: Any                   # 响应数据
    status_code: int = 200      # 状态码
    headers: Dict[str, str] = None  # 响应头
    
    def __post_init__(self):
        if self.headers is None:
            self.headers = {}


class BaseAdapter(ABC):
    """
    框架适配器基类
    
    所有框架适配器需要实现以下方法：
    - get_request_info: 从框架请求对象中提取请求信息
    - make_response: 创建框架特定的响应对象
    - register_route: 注册路由（自动生成导出接口）
    """
    
    def __init__(self, manager):
        """
        初始化适配器
        
        Args:
            manager: CacheExportManager 实例
        """
        self.manager = manager
    
    @abstractmethod
    def get_request_info(self, request) -> RequestInfo:
        """
        从框架请求对象中提取请求信息
        
        Args:
            request: 框架特定的请求对象
        
        Returns:
            RequestInfo: 统一的请求信息
        """
        pass
    
    @abstractmethod
    def make_response(self, response_info: ResponseInfo):
        """
        创建框架特定的响应对象
        
        Args:
            response_info: 统一的响应信息
        
        Returns:
            框架特定的响应对象
        """
        pass
    
    @abstractmethod
    def register_route(
        self,
        app,
        path: str,
        handler: Callable,
        methods: list = None,
        **kwargs
    ):
        """
        注册路由
        
        Args:
            app: 框架应用实例
            path: 路由路径
            handler: 处理函数
            methods: HTTP 方法列表
            **kwargs: 额外参数
        """
        pass
    
    @abstractmethod
    def get_file_response(
        self,
        file_path: str,
        filename: str,
        mime_type: str
    ):
        """
        创建文件下载响应
        
        Args:
            file_path: 文件路径
            filename: 文件名
            mime_type: MIME 类型
        
        Returns:
            框架特定的文件响应对象
        """
        pass
    
    def get_user_id(self, request) -> Optional[str]:
        """
        从请求中获取用户ID
        
        默认实现，子类可以覆盖
        
        Args:
            request: 框架特定的请求对象
        
        Returns:
            用户ID
        """
        info = self.get_request_info(request)
        return info.user_id
    
    def get_query_params(self, request) -> Dict[str, Any]:
        """
        从请求中获取查询参数
        
        Args:
            request: 框架特定的请求对象
        
        Returns:
            查询参数字典
        """
        info = self.get_request_info(request)
        return info.params
    
    def get_json_body(self, request) -> Dict[str, Any]:
        """
        从请求中获取 JSON body
        
        Args:
            request: 框架特定的请求对象
        
        Returns:
            JSON body 字典
        """
        # 默认实现，子类可能需要覆盖
        info = self.get_request_info(request)
        return info.params
    
    def json_response(
        self,
        data: Any,
        status_code: int = 200
    ):
        """
        创建 JSON 响应
        
        Args:
            data: 响应数据
            status_code: 状态码
        
        Returns:
            框架特定的 JSON 响应对象
        """
        return self.make_response(
            ResponseInfo(
                data=data,
                status_code=status_code,
                headers={'Content-Type': 'application/json'}
            )
        )
    
    def error_response(
        self,
        message: str,
        status_code: int = 400
    ):
        """
        创建错误响应
        
        Args:
            message: 错误消息
            status_code: 状态码
        
        Returns:
            框架特定的错误响应对象
        """
        return self.json_response(
            {'success': False, 'error': message},
            status_code
        )
