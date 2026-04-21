"""
Flask 框架适配器
"""

import json
from typing import Any, Dict, Optional, Callable
from flask import request, jsonify, send_file, Response

from .base import BaseAdapter, RequestInfo, ResponseInfo


class FlaskAdapter(BaseAdapter):
    """
    Flask 框架适配器
    
    功能：
    1. 提取 Flask 请求信息
    2. 创建 Flask 响应对象
    3. 注册 Flask 路由
    4. 处理文件下载
    """
    
    def get_request_info(self, flask_request) -> RequestInfo:
        """
        从 Flask 请求中提取信息
        
        Args:
            flask_request: Flask request 对象
        
        Returns:
            RequestInfo
        """
        # 获取查询参数
        params = dict(flask_request.args)
        
        # 如果是 JSON 请求，合并 JSON body
        if flask_request.is_json:
            json_data = flask_request.get_json(silent=True) or {}
            if isinstance(json_data, dict):
                params.update(json_data)
        
        # 获取请求头
        headers = dict(flask_request.headers)
        
        # 尝试获取用户ID（从 header 或 session）
        user_id = None
        if hasattr(flask_request, 'user_id'):
            user_id = flask_request.user_id
        elif 'X-User-Id' in flask_request.headers:
            user_id = flask_request.headers.get('X-User-Id')
        
        # 请求 ID
        request_id = flask_request.headers.get('X-Request-Id')
        
        return RequestInfo(
            params=params,
            headers=headers,
            user_id=user_id,
            request_id=request_id
        )
    
    def make_response(self, response_info: ResponseInfo):
        """
        创建 Flask 响应对象
        
        Args:
            response_info: 响应信息
        
        Returns:
            Flask Response 对象
        """
        # 如果已经是 Response 对象，直接返回
        if isinstance(response_info.data, Response):
            return response_info.data
        
        # 如果是字典，转换为 JSON
        if isinstance(response_info.data, dict):
            response = jsonify(response_info.data)
        else:
            response = Response(
                response=json.dumps(response_info.data),
                status=response_info.status_code,
                mimetype='application/json'
            )
        
        # 设置额外的响应头
        for key, value in response_info.headers.items():
            response.headers[key] = value
        
        response.status_code = response_info.status_code
        
        return response
    
    def register_route(
        self,
        app,
        path: str,
        handler: Callable,
        methods: list = None,
        **kwargs
    ):
        """
        注册 Flask 路由
        
        Args:
            app: Flask 应用实例
            path: 路由路径
            handler: 处理函数
            methods: HTTP 方法列表
            **kwargs: 额外参数
        """
        if methods is None:
            methods = ['GET']
        
        app.route(path, methods=methods, **kwargs)(handler)
    
    def get_file_response(
        self,
        file_path: str,
        filename: str,
        mime_type: str
    ):
        """
        创建 Flask 文件下载响应
        
        Args:
            file_path: 文件路径
            filename: 文件名
            mime_type: MIME 类型
        
        Returns:
            Flask send_file 响应
        """
        return send_file(
            file_path,
            mimetype=mime_type,
            as_attachment=True,
            download_name=filename
        )
    
    def get_json_body(self, flask_request) -> Dict[str, Any]:
        """
        从 Flask 请求中获取 JSON body
        
        Args:
            flask_request: Flask request 对象
        
        Returns:
            JSON body 字典
        """
        if flask_request.is_json:
            return flask_request.get_json(silent=True) or {}
        return {}
    
    def wrap_handler(
        self,
        handler: Callable,
        **kwargs
    ) -> Callable:
        """
        包装处理函数，注入适配器
        
        Args:
            handler: 原始处理函数
            **kwargs: 额外参数
        
        Returns:
            包装后的处理函数
        """
        def wrapped(*args, **kwargs):
            # 注入 request
            return handler(request=request, adapter=self, *args, **kwargs)
        
        # 保持原函数的元信息
        wrapped.__name__ = handler.__name__
        wrapped.__doc__ = handler.__doc__
        
        return wrapped
