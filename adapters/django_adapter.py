"""
Django 框架适配器
"""

import json
from typing import Any, Dict, Optional, Callable
from django.http import HttpResponse, JsonResponse, FileResponse
from django.views.decorators.csrf import csrf_exempt

from .base import BaseAdapter, RequestInfo, ResponseInfo


class DjangoAdapter(BaseAdapter):
    """
    Django 框架适配器
    
    功能：
    1. 提取 Django 请求信息
    2. 创建 Django 响应对象
    3. 注册 Django 路由
    4. 处理文件下载
    """
    
    def get_request_info(self, django_request) -> RequestInfo:
        """从 Django 请求中提取信息"""
        # 获取 GET 和 POST 参数
        params = dict(django_request.GET)
        
        # 合并 POST 数据
        if django_request.method == 'POST':
            if django_request.content_type == 'application/json':
                try:
                    json_data = json.loads(django_request.body)
                    if isinstance(json_data, dict):
                        params.update(json_data)
                except Exception:
                    pass
            else:
                params.update(dict(django_request.POST))
        
        # 获取请求头
        headers = {k: v for k, v in django_request.META.items() if k.startswith('HTTP_')}
        
        # 获取用户ID
        user_id = None
        if hasattr(django_request, 'user') and django_request.user.is_authenticated:
            user_id = str(django_request.user.id)
        elif 'HTTP_X_USER_ID' in django_request.META:
            user_id = django_request.META.get('HTTP_X_USER_ID')
        
        request_id = django_request.META.get('HTTP_X_REQUEST_ID')
        
        return RequestInfo(
            params=params,
            headers=headers,
            user_id=user_id,
            request_id=request_id
        )
    
    def make_response(self, response_info: ResponseInfo):
        """创建 Django 响应对象"""
        if isinstance(response_info.data, HttpResponse):
            return response_info.data
        
        if isinstance(response_info.data, dict):
            response = JsonResponse(response_info.data)
        else:
            response = JsonResponse(response_info.data, safe=False)
        
        response.status_code = response_info.status_code
        
        for key, value in response_info.headers.items():
            response[key] = value
        
        return response
    
    def register_route(self, app, path: str, handler: Callable, methods: list = None, **kwargs):
        """
        注册 Django 路由
        
        注意：Django 的路由注册方式与 Flask/FastAPI 不同
        通常需要在 urls.py 中配置
        """
        if methods is None:
            methods = ['GET']
        
        # Django 使用装饰器方式
        @csrf_exempt
        def wrapped_handler(request, *args, **kwargs):
            return handler(request=request, adapter=self, *args, **kwargs)
        
        wrapped_handler.methods = methods
        wrapped_handler.path = path
        
        return wrapped_handler
    
    def get_file_response(self, file_path: str, filename: str, mime_type: str):
        """创建 Django 文件下载响应"""
        response = FileResponse(
            open(file_path, 'rb'),
            content_type=mime_type
        )
        response['Content-Disposition'] = f'attachment; filename="{filename}"'
        return response
    
    def get_json_body(self, django_request) -> Dict[str, Any]:
        """从 Django 请求中获取 JSON body"""
        if django_request.content_type == 'application/json':
            try:
                json_data = json.loads(django_request.body)
                return json_data if isinstance(json_data, dict) else {}
            except Exception:
                pass
        return {}
