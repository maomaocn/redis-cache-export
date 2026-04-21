"""
FastAPI 框架适配器
"""

import json
from typing import Any, Dict, Optional, Callable
from fastapi import Request, Response, HTTPException
from fastapi.responses import JSONResponse, FileResponse

from .base import BaseAdapter, RequestInfo, ResponseInfo


class FastAPIAdapter(BaseAdapter):
    """
    FastAPI 框架适配器
    
    功能：
    1. 提取 FastAPI 请求信息
    2. 创建 FastAPI 响应对象
    3. 注册 FastAPI 路由
    4. 处理文件下载
    """
    
    def get_request_info(self, fastapi_request: Request) -> RequestInfo:
        """
        从 FastAPI 请求中提取信息
        
        Args:
            fastapi_request: FastAPI Request 对象
        
        Returns:
            RequestInfo
        """
        # 获取查询参数
        params = dict(fastapi_request.query_params)
        
        # 获取请求头
        headers = dict(fastapi_request.headers)
        
        # 尝试获取用户ID（从 state 或 header）
        user_id = None
        if hasattr(fastapi_request.state, 'user_id'):
            user_id = fastapi_request.state.user_id
        elif 'x-user-id' in headers:
            user_id = headers.get('x-user-id')
        
        # 请求 ID
        request_id = headers.get('x-request-id')
        
        return RequestInfo(
            params=params,
            headers=headers,
            user_id=user_id,
            request_id=request_id
        )
    
    def make_response(self, response_info: ResponseInfo):
        """
        创建 FastAPI 响应对象
        
        Args:
            response_info: 响应信息
        
        Returns:
            FastAPI Response 对象
        """
        response = JSONResponse(
            content=response_info.data,
            status_code=response_info.status_code,
            headers=response_info.headers
        )
        
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
        注册 FastAPI 路由
        
        Args:
            app: FastAPI 应用实例
            path: 路由路径
            handler: 处理函数
            methods: HTTP 方法列表
            **kwargs: 额外参数
        """
        if methods is None:
            methods = ['GET']
        
        # 将 methods 转换为小写，FastAPI 接受大写或小写
        methods = [m.upper() for m in methods]
        
        app.add_api_route(
            path=path,
            endpoint=handler,
            methods=methods,
            **kwargs
        )
    
    def get_file_response(
        self,
        file_path: str,
        filename: str,
        mime_type: str
    ):
        """
        创建 FastAPI 文件下载响应
        
        Args:
            file_path: 文件路径
            filename: 文件名
            mime_type: MIME 类型
        
        Returns:
            FastAPI FileResponse
        """
        return FileResponse(
            path=file_path,
            filename=filename,
            media_type=mime_type
        )
    
    async def get_json_body(self, fastapi_request: Request) -> Dict[str, Any]:
        """
        从 FastAPI 请求中获取 JSON body
        
        Args:
            fastapi_request: FastAPI Request 对象
        
        Returns:
            JSON body 字典
        """
        try:
            body = await fastapi_request.json()
            return body if isinstance(body, dict) else {}
        except Exception:
            return {}
    
    def get_query_params(self, fastapi_request: Request) -> Dict[str, Any]:
        """
        从请求中获取查询参数
        
        Args:
            fastapi_request: FastAPI Request 对象
        
        Returns:
            查询参数字典
        """
        return dict(fastapi_request.query_params)


class FastAPIExportHandlers:
    """
    FastAPI 导出接口处理器工厂
    
    用于创建符合 FastAPI 风格的处理器函数
    """
    
    def __init__(self, adapter: FastAPIAdapter):
        """
        初始化处理器工厂
        
        Args:
            adapter: FastAPI 适配器实例
        """
        self.adapter = adapter
        self.manager = adapter.manager
    
    async def export_submit(
        self,
        fastapi_request: Request,
        query_id: str,
        fields: list,
        export_format: str = 'csv'
    ):
        """
        提交导出任务处理器
        
        Args:
            fastapi_request: FastAPI Request 对象
            query_id: 查询 ID
            fields: 导出字段
            export_format: 导出格式
        
        Returns:
            任务 ID 和状态
        """
        try:
            # 获取用户 ID
            user_id = self.adapter.get_user_id(fastapi_request)
            
            # 提交任务
            task_id = self.manager.submit_export_task(
                query_id=query_id,
                fields=fields,
                export_format=export_format,
                user_id=user_id
            )
            
            return {
                'success': True,
                'task_id': task_id,
                'status': 'pending'
            }
        
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")
    
    async def export_progress(
        self,
        fastapi_request: Request,
        task_id: str
    ):
        """
        查询导出进度处理器
        
        Args:
            fastapi_request: FastAPI Request 对象
            task_id: 任务 ID
        
        Returns:
            任务进度信息
        """
        progress = self.manager.get_task_progress(task_id)
        
        if progress is None:
            raise HTTPException(status_code=404, detail="Task not found")
        
        return {
            'success': True,
            'data': progress
        }
    
    async def export_download(
        self,
        fastapi_request: Request,
        task_id: str
    ):
        """
        下载导出文件处理器
        
        Args:
            fastapi_request: FastAPI Request 对象
            task_id: 任务 ID
        
        Returns:
            文件下载响应
        """
        result = self.manager.get_export_file(task_id)
        
        if result is None:
            raise HTTPException(status_code=404, detail="File not found or export not completed")
        
        file_path, filename, mime_type = result
        
        return self.adapter.get_file_response(file_path, filename, mime_type)
    
    async def export_cancel(
        self,
        fastapi_request: Request,
        task_id: str
    ):
        """
        取消导出任务处理器
        
        Args:
            fastapi_request: FastAPI Request 对象
            task_id: 任务 ID
        
        Returns:
            取消结果
        """
        success = self.manager.cancel_task(task_id)
        
        if not success:
            raise HTTPException(status_code=404, detail="Task not found")
        
        return {
            'success': True,
            'message': 'Task cancelled'
        }
    
    async def user_tasks(
        self,
        fastapi_request: Request,
        status: str = None,
        limit: int = 10
    ):
        """
        查询用户任务列表处理器
        
        Args:
            fastapi_request: FastAPI Request 对象
            status: 任务状态过滤
            limit: 返回数量限制
        
        Returns:
            任务列表
        """
        user_id = self.adapter.get_user_id(fastapi_request)
        
        if not user_id:
            raise HTTPException(status_code=401, detail="User not authenticated")
        
        tasks = self.manager.get_user_tasks(user_id, status, limit)
        
        return {
            'success': True,
            'data': tasks
        }
