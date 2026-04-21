"""
FastAPI 完整示例
演示如何使用 redis-cache-export 插件
"""

from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel, Field
from typing import List, Optional
import uvicorn

# 导入插件
import sys
sys.path.insert(0, '..')

from redis_cache_export import setup_manager
from redis_cache_export.adapters import FastAPIAdapter, FastAPIExportHandlers

app = FastAPI(
    title="Device Export API",
    description="设备查询与导出 API",
    version="1.0.0"
)

# ==================== 初始化 ====================

manager = setup_manager(
    redis_url='redis://localhost:6379/0',
    storage_path='/tmp/exports'
)

adapter = FastAPIAdapter(manager)
handlers = FastAPIExportHandlers(adapter)


# ==================== 数据模型 ====================

class Device(BaseModel):
    """设备模型"""
    id: int
    name: str
    type: str
    status: str
    location: str
    created_at: str


class DeviceListResponse(BaseModel):
    """设备列表响应"""
    success: bool = True
    data: List[dict]
    total: int
    page: int
    size: int
    query_id: str


class ExportRequest(BaseModel):
    """导出请求"""
    query_id: str
    fields: List[str] = Field(default=['id', 'name', 'type', 'status'])
    format: str = Field(default='csv', description="csv, excel, json")


class TaskResponse(BaseModel):
    """任务响应"""
    success: bool = True
    task_id: str
    message: Optional[str] = None


class ProgressResponse(BaseModel):
    """进度响应"""
    success: bool = True
    data: dict


# ==================== 模拟数据 ====================

MOCK_DEVICES = []

def init_mock_data():
    """初始化模拟数据"""
    import random
    device_types = ['家电', '工业', '医疗', '农业', '交通']
    statuses = ['在线', '离线', '维护中', '故障']
    
    for i in range(10000):
        MOCK_DEVICES.append({
            'id': i + 1,
            'name': f'设备_{i + 1}',
            'type': random.choice(device_types),
            'status': random.choice(statuses),
            'location': f'区域_{random.randint(1, 10)}',
            'created_at': f'2024-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}'
        })

init_mock_data()


# ==================== 查询函数 ====================

def query_devices(params: dict, offset: int = 0, limit: int = 100) -> list:
    """查询设备数据"""
    results = MOCK_DEVICES.copy()
    
    if params.get('type'):
        results = [d for d in results if d['type'] == params['type']]
    
    if params.get('status'):
        results = [d for d in results if d['status'] == params['status']]
    
    if params.get('location'):
        results = [d for d in results if d['location'] == params['location']]
    
    if limit > 0:
        return results[offset:offset + limit]
    return results[offset:]


def count_devices(params: dict) -> int:
    """统计设备数量"""
    results = MOCK_DEVICES.copy()
    
    if params.get('type'):
        results = [d for d in results if d['type'] == params['type']]
    
    if params.get('status'):
        results = [d for d in results if d['status'] == params['status']]
    
    if params.get('location'):
        results = [d for d in results if d['location'] == params['location']]
    
    return len(results)


# ==================== API 路由 ====================

@app.get("/api/devices", response_model=DeviceListResponse)
async def get_devices(
    request: Request,
    type: Optional[str] = None,
    status: Optional[str] = None,
    location: Optional[str] = None,
    page: int = Query(1, ge=1),
    size: int = Query(100, ge=1, le=1000)
):
    """
    查询设备列表
    
    - **type**: 设备类型（可选）
    - **status**: 设备状态（可选）
    - **location**: 区域（可选）
    - **page**: 页码（默认1）
    - **size**: 每页大小（默认100）
    """
    # 构建过滤条件
    filter_params = {}
    if type:
        filter_params['type'] = type
    if status:
        filter_params['status'] = status
    if location:
        filter_params['location'] = location
    
    # 缓存查询条件
    query_id = manager.cache_query(
        func_name='get_devices',
        params=filter_params,
        user_id=adapter.get_user_id(request)
    )
    
    # 查询总数
    total = count_devices(filter_params)
    manager.query_cache.update_query(query_id, {'total': total})
    
    # 查询数据
    offset = (page - 1) * size
    data = query_devices(filter_params, offset, size)
    
    return {
        'success': True,
        'data': data,
        'total': total,
        'page': page,
        'size': size,
        'query_id': query_id
    }


@app.post("/api/devices/export", response_model=TaskResponse)
async def export_devices(request: Request, body: ExportRequest):
    """
    提交导出任务

    - **query_id**: 查询ID（必需）
    - **fields**: 导出字段列表
    - **format**: 导出格式（csv/excel/json）
    """
    # 检查 query_id
    query_data = manager.get_query(body.query_id)
    if not query_data:
        raise HTTPException(status_code=400, detail="Invalid or expired query_id")
    
    # 提交任务
    task_id = manager.submit_export_task(
        query_id=body.query_id,
        fields=body.fields,
        export_format=body.format,
        user_id=adapter.get_user_id(request)
    )
    
    # 后台执行（生产环境建议用 Celery）
    import asyncio
    from concurrent.futures import ThreadPoolExecutor
    executor = ThreadPoolExecutor(max_workers=1)
    
    def run_export():
        manager.execute_task_sync(
            task_id=task_id,
            query_func=lambda p, o, l: query_devices(p, o, -1)
        )
    
    loop = asyncio.get_event_loop()
    loop.run_in_executor(executor, run_export)
    
    return {
        'success': True,
        'task_id': task_id,
        'message': 'Export task submitted'
    }


@app.get("/api/devices/export/progress", response_model=ProgressResponse)
async def export_progress(task_id: str = Query(..., description="任务ID")):
    """
    查询导出进度

    - **task_id**: 任务ID
    """
    progress = manager.get_task_progress(task_id)
    
    if not progress:
        raise HTTPException(status_code=404, detail="Task not found")
    
    return {
        'success': True,
        'data': progress
    }


@app.get("/api/devices/export/download")
async def export_download(task_id: str = Query(..., description="任务ID")):
    """
    下载导出文件

    - **task_id**: 任务ID
    """
    result = manager.get_export_file(task_id)
    
    if not result:
        raise HTTPException(status_code=404, detail="File not found or export not completed")
    
    file_path, filename, mime_type = result
    
    return FileResponse(
        path=file_path,
        filename=filename,
        media_type=mime_type
    )


@app.post("/api/devices/export/cancel")
async def export_cancel(task_id: str = Query(..., description="任务ID")):
    """
    取消导出任务

    - **task_id**: 任务ID
    """
    success = manager.cancel_task(task_id)
    
    if not success:
        raise HTTPException(status_code=404, detail="Task not found or already completed")
    
    return {'success': True, 'message': 'Task cancelled'}


# ==================== 主程序 ====================

if __name__ == "__main__":
    print("\n" + "=" * 50)
    print("FastAPI Export Example")
    print("=" * 50)
    print("\nAPI Documentation:")
    print("  http://localhost:8000/docs")
    print("\nStarting server...\n")
    
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
