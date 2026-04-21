"""
Django 完整示例
演示如何使用 redis-cache-export 插件
"""

import json
from django.http import JsonResponse, FileResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.conf import settings

# 配置 Django
if not settings.configured:
    settings.configure(
        DEBUG=True,
        SECRET_KEY='your-secret-key',
        INSTALLED_APPS=[
            'django.contrib.contenttypes',
            'django.contrib.auth',
        ],
        DATABASES={
            'default': {
                'ENGINE': 'django.db.backends.sqlite3',
                'NAME': ':memory:',
            }
        }
    )

# 导入插件
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from redis_cache_export import setup_manager
from redis_cache_export.adapters import DjangoAdapter

# 初始化
manager = setup_manager(
    redis_url='redis://localhost:6379/0',
    storage_path='/tmp/exports'
)
adapter = DjangoAdapter(manager)


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


# ==================== 视图函数 ====================

@csrf_exempt
@require_http_methods(["GET"])
def get_devices(request):
    """
    查询设备列表
    
    Query Parameters:
        type: 设备类型
        status: 设备状态
        location: 区域
        page: 页码（默认1）
        size: 每页大小（默认100）
    """
    # 获取请求参数
    params = adapter.get_query_params(request)
    
    # 提取分页参数
    page = int(params.get('page', [1])[0] if isinstance(params.get('page'), list) else params.get('page', 1))
    size = int(params.get('size', [100])[0] if isinstance(params.get('size'), list) else params.get('size', 100))
    
    # 过滤条件
    filter_params = {}
    for key in ['type', 'status', 'location']:
        if key in params:
            val = params[key]
            filter_params[key] = val[0] if isinstance(val, list) else val
    
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
    
    return JsonResponse({
        'success': True,
        'data': data,
        'total': total,
        'page': page,
        'size': size,
        'query_id': query_id
    })


@csrf_exempt
@require_http_methods(["POST"])
def export_devices(request):
    """
    提交导出任务
    
    Request Body:
        {
            "query_id": "abc123",
            "fields": ["id", "name", "type", "status"],
            "format": "csv"
        }
    """
    try:
        data = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({'success': False, 'error': 'Invalid JSON'}, status=400)
    
    query_id = data.get('query_id')
    fields = data.get('fields', ['id', 'name', 'type', 'status', 'location', 'created_at'])
    export_format = data.get('format', 'csv')
    
    if not query_id:
        return JsonResponse({'success': False, 'error': 'query_id is required'}, status=400)
    
    # 检查 query_id
    query_data = manager.get_query(query_id)
    if not query_data:
        return JsonResponse({'success': False, 'error': 'Invalid or expired query_id'}, status=400)
    
    # 提交任务
    task_id = manager.submit_export_task(
        query_id=query_id,
        fields=fields,
        export_format=export_format,
        user_id=adapter.get_user_id(request)
    )
    
    # 后台执行
    import threading
    
    def run_export():
        manager.execute_task_sync(
            task_id=task_id,
            query_func=lambda p, o, l: query_devices(p, o, -1)
        )
    
    thread = threading.Thread(target=run_export)
    thread.start()
    
    return JsonResponse({
        'success': True,
        'task_id': task_id,
        'message': 'Export task submitted'
    })


@csrf_exempt
@require_http_methods(["GET"])
def export_progress(request):
    """查询导出进度"""
    task_id = request.GET.get('task_id')
    
    if not task_id:
        return JsonResponse({'success': False, 'error': 'task_id is required'}, status=400)
    
    progress = manager.get_task_progress(task_id)
    
    if not progress:
        return JsonResponse({'success': False, 'error': 'Task not found'}, status=404)
    
    return JsonResponse({
        'success': True,
        'data': progress
    })


@csrf_exempt
@require_http_methods(["GET"])
def export_download(request):
    """下载导出文件"""
    task_id = request.GET.get('task_id')
    
    if not task_id:
        return JsonResponse({'success': False, 'error': 'task_id is required'}, status=400)
    
    result = manager.get_export_file(task_id)
    
    if not result:
        return JsonResponse({'success': False, 'error': 'File not found'}, status=404)
    
    file_path, filename, mime_type = result
    
    response = FileResponse(
        open(file_path, 'rb'),
        content_type=mime_type
    )
    response['Content-Disposition'] = f'attachment; filename="{filename}"'
    
    return response


@csrf_exempt
@require_http_methods(["POST"])
def export_cancel(request):
    """取消导出任务"""
    task_id = request.GET.get('task_id')
    
    if not task_id:
        return JsonResponse({'success': False, 'error': 'task_id is required'}, status=400)
    
    success = manager.cancel_task(task_id)
    
    if not success:
        return JsonResponse({'success': False, 'error': 'Task not found'}, status=404)
    
    return JsonResponse({
        'success': True,
        'message': 'Task cancelled'
    })


# ==================== URL 配置 ====================

from django.urls import path

urlpatterns = [
    path('api/devices', get_devices),
    path('api/devices/export', export_devices),
    path('api/devices/export/progress', export_progress),
    path('api/devices/export/download', export_download),
    path('api/devices/export/cancel', export_cancel),
]


# ==================== 主程序 ====================

if __name__ == '__main__':
    from django.core.management import execute_from_command_line
    
    print("\n" + "=" * 50)
    print("Django Export Example")
    print("=" * 50)
    print("\nAPI Endpoints:")
    print("  GET  /api/devices              - 查询设备列表")
    print("  POST /api/devices/export       - 提交导出任务")
    print("  GET  /api/devices/export/progress?task_id=xxx - 查询进度")
    print("  GET  /api/devices/export/download?task_id=xxx - 下载文件")
    print("  POST /api/devices/export/cancel?task_id=xxx   - 取消任务")
    print("\nStarting server on http://localhost:8000 ...\n")
    
    execute_from_command_line(['manage.py', 'runserver', '0.0.0.0:8000'])
