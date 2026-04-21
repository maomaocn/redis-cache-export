"""
Flask 完整示例
演示如何使用 redis-cache-export 插件
"""

from flask import Flask, request, jsonify, send_file
import redis

# 导入插件
import sys
sys.path.insert(0, '..')

from redis_cache_export import setup_manager, FlaskAdapter

app = Flask(__name__)

# ==================== 初始化 ====================

# 创建管理器
manager = setup_manager(
    redis_url='redis://localhost:6379/0',
    storage_path='/tmp/exports',
    batch_size=1000
)

# 创建适配器
adapter = FlaskAdapter(manager)


# ==================== 模拟数据 ====================

# 模拟设备数据
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

def query_devices(params: dict, offset: int = 0, limit: int = 100):
    """
    查询设备数据
    
    Args:
        params: 查询参数
        offset: 偏移量
        limit: 限制数量
    
    Returns:
        设备列表
    """
    results = MOCK_DEVICES.copy()
    
    # 过滤条件
    if params.get('type'):
        results = [d for d in results if d['type'] == params['type']]
    
    if params.get('status'):
        results = [d for d in results if d['status'] == params['status']]
    
    if params.get('location'):
        results = [d for d in results if d['location'] == params['location']]
    
    # 分页
    if limit > 0:
        results = results[offset:offset + limit]
    else:
        results = results[offset:]
    
    return results


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


# ==================== 路由定义 ====================

@app.route('/api/devices', methods=['GET'])
def get_devices():
    """
    查询设备列表
    
    Query Parameters:
        type: 设备类型
        status: 设备状态
        location: 区域
        page: 页码（默认1）
        size: 每页大小（默认100）
    
    Returns:
        {
            "data": [...],
            "total": 10000,
            "page": 1,
            "size": 100,
            "query_id": "abc123"
        }
    """
    # 获取请求参数
    params = adapter.get_query_params(request)
    
    # 提取分页参数
    page = int(params.get('page', 1))
    size = int(params.get('size', 100))
    
    # 过滤条件（不含分页参数）
    filter_params = {k: v for k, v in params.items() if k not in ['page', 'size']}
    
    # 缓存查询条件
    query_id = manager.cache_query(
        func_name='get_devices',
        params=filter_params,
        user_id=adapter.get_user_id(request)
    )
    
    # 查询总数
    total = count_devices(filter_params)
    
    # 更新缓存的 total
    manager.query_cache.update_query(query_id, {'total': total})
    
    # 查询数据
    offset = (page - 1) * size
    data = query_devices(filter_params, offset, size)
    
    return jsonify({
        'success': True,
        'data': data,
        'total': total,
        'page': page,
        'size': size,
        'query_id': query_id
    })


@app.route('/api/devices/export', methods=['POST'])
def export_devices():
    """
    提交导出任务
    
    Request Body:
        {
            "query_id": "abc123",
            "fields": ["id", "name", "type", "status"],
            "format": "csv"  // csv, excel, json
        }
    
    Returns:
        {
            "success": true,
            "task_id": "task_456"
        }
    """
    # 获取请求体
    data = adapter.get_json_body(request)
    
    query_id = data.get('query_id')
    fields = data.get('fields', ['id', 'name', 'type', 'status', 'location', 'created_at'])
    export_format = data.get('format', 'csv')
    
    if not query_id:
        return jsonify({'success': False, 'error': 'query_id is required'}), 400
    
    # 检查 query_id 是否有效
    query_data = manager.get_query(query_id)
    if not query_data:
        return jsonify({'success': False, 'error': 'Invalid or expired query_id'}), 400
    
    try:
        # 提交导出任务
        task_id = manager.submit_export_task(
            query_id=query_id,
            fields=fields,
            export_format=export_format,
            user_id=adapter.get_user_id(request)
        )
        
        # 在后台执行导出（这里使用同步执行，生产环境建议用 Celery）
        import threading
        
        def run_export():
            manager.execute_task_sync(
                task_id=task_id,
                query_func=lambda p, o, l: query_devices(p, o, l) if l > 0 else query_devices(p, o, -1)
            )
        
        thread = threading.Thread(target=run_export)
        thread.start()
        
        return jsonify({
            'success': True,
            'task_id': task_id,
            'message': 'Export task submitted'
        })
    
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/devices/export/progress', methods=['GET'])
def export_progress():
    """
    查询导出进度
    
    Query Parameters:
        task_id: 任务ID
    
    Returns:
        {
            "success": true,
            "data": {
                "task_id": "task_456",
                "status": "processing",
                "progress": 35,
                "processed": 3500,
                "total": 10000,
                "estimated_remaining_seconds": 30
            }
        }
    """
    task_id = request.args.get('task_id')
    
    if not task_id:
        return jsonify({'success': False, 'error': 'task_id is required'}), 400
    
    progress = manager.get_task_progress(task_id)
    
    if not progress:
        return jsonify({'success': False, 'error': 'Task not found'}), 404
    
    return jsonify({
        'success': True,
        'data': progress
    })


@app.route('/api/devices/export/download', methods=['GET'])
def export_download():
    """
    下载导出文件
    
    Query Parameters:
        task_id: 任务ID
    
    Returns:
        文件流
    """
    task_id = request.args.get('task_id')
    
    if not task_id:
        return jsonify({'success': False, 'error': 'task_id is required'}), 400
    
    result = manager.get_export_file(task_id)
    
    if not result:
        return jsonify({'success': False, 'error': 'File not found or export not completed'}), 404
    
    file_path, filename, mime_type = result
    
    return send_file(
        file_path,
        mimetype=mime_type,
        as_attachment=True,
        download_name=filename
    )


@app.route('/api/devices/export/cancel', methods=['POST'])
def export_cancel():
    """
    取消导出任务
    
    Query Parameters:
        task_id: 任务ID
    
    Returns:
        {
            "success": true,
            "message": "Task cancelled"
        }
    """
    task_id = request.args.get('task_id')
    
    if not task_id:
        return jsonify({'success': False, 'error': 'task_id is required'}), 400
    
    success = manager.cancel_task(task_id)
    
    if not success:
        return jsonify({'success': False, 'error': 'Task not found or already completed'}), 404
    
    return jsonify({
        'success': True,
        'message': 'Task cancelled'
    })


# ==================== 主程序 ====================

if __name__ == '__main__':
    print("=" * 50)
    print("Flask Export Example")
    print("=" * 50)
    print("\nAPI Endpoints:")
    print("  GET  /api/devices              - 查询设备列表")
    print("  POST /api/devices/export       - 提交导出任务")
    print("  GET  /api/devices/export/progress?task_id=xxx - 查询进度")
    print("  GET  /api/devices/export/download?task_id=xxx - 下载文件")
    print("  POST /api/devices/export/cancel?task_id=xxx   - 取消任务")
    print("\nStarting server...\n")
    
    app.run(host='0.0.0.0', port=5000, debug=True)
