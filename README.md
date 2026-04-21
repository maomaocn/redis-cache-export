# Redis Cache Export

一个通用的查询缓存与导出插件，支持 Flask、FastAPI、Django 等框架。

## 特性

- ✅ **缓存查询条件**：不缓存数据，只缓存查询条件，内存消耗极小
- ✅ **异步导出**：支持大数据量导出，不阻塞主线程
- ✅ **进度追踪**：实时查询导出进度
- ✅ **多格式支持**：CSV、Excel、JSON
- ✅ **框架兼容**：支持 Flask、FastAPI、Django
- ✅ **流式处理**：分批查询，避免 OOM

## 安装

```bash
pip install redis
pip install openpyxl  # Excel 导出需要
```

## 快速开始

### 1. 初始化管理器

```python
from redis_cache_export import setup_manager

# 创建管理器
manager = setup_manager(
    redis_url='redis://localhost:6379/0',
    storage_path='/tmp/exports',
    batch_size=1000
)
```

### 2. 定义查询函数

```python
from redis_cache_export.decorators import cache_query

@cache_query(manager=manager)
def get_devices(**params):
    """查询设备列表"""
    # 构建查询
    query = Device.query
    
    if params.get('type'):
        query = query.filter_by(type=params['type'])
    
    if params.get('status'):
        query = query.filter_by(status=params['status'])
    
    # 分页
    page = params.get('page', 1)
    size = params.get('size', 100)
    
    total = query.count()
    items = query.offset((page - 1) * size).limit(size).all()
    
    return {
        'data': items,
        'total': total,
    }
```

### 3. 用户搜索

```python
# 用户请求：GET /devices?type=家电&page=1&size=100
result = get_devices(type='家电', page=1, size=100)

# 返回：
# {
#     'data': [...],           # 数据列表
#     'total': 10000,         # 总数
#     'query_id': 'abc123',    # 缓存的查询ID
# }
```

### 4. 用户点击导出

```python
# 提交导出任务
task_id = manager.submit_export_task(
    query_id='abc123',
    fields=['id', 'name', 'type', 'status', 'created_at'],
    export_format='csv',
    user_id='user_001'
)

# 返回：task_id = 'task_456'
```

### 5. 查询进度

```python
progress = manager.get_task_progress('task_456')

# 返回：
# {
#     'status': 'processing',
#     'progress': 35,
#     'processed': 3500,
#     'total': 10000,
#     'estimated_remaining_seconds': 30
# }
```

### 6. 下载文件

```python
result = manager.get_export_file('task_456')

if result:
    file_path, filename, mime_type = result
    # 返回文件给用户
```

## 架构

```
用户操作流程：

1. 用户搜索 → 缓存查询条件 → 返回 query_id
   GET /devices?type=家电&page=1
   → 返回：{ data: [...], query_id: "abc123" }

2. 用户点击导出 → 提交任务 → 返回 task_id
   POST /devices/export
   → 返回：{ task_id: "task_456" }

3. 用户查询进度 → 返回进度
   GET /devices/export/progress?task_id=task_456
   → 返回：{ progress: 35%, processed: 3500, total: 10000 }

4. 导出完成 → 下载文件
   GET /devices/export/download?task_id=task_456
   → 返回文件流
```

## 框架集成

### Flask

```python
from flask import Flask, request
from redis_cache_export import setup_manager, FlaskAdapter

app = Flask(__name__)
manager = setup_manager()
adapter = FlaskAdapter(manager)

@app.route('/devices')
def devices():
    # 缓存查询条件
    params = adapter.get_query_params(request)
    query_id = manager.cache_query('get_devices', params)
    
    # 执行查询
    result = get_devices(**params)
    result['query_id'] = query_id
    
    return adapter.json_response(result)

@app.route('/devices/export', methods=['POST'])
def export():
    data = adapter.get_json_body(request)
    query_id = data.get('query_id')
    fields = data.get('fields', [])
    export_format = data.get('format', 'csv')
    
    task_id = manager.submit_export_task(query_id, fields, export_format)
    
    return adapter.json_response({'task_id': task_id})

@app.route('/devices/export/progress')
def export_progress():
    task_id = request.args.get('task_id')
    progress = manager.get_task_progress(task_id)
    
    return adapter.json_response(progress)

@app.route('/devices/export/download')
def export_download():
    task_id = request.args.get('task_id')
    result = manager.get_export_file(task_id)
    
    if result:
        return adapter.get_file_response(*result)
    
    return adapter.error_response('File not found', 404)
```

### FastAPI

```python
from fastapi import FastAPI, Request
from pydantic import BaseModel
from redis_cache_export import setup_manager, FastAPIAdapter, FastAPIExportHandlers

app = FastAPI()
manager = setup_manager()
adapter = FastAPIAdapter(manager)
handlers = FastAPIExportHandlers(adapter)

class ExportRequest(BaseModel):
    query_id: str
    fields: list
    format: str = 'csv'

@app.get('/devices')
async def devices(request: Request):
    params = adapter.get_query_params(request)
    query_id = manager.cache_query('get_devices', dict(params))
    
    result = get_devices(**params)
    result['query_id'] = query_id
    
    return result

@app.post('/devices/export')
async def export(request: Request, body: ExportRequest):
    return await handlers.export_submit(
        request,
        query_id=body.query_id,
        fields=body.fields,
        export_format=body.format
    )

@app.get('/devices/export/progress')
async def export_progress(request: Request, task_id: str):
    return await handlers.export_progress(request, task_id)

@app.get('/devices/export/download')
async def export_download(request: Request, task_id: str):
    return await handlers.export_download(request, task_id)
```

## 异步导出

对于大数据量导出，建议使用后台任务队列：

```python
# 使用 Celery
from celery import Celery
from redis_cache_export import CacheExportManager

celery_app = Celery('tasks', broker='redis://localhost:6379/1')

@celery_app.task
def process_export(task_id: str, query_func, total_func):
    """后台处理导出任务"""
    manager = CacheExportManager(redis_url='redis://localhost:6379/0')
    manager.execute_task_sync(task_id, query_func, total_func)

# 提交任务时
task_id = manager.submit_export_task(...)
process_export.delay(task_id, query_func, total_func)
```

## 配置项

| 参数 | 说明 | 默认值 |
|------|------|--------|
| redis_url | Redis 连接 URL | redis://localhost:6379/0 |
| storage_path | 文件存储路径 | /tmp/exports |
| batch_size | 分批查询大小 | 1000 |
| query_ttl | 查询缓存时间 | 3600 (1小时) |
| progress_ttl | 进度保存时间 | 86400 (1天) |

## API 参考

### CacheExportManager

#### cache_query()
缓存查询条件。

```python
query_id = manager.cache_query(
    func_name='get_devices',
    params={'type': '家电', 'status': '在线'},
    user_id='user_001'
)
```

#### submit_export_task()
提交导出任务。

```python
task_id = manager.submit_export_task(
    query_id='abc123',
    fields=['id', 'name', 'status'],
    export_format='csv'
)
```

#### get_task_progress()
查询任务进度。

```python
progress = manager.get_task_progress('task_456')
# {
#     'status': 'processing',
#     'progress': 35,
#     'processed': 3500,
#     'total': 10000,
#     'estimated_remaining_seconds': 30
# }
```

#### get_export_file()
获取导出文件。

```python
file_path, filename, mime_type = manager.get_export_file('task_456')
```

## License

MIT
