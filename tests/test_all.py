"""
单元测试
"""

import unittest
import tempfile
import os
import sys

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 使用 fakeredis 进行测试（无需真实 Redis）
try:
    import fakeredis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    print("Warning: fakeredis not installed, some tests will be skipped")


@unittest.skipIf(not REDIS_AVAILABLE, "fakeredis not installed")
class TestQueryCache(unittest.TestCase):
    """测试查询缓存"""
    
    def setUp(self):
        """测试前准备"""
        self.redis_client = fakeredis.FakeRedis()
        from redis_cache_export.core import QueryCache
        self.cache = QueryCache(self.redis_client)
    
    def test_cache_query(self):
        """测试缓存查询条件"""
        query_id = self.cache.cache_query(
            func_name='get_devices',
            params={'type': '家电', 'page': 1},
            exclude_params={'page'}
        )
        
        self.assertIsNotNone(query_id)
        self.assertEqual(len(query_id), 16)
    
    def test_get_query(self):
        """测试获取缓存的查询条件"""
        query_id = self.cache.cache_query(
            func_name='get_devices',
            params={'type': '家电'},
            user_id='user_001'
        )
        
        query_data = self.cache.get_query(query_id)
        
        self.assertIsNotNone(query_data)
        self.assertEqual(query_data['func_name'], 'get_devices')
        self.assertEqual(query_data['params'], {'type': '家电'})
        self.assertEqual(query_data['user_id'], 'user_001')
    
    def test_delete_query(self):
        """测试删除缓存"""
        query_id = self.cache.cache_query(
            func_name='get_devices',
            params={'type': '家电'}
        )
        
        self.assertTrue(self.cache.exists(query_id))
        self.assertTrue(self.cache.delete_query(query_id))
        self.assertFalse(self.cache.exists(query_id))


@unittest.skipIf(not REDIS_AVAILABLE, "fakeredis not installed")
class TestProgressTracker(unittest.TestCase):
    """测试进度追踪"""
    
    def setUp(self):
        """测试前准备"""
        self.redis_client = fakeredis.FakeRedis()
        from redis_cache_export.core import ProgressTracker
        self.tracker = ProgressTracker(self.redis_client)
    
    def test_init_progress(self):
        """测试初始化进度"""
        progress = self.tracker.init_progress(
            task_id='task_001',
            total=1000,
            query_id='query_001',
            export_format='csv',
            fields=['id', 'name']
        )
        
        self.assertEqual(progress['task_id'], 'task_001')
        self.assertEqual(progress['total'], 1000)
        self.assertEqual(progress['status'], 'pending')
    
    def test_update_progress(self):
        """测试更新进度"""
        self.tracker.init_progress(
            task_id='task_001',
            total=1000,
            query_id='query_001',
            export_format='csv',
            fields=['id']
        )
        
        updated = self.tracker.update_progress(
            task_id='task_001',
            processed=500,
            status='processing'
        )
        
        self.assertEqual(updated['processed'], 500)
        self.assertEqual(updated['status'], 'processing')


class TestFormatters(unittest.TestCase):
    """测试格式化器"""
    
    def setUp(self):
        """测试前准备"""
        self.test_data = [
            {'id': 1, 'name': '设备1', 'type': '家电'},
            {'id': 2, 'name': '设备2', 'type': '工业'},
        ]
        self.fields = ['id', 'name', 'type']
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """测试后清理"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_csv_formatter(self):
        """测试 CSV 格式化器"""
        from redis_cache_export.formatters import CSVFormatter
        
        file_path = os.path.join(self.temp_dir, 'test.csv')
        
        formatter = CSVFormatter(self.fields)
        formatter.open(file_path)
        formatter.write_batch(self.test_data)
        formatter.close()
        
        self.assertTrue(os.path.exists(file_path))
    
    def test_json_formatter(self):
        """测试 JSON 格式化器"""
        from redis_cache_export.formatters import JSONFormatter
        
        file_path = os.path.join(self.temp_dir, 'test.json')
        
        formatter = JSONFormatter(self.fields)
        formatter.open(file_path)
        formatter.write_batch(self.test_data)
        formatter.close()
        
        self.assertTrue(os.path.exists(file_path))


if __name__ == '__main__':
    unittest.main(verbosity=2)
