"""
格式化器基类
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any


class BaseFormatter(ABC):
    """
    格式化器基类
    
    所有格式化器（CSV, Excel, JSON）都需要继承此类
    """
    
    def __init__(self, fields: List[str]):
        """
        初始化格式化器
        
        Args:
            fields: 导出字段列表
        """
        self.fields = fields
        self.file_path = None
        self.file_handle = None
        self.row_count = 0
    
    @abstractmethod
    def open(self, file_path: str):
        """
        打开/创建文件
        
        Args:
            file_path: 文件路径
        """
        pass
    
    @abstractmethod
    def write_batch(self, data: List[Dict[str, Any]]):
        """
        写入一批数据
        
        Args:
            data: 数据列表，每个元素是一个字典
        """
        pass
    
    @abstractmethod
    def close(self):
        """关闭文件"""
        pass
    
    def _extract_values(self, row: Dict[str, Any]) -> List[Any]:
        """
        从行数据中提取指定字段的值
        
        Args:
            row: 行数据字典
        
        Returns:
            值列表
        """
        values = []
        for field in self.fields:
            # 支持嵌套字段，如 'user.name'
            value = row
            for key in field.split('.'):
                if isinstance(value, dict):
                    value = value.get(key)
                else:
                    value = None
                    break
            values.append(value)
        return values
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口，确保文件关闭"""
        self.close()
        return False
