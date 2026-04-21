"""
JSON 格式化器
"""

import json
from typing import List, Dict, Any
from .base import BaseFormatter


class JSONFormatter(BaseFormatter):
    """
    JSON 格式化器
    
    功能：
    1. 写入 JSON Lines 格式（每行一个 JSON 对象）
    2. 内存友好，支持流式写入
    3. 支持中文
    """
    
    def __init__(self, fields: List[str], pretty: bool = False):
        """
        初始化 JSON 格式化器
        
        Args:
            fields: 导出字段列表
            pretty: 是否格式化（美化输出）
        """
        super().__init__(fields)
        self.pretty = pretty
        self.is_first_row = True
    
    def open(self, file_path: str):
        """
        打开 JSON 文件
        
        Args:
            file_path: 文件路径
        """
        self.file_path = file_path
        self.file_handle = open(file_path, 'w', encoding='utf-8')
        
        if self.pretty:
            # 格式化模式：写入数组开始
            self.file_handle.write('[\n')
        
        self.is_first_row = True
        self.row_count = 0
    
    def write_batch(self, data: List[Dict[str, Any]]):
        """
        写入一批数据
        
        Args:
            data: 数据列表
        """
        if not self.file_handle:
            raise RuntimeError("File not opened. Call open() first.")
        
        for row_data in data:
            # 提取指定字段
            extracted = {}
            for field in self.fields:
                value = row_data
                for key in field.split('.'):
                    if isinstance(value, dict):
                        value = value.get(key)
                    else:
                        value = None
                        break
                extracted[field] = value
            
            if self.pretty:
                # 格式化模式
                if not self.is_first_row:
                    self.file_handle.write(',\n')
                
                json_str = json.dumps(
                    extracted,
                    ensure_ascii=False,
                    indent=2
                )
                # 缩进
                indented = '\n'.join('  ' + line for line in json_str.split('\n'))
                self.file_handle.write(indented)
                self.is_first_row = False
            else:
                # JSON Lines 模式：每行一个 JSON 对象
                self.file_handle.write(
                    json.dumps(extracted, ensure_ascii=False) + '\n'
                )
            
            self.row_count += 1
    
    def close(self):
        """关闭文件"""
        if self.file_handle:
            if self.pretty:
                # 格式化模式：写入数组结束
                self.file_handle.write('\n]')
            
            self.file_handle.close()
            self.file_handle = None
    
    def get_stats(self) -> dict:
        """
        获取统计信息
        
        Returns:
            统计数据
        """
        return {
            'file_path': self.file_path,
            'row_count': self.row_count,
            'field_count': len(self.fields),
            'format': 'json_lines' if not self.pretty else 'json_array'
        }
