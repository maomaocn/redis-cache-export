"""
CSV 格式化器
"""

import csv
from typing import List, Dict, Any
from .base import BaseFormatter


class CSVFormatter(BaseFormatter):
    """
    CSV 格式化器
    
    功能：
    1. 写入 CSV 文件
    2. 支持中文
    3. 分批写入，内存友好
    """
    
    def __init__(self, fields: List[str], encoding: str = 'utf-8-sig'):
        """
        初始化 CSV 格式化器
        
        Args:
            fields: 导出字段列表
            encoding: 文件编码，默认 utf-8-sig（支持 Excel 打开）
        """
        super().__init__(fields)
        self.encoding = encoding
        self.writer = None
    
    def open(self, file_path: str):
        """
        打开 CSV 文件
        
        Args:
            file_path: 文件路径
        """
        self.file_path = file_path
        self.file_handle = open(file_path, 'w', encoding=self.encoding, newline='')
        self.writer = csv.writer(self.file_handle)
        
        # 写入表头
        self.writer.writerow(self.fields)
        self.row_count = 0
    
    def write_batch(self, data: List[Dict[str, Any]]):
        """
        写入一批数据
        
        Args:
            data: 数据列表
        """
        if not self.writer:
            raise RuntimeError("File not opened. Call open() first.")
        
        for row in data:
            values = self._extract_values(row)
            # 转换为字符串，处理 None 值
            str_values = [str(v) if v is not None else '' for v in values]
            self.writer.writerow(str_values)
            self.row_count += 1
        
        # 刷新到磁盘
        self.file_handle.flush()
    
    def close(self):
        """关闭文件"""
        if self.file_handle:
            self.file_handle.close()
            self.file_handle = None
            self.writer = None
    
    def get_stats(self) -> dict:
        """
        获取统计信息
        
        Returns:
            统计数据
        """
        return {
            'file_path': self.file_path,
            'row_count': self.row_count,
            'field_count': len(self.fields)
        }
