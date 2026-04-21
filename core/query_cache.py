"""
查询条件缓存模块
负责：缓存查询条件，生成 query_id，管理缓存生命周期
"""

import json
import hashlib
import time
from typing import Any, Dict, Optional
import redis

from ..logger import get_logger
from ..exceptions import (
    QueryCacheError,
    QueryNotFoundError,
    QueryCacheExpiredError,
    InvalidQueryParamsError,
    RedisConnectionError,
    RedisOperationError
)


class QueryCache:
    """
    查询条件缓存管理器
    
    功能：
    1. 缓存查询条件（不含分页参数）
    2. 生成唯一的 query_id
    3. 支持设置 TTL
    4. 支持取回缓存的查询条件
    """
    
    # 默认要排除的参数（分页相关）
    DEFAULT_EXCLUDE_PARAMS = {
        'page', 'size', 'limit', 'offset', 
        'order', 'sort', 'order_by', 'sort_by'
    }
    
    def __init__(self, redis_client: redis.Redis, prefix: str = 'query_cache'):
        """
        初始化查询缓存
        
        Args:
            redis_client: Redis 客户端实例
            prefix: Redis key 前缀
        """
        self.redis = redis_client
        self.prefix = prefix
        self.logger = get_logger('query_cache')
    
    def _generate_query_id(self, func_name: str, params: Dict[str, Any], user_id: Optional[str] = None) -> str:
        """
        根据函数名和参数生成唯一的 query_id
        
        Args:
            func_name: 查询函数名称
            params: 查询参数
            user_id: 用户ID（可选）
        
        Returns:
            唯一的 query_id
        """
        # 构建唯一标识
        unique_str = f"{func_name}:{json.dumps(params, sort_keys=True)}"
        if user_id:
            unique_str = f"{user_id}:{unique_str}"
        
        # 使用 MD5 生成短 ID
        hash_obj = hashlib.md5(unique_str.encode('utf-8'))
        return hash_obj.hexdigest()[:16]
    
    def _get_cache_key(self, query_id: str) -> str:
        """
        获取 Redis 存储的完整 key
        
        Args:
            query_id: 查询ID
        
        Returns:
            完整的 Redis key
        """
        return f"{self.prefix}:{query_id}"
    
    def cache_query(
        self,
        func_name: str,
        params: Dict[str, Any],
        user_id: Optional[str] = None,
        exclude_params: Optional[set] = None,
        ttl: int = 3600,
        extra_data: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        缓存查询条件
        
        Args:
            func_name: 查询函数名称
            params: 完整查询参数
            user_id: 用户ID（可选）
            exclude_params: 需要排除的参数集合
            ttl: 缓存时间（秒），默认1小时
            extra_data: 额外需要缓存的数据
        
        Returns:
            query_id: 查询ID，用于后续导出
        
        Raises:
            InvalidQueryParamsError: 参数无效
            RedisOperationError: Redis 操作失败
        """
        # 参数校验
        if not func_name:
            raise InvalidQueryParamsError("func_name is required")
        
        if not isinstance(params, dict):
            raise InvalidQueryParamsError("params must be a dictionary")
        
        try:
            # 排除分页参数
            exclude = exclude_params or self.DEFAULT_EXCLUDE_PARAMS
            filtered_params = {
                k: v for k, v in params.items() 
                if k not in exclude and v is not None
            }
            
            # 生成 query_id
            query_id = self._generate_query_id(func_name, filtered_params, user_id)
            
            # 构建缓存数据
            cache_data = {
                'query_id': query_id,
                'func_name': func_name,
                'params': filtered_params,
                'user_id': user_id,
                'created_at': time.time(),
                'extra': extra_data or {}
            }
            
            # 存入 Redis
            cache_key = self._get_cache_key(query_id)
            
            self.logger.debug(
                f"Caching query: func={func_name}, query_id={query_id}, ttl={ttl}",
                extra_data={
                    'query_id': query_id,
                    'func_name': func_name,
                    'params_count': len(filtered_params),
                    'ttl': ttl
                }
            )
            
            self.redis.setex(
                cache_key,
                ttl,
                json.dumps(cache_data, ensure_ascii=False)
            )
            
            self.logger.info(
                f"Query cached successfully: {query_id}",
                extra_data={
                    'query_id': query_id,
                    'func_name': func_name,
                    'user_id': user_id
                }
            )
            
            return query_id
            
        except redis.RedisError as e:
            self.logger.error(
                f"Redis error while caching query: {str(e)}",
                extra_data={
                    'func_name': func_name,
                    'exception': str(e)
                }
            )
            raise RedisOperationError(
                operation='setex',
                key=f"{self.prefix}:*",
                reason=str(e)
            )
    
    def get_query(self, query_id: str) -> Optional[Dict[str, Any]]:
        """
        获取缓存的查询条件
        
        Args:
            query_id: 查询ID
        
        Returns:
            缓存的查询数据，如果不存在返回 None
        
        Raises:
            QueryNotFoundError: 查询不存在
            RedisOperationError: Redis 操作失败
        """
        if not query_id:
            raise InvalidQueryParamsError("query_id is required")
        
        try:
            cache_key = self._get_cache_key(query_id)
            data = self.redis.get(cache_key)
            
            if data is None:
                self.logger.warning(
                    f"Query cache not found: {query_id}",
                    extra_data={'query_id': query_id}
                )
                return None
            
            try:
                result = json.loads(data.decode('utf-8'))
                self.logger.debug(
                    f"Query cache retrieved: {query_id}",
                    extra_data={'query_id': query_id}
                )
                return result
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                self.logger.error(
                    f"Failed to decode query cache: {query_id}, error: {str(e)}",
                    extra_data={'query_id': query_id, 'exception': str(e)}
                )
                return None
                
        except redis.RedisError as e:
            self.logger.error(
                f"Redis error while getting query: {str(e)}",
                extra_data={'query_id': query_id, 'exception': str(e)}
            )
            raise RedisOperationError(
                operation='get',
                key=f"{self.prefix}:{query_id}",
                reason=str(e)
            )
    
    def update_query(self, query_id: str, extra_data: Dict[str, Any]) -> bool:
        """
        更新缓存的额外数据
        
        Args:
            query_id: 查询ID
            extra_data: 要更新的额外数据
        
        Returns:
            是否更新成功
        """
        query_data = self.get_query(query_id)
        if query_data is None:
            return False
        
        # 合并额外数据
        query_data['extra'].update(extra_data)
        
        # 获取剩余 TTL
        cache_key = self._get_cache_key(query_id)
        ttl = self.redis.ttl(cache_key)
        
        if ttl <= 0:
            return False
        
        # 更新缓存
        self.redis.setex(
            cache_key,
            ttl,
            json.dumps(query_data, ensure_ascii=False)
        )
        
        return True
    
    def delete_query(self, query_id: str) -> bool:
        """
        删除缓存的查询条件
        
        Args:
            query_id: 查询ID
        
        Returns:
            是否删除成功
        """
        cache_key = self._get_cache_key(query_id)
        return bool(self.redis.delete(cache_key))
    
    def extend_ttl(self, query_id: str, additional_seconds: int) -> bool:
        """
        延长缓存时间
        
        Args:
            query_id: 查询ID
            additional_seconds: 额外的秒数
        
        Returns:
            是否延长成功
        """
        cache_key = self._get_cache_key(query_id)
        ttl = self.redis.ttl(cache_key)
        
        if ttl <= 0:
            return False
        
        query_data = self.get_query(query_id)
        if query_data is None:
            return False
        
        self.redis.setex(
            cache_key,
            ttl + additional_seconds,
            json.dumps(query_data, ensure_ascii=False)
        )
        
        return True
    
    def exists(self, query_id: str) -> bool:
        """
        检查查询条件是否存在
        
        Args:
            query_id: 查询ID
        
        Returns:
            是否存在
        """
        cache_key = self._get_cache_key(query_id)
        return bool(self.redis.exists(cache_key))
