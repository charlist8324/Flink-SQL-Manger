import json
import requests
import time
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class DorisStreamLoader:
    """Doris Stream Load 写入器，支持攒批写入和自动建表"""
    
    def __init__(
        self,
        fe_host: str,
        fe_port: int,
        database: str,
        username: str,
        password: str,
        batch_size: int = 1000,
        flush_interval: int = 10,
        label_prefix: str = "flink_manager"
    ):
        """
        初始化Doris Stream Load写入器
        
        :param fe_host: Doris FE节点IP
        :param fe_port: Doris FE HTTP端口（默认8030）
        :param database: 数据库名称
        :param username: 用户名
        :param password: 密码
        :param batch_size: 批次大小，达到该数量自动flush
        :param flush_interval: 自动flush间隔（秒）
        :param label_prefix: 导入任务label前缀
        """
        self.fe_host = fe_host
        self.fe_port = fe_port
        self.database = database
        self.username = username
        self.password = password
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.label_prefix = label_prefix
        
        # 缓存数据
        self._buffer: List[Dict[str, Any]] = []
        self._last_flush_time = time.time()
        self._session = requests.Session()
        self._session.auth = (username, password)
    
    def add_record(self, record: Dict[str, Any]) -> None:
        """添加单条记录到缓冲区"""
        self._buffer.append(record)
        
        # 检查是否需要flush
        if len(self._buffer) >= self.batch_size:
            self.flush()
        elif time.time() - self._last_flush_time >= self.flush_interval:
            self.flush()
    
    def add_batch(self, records: List[Dict[str, Any]]) -> None:
        """批量添加记录到缓冲区"""
        self._buffer.extend(records)
        
        if len(self._buffer) >= self.batch_size:
            self.flush()
    
    def flush(self, table_name: str, format: str = "json") -> Dict[str, Any]:
        """
        将缓冲区数据写入Doris
        
        :param table_name: 目标表名
        :param format: 数据格式，默认为json
        :return: Stream Load响应结果
        """
        if not self._buffer:
            return {"status": "success", "message": "No data to flush"}
        
        try:
            # 构造Stream Load URL
            url = f"http://{self.fe_host}:{self.fe_port}/api/{self.database}/{table_name}/_stream_load"
            
            # 生成唯一label
            label = f"{self.label_prefix}_{table_name}_{int(time.time() * 1000)}"
            
            # 构造数据
            data = "\n".join(json.dumps(record, ensure_ascii=False) for record in self._buffer)
            
            # 请求头
            headers = {
                "label": label,
                "format": format,
                "strip_outer_array": "false",
                "read_json_by_line": "true",
                "max_filter_ratio": "0.1",
                "timeout": "300"
            }
            
            logger.info(f"开始Stream Load写入, 表: {table_name}, 数据量: {len(self._buffer)}条, label: {label}")
            
            # 发送请求
            response = self._session.put(
                url,
                headers=headers,
                data=data.encode("utf-8"),
                timeout=300
            )
            
            response.raise_for_status()
            result = response.json()
            
            if result.get("Status") == "Success":
                logger.info(f"Stream Load成功, 表: {table_name}, 已加载: {result.get('LoadedRows', 0)}条, 标签: {result.get('Label')}")
                # 清空缓冲区
                self._buffer.clear()
                self._last_flush_time = time.time()
                return {"status": "success", "data": result}
            else:
                error_msg = f"Stream Load失败, 状态: {result.get('Status')}, 错误: {result.get('Message')}"
                logger.error(error_msg)
                return {"status": "error", "message": error_msg, "data": result}
                
        except Exception as e:
            error_msg = f"Stream Load异常: {str(e)}"
            logger.error(error_msg)
            return {"status": "error", "message": error_msg}
    
    def create_table(
        self,
        table_name: str,
        columns: List[Dict[str, Any]],
        keys: List[str],
        distribution_type: str = "HASH",
        distribution_keys: Optional[List[str]] = None,
        buckets: int = 10,
        properties: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        自动创建Doris表
        
        :param table_name: 表名
        :param columns: 列定义列表，每个元素包含name、type、nullable等属性
        :param keys: 主键/分桶键列表
        :param distribution_type: 分布类型，HASH或RANGE
        :param distribution_keys: 分布键列表，默认使用keys
        :param buckets: 分桶数量
        :param properties: 表属性配置
        :return: 创建结果
        """
        if not distribution_keys:
            distribution_keys = keys
        
        # 构造建表SQL
        column_defs = []
        for col in columns:
            col_def = f"`{col['name']}` {col['type']}"
            if not col.get("nullable", True):
                col_def += " NOT NULL"
            if col.get("comment"):
                col_def += f" COMMENT '{col['comment']}'"
            column_defs.append(col_def)
        
        key_def = ", ".join(f"`{k}`" for k in keys)
        dist_key_def = ", ".join(f"`{k}`" for k in distribution_keys)
        
        sql = f"""CREATE TABLE IF NOT EXISTS `{self.database}`.`{table_name}` (
    {',\n    '.join(column_defs)}
)
DUPLICATE KEY({key_def})
DISTRIBUTED BY {distribution_type}({dist_key_def}) BUCKETS {buckets}
"""
        
        # 添加表属性
        if properties:
            props = [f"'{k}' = '{v}'" for k, v in properties.items()]
            sql += f"PROPERTIES (\n    {',\n    '.join(props)}\n)"
        
        try:
            # 通过MySQL协议执行建表语句（需要使用MySQL客户端连接）
            # 这里简化处理，返回建表SQL供调用方执行
            return {
                "status": "success",
                "create_table_sql": sql,
                "message": "建表SQL生成成功"
            }
        except Exception as e:
            error_msg = f"生成建表SQL失败: {str(e)}"
            logger.error(error_msg)
            return {"status": "error", "message": error_msg}
    
    def close(self) -> None:
        """关闭写入器，flush剩余数据"""
        if self._buffer:
            logger.info(f"关闭写入器，flush剩余{len(self._buffer)}条数据")
            self.flush()
        self._session.close()

def stream_load_to_doris(
    fe_host: str,
    fe_port: int,
    database: str,
    table_name: str,
    username: str,
    password: str,
    data: List[Dict[str, Any]],
    **kwargs
) -> Dict[str, Any]:
    """
    一次性Stream Load写入数据到Doris（不使用攒批）
    
    :param fe_host: Doris FE IP
    :param fe_port: Doris FE端口
    :param database: 数据库名
    :param table_name: 表名
    :param username: 用户名
    :param password: 密码
    :param data: 要写入的数据列表
    :param kwargs: 其他参数
    :return: 写入结果
    """
    loader = DorisStreamLoader(
        fe_host=fe_host,
        fe_port=fe_port,
        database=database,
        username=username,
        password=password,
        **kwargs
    )
    
    loader.add_batch(data)
    result = loader.flush(table_name)
    loader.close()
    
    return result