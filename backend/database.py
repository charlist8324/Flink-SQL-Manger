"""
MySQL数据库配置
"""
import os
from pydantic_settings import BaseSettings


class DatabaseSettings(BaseSettings):
    """数据库配置"""
    
    # MySQL数据库配置
    DB_HOST: str = "192.168.31.251"
    DB_PORT: int = 3306
    DB_USER: str = "root"
    DB_PASSWORD: str = "Admin@900"
    DB_NAME: str = "flink_db"
    
    # 连接池配置
    DB_POOL_SIZE: int = 5
    DB_MAX_OVERFLOW: int = 10
    DB_POOL_TIMEOUT: int = 30
    DB_POOL_RECYCLE: int = 3600
    
    class Config:
        env_prefix = "FLINK_"


# 全局配置实例
db_settings = DatabaseSettings()


from urllib.parse import quote_plus

def get_database_url() -> str:
    """获取数据库连接URL"""
    # 对密码进行URL编码，处理特殊字符如@符号
    encoded_password = quote_plus(db_settings.DB_PASSWORD)
    return (
        f"mysql+pymysql://{db_settings.DB_USER}:{encoded_password}"
        f"@{db_settings.DB_HOST}:{db_settings.DB_PORT}/{db_settings.DB_NAME}"
        f"?charset=utf8mb4"
    )
