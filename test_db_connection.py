"""
测试数据库连接
"""
import sys
from pathlib import Path

# 添加 backend 目录到 Python 路径
backend_dir = Path(__file__).parent / "backend"
sys.path.insert(0, str(backend_dir))

from database import get_database_url, db_settings

print("=== 数据库配置 ===")
print(f"DB_HOST: {db_settings.DB_HOST}")
print(f"DB_PORT: {db_settings.DB_PORT}")
print(f"DB_USER: {db_settings.DB_USER}")
print(f"DB_PASSWORD: {db_settings.DB_PASSWORD}")
print(f"DB_NAME: {db_settings.DB_NAME}")
print()

url = get_database_url()
print("=== 数据库连接URL ===")
print(url)
print()

# 测试连接
try:
    from sqlalchemy import create_engine, text
    engine = create_engine(url)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        print("✅ 数据库连接成功！")
        print(f"   查询结果: {result.fetchone()}")
except Exception as e:
    print("❌ 数据库连接失败！")
    print(f"   错误: {e}")
    import traceback
    traceback.print_exc()
