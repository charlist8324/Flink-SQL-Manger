#!/usr/bin/env python3
import asyncio
import sys
sys.path.insert(0, '/d/python/py_opencode/Flink_manger/backend')

from flink_client import FlinkClient

async def test_sql_gateway():
    """测试 SQL Gateway 功能"""
    print("=== 测试 SQL Gateway ===")
    
    # 创建客户端
    client = FlinkClient()
    
    # 测试提交 SQL
    sql = """
    CREATE TABLE test_table (
        id INT,
        name STRING
    ) WITH (
        'connector' = 'print'
    )
    """
    
    try:
        print(f"提交 SQL: {sql[:100]}...")
        result = await client.submit_sql_job(
            sql=sql,
            job_name="test-job",
            parallelism=1
        )
        print(f"结果: {result}")
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_sql_gateway())
