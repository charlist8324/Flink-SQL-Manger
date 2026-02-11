import asyncio
import sys
sys.path.insert(0, 'backend')

from flink_client import FlinkClient

async def test_sql_gateway():
    """测试 SQL Gateway 提交 INSERT 作业"""
    print("=== 测试 SQL Gateway ===")
    
    client = FlinkClient()
    
    # 测试 INSERT 语句
    sql = """
    CREATE TABLE test_source (
        id INT,
        name STRING,
        ts TIMESTAMP(3)
    ) WITH (
        'connector' = 'datagen',
        'rows-per-second' = '1',
        'fields.id.kind' = 'sequence',
        'fields.id.start' = '1',
        'fields.id.end' = '100'
    );
    
    INSERT INTO test_sink 
    SELECT id, name, ts 
    FROM test_source;
    """
    
    try:
        print(f"提交 SQL: {sql[:100]}...")
        result = await client.submit_sql_job(
            sql=sql,
            job_name="test-insert-fix",
            parallelism=1
        )
        print(f"结果: {result}")
        if result.get("jobid"):
            print(f"✅ 作业提交成功，ID: {result['jobid']}")
        else:
            print(f"⚠️ 作业状态: {result.get('status')}")
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_sql_gateway())
