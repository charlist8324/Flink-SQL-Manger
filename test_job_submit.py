import requests
import json

# 提交一个简单的测试作业
print("=== 提交测试作业 ===")
sql_text = """
CREATE TABLE source_table (
    id INT,
    name STRING,
    timestamp TIMESTAMP(3)
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1'
);

CREATE TABLE sink_table (
    id INT,
    name STRING,
    timestamp TIMESTAMP(3)
) WITH (
    'connector' = 'print'
);

INSERT INTO sink_table
SELECT id, name, timestamp FROM source_table;
"""

data = {
    'job_name': 'test_checkpoint_savepoint',
    'sql_text': sql_text
}

print("正在提交作业...")
response = requests.post('http://localhost:8000/api/sql/submit', json=data)
print(f"提交状态: {response.status_code}")
print(f"响应内容: {response.text}")
