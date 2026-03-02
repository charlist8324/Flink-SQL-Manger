import requests
import json

# 检查配置表中的配置
print("=== 检查Flink配置 ===")
response = requests.get('http://localhost:8000/api/flink-config')
configs = response.json()
print(f"当前配置数量: {len(configs)}")
for config in configs:
    print(f"  - {config['config_key']}: {config['config_value']}")

# 检查是否有checkpoint和savepoint配置
checkpoint_config = next((c for c in configs if c['config_key'] == 'checkpoint_path'), None)
savepoint_config = next((c for c in configs if c['config_key'] == 'savepoint_path'), None)

print()
print("=== 配置状态 ===")
if checkpoint_config:
    print(f"✅ Checkpoint路径已配置: {checkpoint_config['config_value']}")
else:
    print("⚠️ Checkpoint路径未配置")

if savepoint_config:
    print(f"✅ Savepoint路径已配置: {savepoint_config['config_value']}")
else:
    print("⚠️ Savepoint路径未配置")

# 如果没有配置，添加默认配置
if not checkpoint_config:
    print()
    print("=== 添加Checkpoint配置 ===")
    data = {
        'config_key': 'checkpoint_path',
        'config_value': 'hdfs:///flink/checkpoints',
        'description': 'Checkpoint路径'
    }
    response = requests.post('http://localhost:8000/api/flink-config', json=data)
    print(f"添加结果: {response.status_code} - {response.text}")

if not savepoint_config:
    print()
    print("=== 添加Savepoint配置 ===")
    data = {
        'config_key': 'savepoint_path',
        'config_value': 'hdfs:///flink/savepoints',
        'description': 'Savepoint路径'
    }
    response = requests.post('http://localhost:8000/api/flink-config', json=data)
    print(f"添加结果: {response.status_code} - {response.text}")

print()
print("=== 最终配置 ===")
response = requests.get('http://localhost:8000/api/flink-config')
configs = response.json()
for config in configs:
    print(f"  - {config['config_key']}: {config['config_value']}")
