import requests
import json

# Test GET request
print("Testing GET /api/flink-config...")
response = requests.get('http://localhost:8000/api/flink-config')
print(f"Status: {response.status_code}")
print(f"Content: {response.text}")
print()

# Test POST request
print("Testing POST /api/flink-config...")
data = {
    'config_key': 'checkpoint_path',
    'config_value': 'hdfs:///flink/checkpoints',
    'description': 'Checkpoint路径'
}
response = requests.post('http://localhost:8000/api/flink-config', json=data)
print(f"Status: {response.status_code}")
print(f"Content: {response.text}")
print()

# Test GET again to verify
print("Testing GET /api/flink-config again...")
response = requests.get('http://localhost:8000/api/flink-config')
print(f"Status: {response.status_code}")
print(f"Content: {response.text}")
