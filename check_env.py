#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Flink Manager 环境检查脚本
"""
import sys
import os

print("=" * 60)
print("Flink Manager Environment Check")
print("=" * 60)
print()

# Python version
print("1. Python Version:")
print(f"   Python Path: {sys.executable}")
print(f"   Python Version: {sys.version}")
print()

# uvicorn
print("2. Check uvicorn:")
try:
    import uvicorn
    print(f"   OK uvicorn installed (version: {uvicorn.__version__})")
except ImportError:
    print(f"   FAIL uvicorn not installed")
print()

# FastAPI
print("3. Check FastAPI:")
try:
    import fastapi
    print(f"   OK FastAPI installed (version: {fastapi.__version__})")
except ImportError:
    print(f"   FAIL FastAPI not installed")
print()

# httpx
print("4. Check httpx:")
try:
    import httpx
    print(f"   OK httpx installed (version: {httpx.__version__})")
except ImportError:
    print(f"   FAIL httpx not installed")
print()

# Config file
print("5. Config File:")
config_path = os.path.join(os.path.dirname(__file__), "backend", "config.py")
if os.path.exists(config_path):
    print(f"   OK config.py exists")
    with open(config_path, "r", encoding="utf-8") as f:
        content = f.read()
        for line in content.split('\n'):
            if 'FLINK_REST_URL' in line and '=' in line and not line.strip().startswith('#'):
                print(f"   Flink REST URL: {line.strip()}")
            if 'SQL_GATEWAY_URL' in line and '=' in line and not line.strip().startswith('#'):
                print(f"   SQL Gateway URL: {line.strip()}")
else:
    print(f"   FAIL config.py not found")
print()

# Backend module
print("6. Check Backend Module:")
try:
    import backend.main
    print(f"   OK backend.main can be imported")
except ImportError as e:
    print(f"   FAIL cannot import backend.main: {e}")
print()

# Port check
print("7. Port Check (8000):")
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    result = s.connect_ex(('localhost', 8000))
    s.close()
    if result == 0:
        print(f"   WARN Port 8000 already in use")
    else:
        print(f"   OK Port 8000 available")
except Exception as e:
    print(f"   FAIL Port check failed: {e}")

print()
print("=" * 60)
print("Environment Check Complete")
print("=" * 60)
