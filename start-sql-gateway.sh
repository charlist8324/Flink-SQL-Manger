#!/bin/bash

# Flink SQL Gateway 启动脚本

echo "=== Flink SQL Gateway 启动脚本 ==="

# Flink 安装目录（根据实际情况修改）
FLINK_HOME="/path/to/flink"

# SQL Gateway 配置
SQL_GATEWAY_HOST="0.0.0.0"
SQL_GATEWAY_PORT="8083"

# 启动 SQL Gateway
cd "$FLINK_HOME"

echo "启动 Flink SQL Gateway..."
./bin/sql-gateway.sh start-foreground \
  -Dsql-gateway.enabled=true \
  -Dsql-gateway.endpoint.rest.address=$SQL_GATEWAY_HOST \
  -Dsql-gateway.endpoint.rest.port=$SQL_GATEWAY_PORT
