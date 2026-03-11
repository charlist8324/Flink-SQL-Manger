#!/bin/bash

# Flink SQL Runner 编译脚本

echo "=== Flink SQL Runner 编译脚本 ==="

# 检查 Java 是否安装
if ! command -v java &> /dev/null; then
    echo "错误: Java 未安装，请先安装 JDK 8 或以上"
    exit 1
fi

# 检查 Maven 是否安装
if ! command -v mvn &> /dev/null; then
    echo "错误: Maven 未安装，请先安装 Maven 3.3 或以上"
    exit 1
fi

echo "Java 版本: $(java -version 2>&1 | head -1)"
echo "Maven 版本: $(mvn -version | head -1)"
echo ""

# 进入项目目录
cd "$(dirname "$0")"

# 编译项目
echo "开始编译..."
mvn clean package -DskipTests

if [ $? -eq 0 ]; then
    echo ""
    echo "=== 编译成功！==="
    echo "编译后的 Jar 文件位于: target/flink-sql-runner-1.0.jar"
    echo ""
    echo "使用方法："
    echo "1. 上传到 Flink 集群："
    echo "   curl -X POST -F \"jarfile=@target/flink-sql-runner-1.0.jar\" http://10.160.10.221:8081/jars/upload"
    echo ""
    echo "2. 或通过 Flink CLI 运行："
    echo "   ./bin/flink run -c com.flink.sqlrunner.SQLRunner target/flink-sql-runner-1.0.jar \\"
    echo "     --sqlFile /path/to/job.sql --jobName MyJob"
    echo ""
else
    echo ""
    echo "=== 编译失败 ==="
    echo "请检查错误信息并重试"
    exit 1
fi
