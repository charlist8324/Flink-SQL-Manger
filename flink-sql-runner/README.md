# Flink SQL Runner

一个简单的 Flink SQL Runner，用于执行 SQL 文件中的 Flink SQL 语句。

## 功能特性

- ✅ 从文件读取并执行 Flink SQL
- ✅ 支持所有 Flink SQL 语法（DDL、DML、DQL）
- ✅ 支持自定义作业名称和并行度
- ✅ 兼容 Flink 1.18.1
- ✅ 可与 Flink Manager 系统集成

## 编译步骤

### 前置要求

- JDK 8 或以上
- Maven 3.3 或以上
- Flink 1.18.1（可选，用于本地测试）

### 编译

```bash
# 进入项目目录
cd flink-sql-runner

# 编译项目（跳过测试）
mvn clean package -DskipTests

# 编译后的 Jar 位于 target/flink-sql-runner-1.0.jar
```

## 使用方式

### 方式 1：通过 Flink CLI 提交

```bash
# 在 Flink 安装目录下执行
./bin/flink run -c com.flink.sqlrunner.SQLRunner \
  /path/to/flink-sql-runner-1.0.jar \
  --sqlFile /path/to/your-job.sql \
  --jobName MyFlinkJob \
  --parallelism 2
```

### 方式 2：集成到 Flink Manager

#### 步骤 1：上传 Jar 到 Flink 集群

```bash
curl -X POST \
  -F "jarfile=@/path/to/flink-sql-runner-1.0.jar" \
  http://10.160.10.221:8081/jars/upload
```

#### 步骤 2：获取 Jar ID

从上传响应中复制 Jar ID，格式类似：
```
abc12345-6789-1234-5678-abcdef123456_flink-sql-runner-1.0.jar
```

#### 步骤 3：配置 Flink Manager

编辑 `backend/config.py`：

```python
SQL_RUNNER_JAR_ID = "abc12345-6789-1234-5678-abcdef123456_flink-sql-runner-1.0.jar"
```

#### 步骤 4：重启后端服务

```bash
# 停止当前服务（Ctrl+C）
# 重新启动
python -m uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
```

#### 步骤 5：提交作业

在 Web 界面的"可视化配置"页面配置作业并提交。

## 命令行参数

| 参数 | 必需 | 说明 | 默认值 |
|------|------|------|--------|
| `--sqlFile` | 是 | SQL 文件路径 | - |
| `--jobName` | 否 | 作业名称 | flink-sql-job |
| `--parallelism` | 否 | 并行度 | 1 |
| `--help` | 否 | 显示帮助信息 | - |

## SQL 文件格式

SQL 文件可以包含多个 SQL 语句，用分号 `;` 分隔：

```sql
-- 创建源表
CREATE TABLE source_table (
    id INT,
    name STRING,
    event_time TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'test-topic',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json'
);

-- 创建汇表
CREATE TABLE sink_table (
    id INT,
    name STRING,
    event_time TIMESTAMP(3)
) WITH (
    'connector' = 'print'
);

-- 执行插入语句（这将提交一个作业）
INSERT INTO sink_table
SELECT * FROM source_table;
```

## 作业提交流程

1. SQL Runner 读取 SQL 文件
2. 按分号分割 SQL 语句
3. 在 StreamTableEnvironment 中执行每个语句
4. 如果是 INSERT 语句，将作业提交到 Flink 集群
5. 作业提交后，SQL Runner 等待作业完成

## 查看作业状态

提交作业后，可以通过以下方式查看状态：

1. **Flink Web UI**：http://10.160.10.221:8081
2. **Flink Manager**：http://localhost:8000（作业管理页面）
3. **命令行**：

```bash
# 查看所有作业
curl http://10.160.10.221:8081/jobs

# 查看特定作业状态
curl http://10.160.10.221:8081/jobs/<job-id>
```

## 故障排除

### 编译失败

**问题**：Maven 编译报错

**解决方案**：
```bash
# 清理并重新编译
mvn clean install -U -DskipTests

# 如果依赖下载失败，使用阿里云镜像
# 在 pom.xml 中添加：
<repositories>
    <repository>
        <id>aliyun</id>
        <url>https://maven.aliyun.com/repository/public</url>
    </repository>
</repositories>
```

### Jar 上传失败

**问题**：上传 Jar 时返回错误

**解决方案**：
```bash
# 检查 Flink 集群是否运行
curl http://10.160.10.221:8081/overview

# 检查磁盘空间
df -h

# 查看 Flink 日志
tail -f /path/to/flink/log/flink-standalonesession-*.log
```

### SQL 执行失败

**问题**：作业提交后失败

**解决方案**：
1. 检查 SQL 语法是否正确
2. 查看作业日志：http://10.160.10.221:8081
3. 确保连接器配置正确
4. 检查数据源和汇表的权限

## 高级用法

### 使用自定义 Flink 配置

在 Flink CLI 运行时添加 `-y` 参数：

```bash
./bin/flink run -c com.flink.sqlrunner.SQLRunner \
  -yDparallelism.default=4 \
  -yDtaskmanager.memory.process.size=2048m \
  /path/to/flink-sql-runner-1.0.jar \
  --sqlFile /path/to/job.sql
```

### 从远程位置读取 SQL 文件

将 SQL 文件上传到 Flink 集群可访问的位置：

```bash
# 方式 1：使用 Flink 的分布式文件系统
hdfs dfs -put job.sql /flink/jobs/

# 方式 2：使用 Flink REST API 临时文件
# SQL Manager 系统会自动处理
```

## 许可证

Apache License 2.0

## 贡献

欢迎提交 Issue 和 Pull Request！
