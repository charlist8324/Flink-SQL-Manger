# 快速入门指南

## 回答您的问题

### 是否需要实现 flink-sql-runner.jar？

**答案：取决于您选择的提交方式！**

---

## 方式对比

### ✅ 方式 1：SQL Gateway（推荐）

**不需要 flink-sql-runner.jar**

- 直接通过 Flink SQL Gateway 的 REST API 提交 SQL
- 系统已自动支持
- 无需任何额外开发

**如何启用：**

1. 检查 SQL Gateway 是否运行：
   ```bash
   curl http://10.160.10.221:8083/v1/sessions
   ```

2. 如果未运行，启动它：
   ```bash
   ./bin/sql-gateway.sh start-foreground
   ```

3. 完成！无需任何配置，直接提交 SQL 作业。

---

### ⚙️ 方式 2：SQL Runner Jar

**需要 flink-sql-runner.jar**

我已经为您提供了完整的实现！

- 项目位置：`flink-sql-runner/`
- 包含：源代码、pom.xml、README、编译脚本

---

## 如果您选择 SQL Runner Jar 方式

### 快速编译（3 步）

```bash
# 1. 进入目录
cd flink-sql-runner

# 2. 运行编译脚本
chmod +x build.sh
./build.sh

# 3. 编译完成后，上传 Jar
curl -X POST -F "jarfile=@target/flink-sql-runner-1.0.jar" \
  http://10.160.10.221:8081/jars/upload
```

### 配置系统（3 步）

```bash
# 1. 复制 Jar ID（从上传响应中）
# 格式类似：abc123..._flink-sql-runner-1.0.jar

# 2. 编辑 backend/config.py
SQL_RUNNER_JAR_ID = "你的_jar_id"

# 3. 重启后端服务
python -m uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
```

---

## 我的建议

### 如果您的环境允许（推荐）：
✅ **使用 SQL Gateway**
- 无需编译任何东西
- 无需上传 Jar
- 即开即用

### 如果您的环境不支持：
✅ **使用我提供的 SQL Runner Jar**
- 已经为您准备好完整的实现
- 只需编译并上传
- 编译一次，永久使用

---

## 详细文档位置

1. **`SQL_GATEWAY_CONFIG.md`** - SQL Gateway 详细配置
2. **`SQL_RUNNER_JAR_CONFIG.md`** - SQL Runner Jar 配置
3. **`flink-sql-runner/README.md`** - SQL Runner 项目文档

---

## 快速决策树

```
能否启动 SQL Gateway？
├─ 是 → 使用 SQL Gateway（推荐）
│          └─ 无需任何额外工作
└─ 否 → 使用 SQL Runner Jar
           ├─ 编译我提供的代码
           ├─ 上传到 Flink
           └─ 配置 Jar ID
```

---

## 下一步

### 如果选择 SQL Gateway：

1. 启动 SQL Gateway：`./bin/sql-gateway.sh start-foreground`
2. 验证：`curl http://10.160.10.221:8083/v1/sessions`
3. 在可视化配置页面提交作业

### 如果选择 SQL Runner Jar：

1. 编译：`cd flink-sql-runner && ./build.sh`
2. 上传：`curl -X POST -F "jarfile=@target/flink-sql-runner-1.0.jar" http://10.160.10.221:8081/jars/upload`
3. 配置：编辑 `backend/config.py`
4. 重启：`python -m uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload`
5. 在可视化配置页面提交作业

---

## 问题？

- **SQL Gateway 相关**：查看 `SQL_GATEWAY_CONFIG.md`
- **SQL Runner Jar 相关**：查看 `flink-sql-runner/README.md`
- **通用问题**：查看 `SQL_RUNNER_JAR_CONFIG.md`
