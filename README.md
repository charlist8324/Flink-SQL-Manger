# Flink SQL Manager - Flink SQL 低代码可视化配置平台

Flink Manager 是一个低代码的 Apache Flink SQL 可视化配置管理平台，通过图形化界面简化实时计算作业的开发、部署和运维。无需手动编写复杂的 Flink SQL 和连接器配置，只需通过表单选择即可快速构建实时数据集成任务。
备注：前端页面index.html是老版页面，index_new.html是新版页面，做了功能拆分，更流畅，功能更友好

## 核心特性

### 低代码可视化配置

- **拖拽式 SQL 构建**：通过表单配置自动生成 Flink SQL，无需手动编写 `WITH` 参数和复杂的连接器配置

- **多源多汇支持**：支持 CDC、JDBC、Kafka 等多种连接器的可视化配置

- **智能类型映射**：自动处理不同数据库之间的字段类型转换

- **实时预览**：配置过程中实时预览生成的 Flink SQL 和建表语句

  ![描述](./pic/1.png)

  ![描述](./pic/集群监控.png)

  ![描述](./pic/实时作业.png)

  ![描述](./pic/历史作业.png)

### 支持的连接器

#### 源表 (Source)
| 连接器 | 说明 |
|--------|------|
| MySQL CDC | 支持增量快照、无锁读取 |
| PostgreSQL CDC | 支持 Schema 配置、Slot 管理 |
| SQL Server CDC | 支持 Schema 配置 |
| Oracle CDC | 支持 Service Name 配置 |
| TiDB CDC | 未进行深入测试，进行中 |
| OceanBase CDC | 未进行深入测试，进行中 |
| Kafka | 未进行深入测试，进行中 |
| JDBC | 支持主流关系型数据库 |

#### 汇表 (Sink)
| 连接器 | 说明 |
|--------|------|
| Doris | 支持 UNIQUE/AGGREGATE/DUPLICATE 表类型，自动建表 |
| StarRocks | 支持主键/聚合/明细表，自动建表 |
| ClickHouse | 支持 MergeTree 引擎，自动建表 |
| Kafka | 未进行深入测试，进行中 |
| MySQL JDBC | 支持批量写入优化 |
| PostgreSQL JDBC | 支持自动类型映射 |
| Oracle JDBC | 支持 NUMBER/VARCHAR2 类型 |
| SQL Server JDBC | 支持 NVARCHAR 类型 |
| FileSystem | 支持 CSV/JSON/Parquet 格式 |

### 高级功能

#### 自动建表
- 根据源表结构自动生成目标表 DDL

- 支持 MySQL、PostgreSQL、Oracle、SQL Server、ClickHouse、Doris、StarRocks 等多种数据库

- 自动处理主键、索引、分区等配置

- Doris/StarRocks 支持配置表类型、副本数、Buckets

  ![描述](./pic/2.png)

  ![描述](./pic/3.png)

#### 批量同步配置
- **源表公共配置**：连接器类型、数据源、主机、端口、数据库、时区、认证信息等

- **汇表公共配置**：连接器类型、数据源、自动建表、攒批配置等

- 一键应用到所有表，大幅减少重复配置工作

  ![描述](./pic/4.png)

  ![描述](./pic/5.png)

#### 攒批优化
- 支持配置批量大小和刷新间隔
- 提升写入吞吐量，降低网络开销
- 适用于高频写入场景

#### 多种查询模式
- **INSERT 插入**：简单的数据同步
- **聚合查询**：支持 GROUP BY 聚合
- **JOIN 关联**：支持多表关联
- **窗口聚合**：支持滚动、滑动、会话窗口
- **自定义 SQL**：支持手写复杂 SQL

### 作业全生命周期管理

- **一键提交**：SQL 自动提交到 Flink 集群
- **状态监控**：实时跟踪作业运行状态
- **停止/取消**：优雅停止作业，支持 Savepoint
- **作业恢复**：从历史 Savepoint 快速恢复作业
- **历史作业**：查看所有历史作业记录，支持状态自动同步

### Savepoint 管理

- 手动触发 Savepoint

- 从 Savepoint 恢复作业

- Savepoint 历史记录管理

- 支持指定 Savepoint 路径、

  ![描述](./pic/实时作业管理.png)

  ![描述](./pic/历史作业管理.png)

### 数据源管理

- 集中管理数据库连接信息

- 支持连接测试

- 数据源与作业配置解耦

  ![描述](./pic/数据源管理页.png)

### 运维友好特性

- **自动容错**：CDC 连接器参数自动修正
- **类型转换**：自动处理不同数据库的类型映射
- **SQL 验证**：提交前验证 SQL 正确性
- **错误提示**：清晰的错误信息和解决建议

## 技术栈

- **后端**: Python 3.10+ (FastAPI, SQLAlchemy, Pydantic)
- **前端**: Vue.js 3 (Element Plus)
- **引擎**: Apache Flink 1.13+ (REST API)
- **数据库**: MySQL (存储元数据)

## 快速开始

### 1. 环境准备

```bash
# Python 依赖
pip install fastapi uvicorn sqlalchemy pymysql requests pydantic

# Flink 集群
# 下载并启动 Flink 1.18.1
```

### 2. 配置数据库

修改 `backend/config.py` 中的数据库连接信息：

```python
DATABASE_URL = "mysql+pymysql://user:password@localhost:3306/flink_manager"
```

### 3. 启动服务

```bash
# 后端
cd backend
python main.py

# 前端 (静态文件服务)
cd frontend/dist
python -m http.server 8080
```

### 4. 访问平台

打开浏览器访问 `http://localhost:8080`

## 使用流程

### 创建实时同步任务

1. **配置源表**
   - 选择连接器类型 (如 MySQL CDC)
   - 选择或配置数据源
   - 选择要同步的表
   - 配置 CDC 参数 (启动模式、时区等)

2. **配置汇表**
   - 选择连接器类型 (如 Doris)
   - 选择或配置数据源
   - 配置目标表名
   - 启用自动建表 (可选)

3. **配置查询**
   - 选择查询类型 (如 INSERT)
   - 配置字段映射
   - 配置过滤条件 (可选)

4. **预览并提交**
   - 预览生成的 Flink SQL
   - 预览建表语句
   - 提交作业

### 管理作业

- **查看作业列表**：实时监控作业状态
- **停止作业**：选择是否创建 Savepoint
- **恢复作业**：从历史 Savepoint 恢复
- **查看日志**：查看作业运行日志

## 项目结构

```
Flink_manger/
├── backend/                 # 后端代码
│   ├── main.py             # FastAPI 入口
│   ├── config.py           # 配置文件
│   ├── database.py         # 数据库连接
│   ├── flink_client.py     # Flink REST API 客户端
│   └── db_operations.py    # 数据库操作
├── frontend/               # 前端代码
│   └── dist/              # 编译后的静态文件
│       ├── index.html     # 主页面
│       └── pages/         # 页面组件
├── logs/                   # 运行日志
└── README.md              # 项目说明
```

## 常见问题

### 1. 如何添加新的连接器？

下载对应的 Flink 连接器 JAR 包到 Flink 的 `lib/` 目录，重启 Flink 集群。

### 2. 自动建表失败怎么办？

检查数据库用户权限，确保有 CREATE TABLE 权限。可以手动预览建表语句并手动执行。

### 3. 作业提交失败怎么办？

查看 Flink Web UI 的日志，确认连接器配置是否正确，数据源连接是否正常。

## 贡献

欢迎提交 Issue 和 Pull Request。

## 技术支持

![描述](./pic/knw.jpg)

