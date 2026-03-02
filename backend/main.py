"""
Flink SQL 管理系统 - 后端主程序
"""
import logging
import time
from pathlib import Path
import uuid
from fastapi import FastAPI, HTTPException, UploadFile, File, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from typing import List, Optional
from contextlib import asynccontextmanager

# 配置日志
LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "run.log"

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)
logger.info(f"=== Flink Manager 启动 ===")
logger.info(f"日志文件: {LOG_FILE}")

from config import FLINK_REST_URL, SQL_FILES_DIR, BASE_DIR
from database import db_settings, get_database_url
from db_operations import (
    db_manager, save_job, update_job_state, get_job, get_all_jobs,
    save_savepoint, get_latest_savepoint, get_savepoints, log_operation,
    add_datasource, get_datasources, update_datasource, delete_datasource, test_datasource_connection
)
from schemas import (
    SqlJobSubmitRequest, JarJobSubmitRequest, JobSubmitResponse,
    ClusterStatus, JobInfo, JobDetail, SavepointRequest, ResumeJobRequest,
    DataSourceCreateRequest, DataSourceUpdateRequest, DataSourceResponse,
    DataSourceBase, TableCreateRequest
)
from flink_client import flink_client, FlinkClient
from sqlalchemy import create_engine, inspect, text
from pydantic import BaseModel, Field
from urllib.parse import quote_plus

class DatabaseConnectionRequest(BaseModel):
    host: str
    port: int
    username: str
    password: str
    database: str






# ============ 应用生命周期管理 ============

@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动时初始化数据库
    logger.info("正在初始化数据库连接...")
    
    # 后台任务引用
    checkpoint_sync_task = None
    
    try:
        db_manager.initialize()
        
        # 自动修复数据库 Schema
        session = db_manager.get_session()
        try:
            logger.info("正在检查数据库 Schema...")
            # 1. 检查 flink_jobs 表
            result = session.execute(text("SHOW COLUMNS FROM flink_jobs LIKE 'savepoint_timestamp'"))
            if not result.fetchone():
                logger.info("⚠️ flink_jobs 表缺少 savepoint_timestamp 字段，正在添加...")
                session.execute(text("ALTER TABLE flink_jobs ADD COLUMN savepoint_timestamp BIGINT COMMENT 'Savepoint时间戳(毫秒)'"))
                session.commit()
                logger.info("✅ savepoint_timestamp 字段已添加")
            
            # 2. 检查 flink_savepoints 表
            result = session.execute(text("SHOW COLUMNS FROM flink_savepoints LIKE 'timestamp'"))
            if not result.fetchone():
                logger.info("⚠️ flink_savepoints 表缺少 timestamp 字段，正在添加...")
                session.execute(text("ALTER TABLE flink_savepoints ADD COLUMN timestamp BIGINT COMMENT '时间戳(毫秒)'"))
                session.commit()
                logger.info("✅ timestamp 字段已添加")
            
            # 3. 检查 flink_checkpoints 表是否存在
            result = session.execute(text("SHOW TABLES LIKE 'flink_checkpoints'"))
            if not result.fetchone():
                logger.info("⚠️ flink_checkpoints 表不存在，正在创建...")
                session.execute(text("""
                    CREATE TABLE flink_checkpoints (
                        id INT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
                        job_id VARCHAR(50) NOT NULL COMMENT '作业ID',
                        checkpoint_id BIGINT NOT NULL COMMENT 'Checkpoint ID',
                        checkpoint_path VARCHAR(500) NOT NULL COMMENT 'Checkpoint路径',
                        status VARCHAR(20) DEFAULT 'COMPLETED' COMMENT '状态: COMPLETED, IN_PROGRESS, FAILED',
                        trigger_time BIGINT COMMENT '触发时间(毫秒)',
                        finish_time BIGINT COMMENT '完成时间(毫秒)',
                        checkpoint_size BIGINT COMMENT 'Checkpoint大小(字节)',
                        duration INT COMMENT '耗时(毫秒)',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                        INDEX idx_job_id (job_id),
                        INDEX idx_checkpoint_id (checkpoint_id)
                    ) COMMENT='Checkpoint记录表'
                """))
                session.commit()
                logger.info("✅ flink_checkpoints 表已创建")
            
            # 4. 删除 data_sources 表中不再需要的 Kafka 字段
            kafka_columns_to_drop = [
                'kafka_bootstrap_servers',
                'kafka_topic', 
                'kafka_group_id',
                'kafka_sasl_mechanism',
                'kafka_sasl_username',
                'kafka_sasl_password'
            ]
            
            for col_name in kafka_columns_to_drop:
                result = session.execute(text(f"SHOW COLUMNS FROM data_sources LIKE '{col_name}'"))
                if result.fetchone():
                    logger.info(f"⚠️ 删除 data_sources 表中不再需要的字段: {col_name}...")
                    session.execute(text(f"ALTER TABLE data_sources DROP COLUMN {col_name}"))
                    session.commit()
                    logger.info(f"✅ {col_name} 字段已删除")
            
            # 5. 创建 kafka_jobs 表（Kafka同步作业配置）
            result = session.execute(text("SHOW TABLES LIKE 'kafka_jobs'"))
            if not result.fetchone():
                logger.info("⚠️ kafka_jobs 表不存在，正在创建...")
                session.execute(text("""
                    CREATE TABLE kafka_jobs (
                        id INT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
                        job_name VARCHAR(100) NOT NULL UNIQUE COMMENT '作业名称',
                        job_type VARCHAR(20) DEFAULT 'kafka_to_db' COMMENT '作业类型',
                        source_datasource_id INT NOT NULL COMMENT '源数据源ID',
                        source_topics VARCHAR(500) NOT NULL COMMENT 'Kafka Topic列表',
                        source_group_id VARCHAR(200) NULL COMMENT '消费者组ID',
                        source_start_mode VARCHAR(20) DEFAULT 'latest' COMMENT '启动模式',
                        source_timestamp BIGINT NULL COMMENT '起始时间戳',
                        source_format VARCHAR(20) DEFAULT 'json' COMMENT '数据格式',
                        source_schema JSON NULL COMMENT '源数据Schema',
                        target_datasource_id INT NOT NULL COMMENT '目标数据源ID',
                        target_table VARCHAR(100) NOT NULL COMMENT '目标表名',
                        target_database VARCHAR(100) NULL COMMENT '目标数据库名',
                        auto_create_table TINYINT(1) DEFAULT 1 COMMENT '是否自动建表',
                        table_primary_keys VARCHAR(200) NULL COMMENT '主键字段',
                        field_mappings JSON NULL COMMENT '字段映射配置',
                        parallelism INT DEFAULT 1 COMMENT '并行度',
                        checkpoint_interval INT DEFAULT 60000 COMMENT 'Checkpoint间隔',
                        flink_job_id VARCHAR(50) NULL COMMENT 'Flink作业ID',
                        state VARCHAR(20) DEFAULT 'CREATED' COMMENT '作业状态',
                        last_error TEXT NULL COMMENT '最后错误信息',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                        INDEX idx_state (state),
                        INDEX idx_flink_job_id (flink_job_id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Kafka同步作业配置表'
                """))
                session.commit()
                logger.info("✅ kafka_jobs 表已创建")
            
            # 6. 修改 data_sources 表的 host 和 port 字段为可空
            result = session.execute(text("SHOW COLUMNS FROM data_sources LIKE 'host'"))
            row = result.fetchone()
            if row and 'NO' in str(row):
                logger.info("⚠️ 修改 data_sources.host 为可空...")
                session.execute(text("ALTER TABLE data_sources MODIFY COLUMN host VARCHAR(255) NULL COMMENT '主机地址'"))
                session.commit()
            
            result = session.execute(text("SHOW COLUMNS FROM data_sources LIKE 'port'"))
            row = result.fetchone()
            if row and 'NO' in str(row):
                logger.info("⚠️ 修改 data_sources.port 为可空...")
                session.execute(text("ALTER TABLE data_sources MODIFY COLUMN port INT NULL COMMENT '端口'"))
                session.commit()
                
        except Exception as e:
            logger.error(f"❌ Schema 检查/修复失败: {e}")
            # 不阻断启动，因为可能只是权限问题或已存在
        finally:
            session.close()
            
        logger.info("✅ 数据库初始化成功")
        
        # 启动后台任务：定期同步Checkpoint状态
        import asyncio
        from db_operations import get_flink_config, save_checkpoint, get_all_jobs
        
        async def sync_checkpoints():
            """定期同步Checkpoint状态到数据库"""
            while True:
                try:
                    # 获取checkpoint同步间隔配置（默认30秒）
                    sync_interval_str = get_flink_config("checkpoint_sync_interval")
                    sync_interval = int(sync_interval_str) if sync_interval_str else 30
                    
                    logger.info(f"🔄 开始同步Checkpoint状态...")
                    
                    # 获取所有运行中的作业
                    running_jobs = get_all_jobs(limit=1000)
                    running_jobs = [job for job in running_jobs if job.get("state") == "RUNNING"]
                    
                    logger.info(f"找到 {len(running_jobs)} 个运行中的作业")
                    
                    for job in running_jobs:
                        job_id = job.get("job_id")
                        if not job_id:
                            continue
                        
                        try:
                            # 从Flink REST API获取checkpoint信息
                            checkpoints_info = await flink_client.get_job_checkpoints(job_id)
                            
                            if checkpoints_info and "history" in checkpoints_info:
                                history = checkpoints_info["history"]
                                
                                logger.info(f"作业 {job_id}: history数量={len(history)}")
                                
                                # 从history中获取id最大的checkpoint（最新的）
                                if history and len(history) > 0:
                                    # 按id排序，取最大的
                                    latest_checkpoint = max(history, key=lambda x: x.get("id", 0))
                                    latest_id = latest_checkpoint.get("id")
                                    
                                    logger.info(f"作业 {job_id}: 找到最新Checkpoint #{latest_id}")
                                    
                                    # 打印完整的checkpoint数据结构
                                    logger.info(f"Checkpoint完整数据: {latest_checkpoint}")
                                    
                                    cp_id = latest_checkpoint.get("id")
                                    cp_path = latest_checkpoint.get("path")
                                    
                                    # 如果没有path字段，尝试从external_path获取
                                    if not cp_path:
                                        cp_path = latest_checkpoint.get("external_path")
                                    
                                    # 如果还是没有路径，跳过保存，保留数据库已有记录
                                    if not cp_path:
                                        logger.warning(f"⚠️ 无法从API获取Checkpoint路径，保留数据库已有记录")
                                        continue
                                    
                                    logger.info(f"Checkpoint #{cp_id}: path={cp_path}")
                                    
                                    cp_status = latest_checkpoint.get("status", "COMPLETED")
                                    
                                    # 转换时间戳（毫秒）
                                    trigger_time = int(latest_checkpoint.get("trigger_timestamp", 0))
                                    
                                    # 获取完成时间和大小
                                    finish_time = None
                                    checkpoint_size = None
                                    duration = None
                                    
                                    if "latest_ack_timestamp" in latest_checkpoint:
                                        finish_time = int(latest_checkpoint.get("latest_ack_timestamp", 0))
                                    
                                    if "checkpointed_size" in latest_checkpoint:
                                        checkpoint_size = int(latest_checkpoint.get("checkpointed_size", 0))
                                    
                                    if "end_to_end_duration" in latest_checkpoint:
                                        duration = int(latest_checkpoint.get("end_to_end_duration", 0))
                                    
                                    # 保存到数据库
                                    if cp_path:
                                        result = save_checkpoint(
                                            job_id=job_id,
                                            checkpoint_id=cp_id,
                                            checkpoint_path=cp_path,
                                            trigger_time=trigger_time,
                                            finish_time=finish_time or trigger_time,
                                            status=cp_status,
                                            checkpoint_size=checkpoint_size,
                                            duration=duration
                                        )
                                        if result:
                                            logger.info(f"✅ 已保存作业 {job_id} 的Checkpoint #{cp_id}")
                        except Exception as e:
                            error_msg = str(e)
                            # 如果是404错误，说明作业已经不在Flink中了，更新状态
                            if "404" in error_msg or "not found" in error_msg.lower():
                                logger.warning(f"⚠️ 作业 {job_id} 在Flink中不存在，更新状态为FAILED")
                                try:
                                    update_job_state(job_id, "FAILED")
                                except Exception as update_err:
                                    logger.error(f"更新作业 {job_id} 状态失败: {update_err}")
                            else:
                                logger.error(f"❌ 同步作业 {job_id} 的Checkpoint失败: {e}")
                    
                    logger.info(f"✅ Checkpoint同步完成，处理了 {len(running_jobs)} 个运行中的作业")
                    
                except Exception as e:
                    logger.error(f"❌ Checkpoint同步任务异常: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                
                # 固定每N秒同步一次（根据配置）
                logger.info(f"⏰ 等待{sync_interval}秒后进行下一次同步...")
                await asyncio.sleep(sync_interval)
        
        # 启动后台任务
        checkpoint_sync_task = asyncio.create_task(sync_checkpoints())
        logger.info("✅ Checkpoint同步后台任务已启动")
        
    except Exception as e:
        logger.error(f"❌ 数据库初始化失败: {e}")
        raise
    
    yield
    
    # 关闭时清理资源
    if checkpoint_sync_task:
        checkpoint_sync_task.cancel()
        try:
            await checkpoint_sync_task
        except asyncio.CancelledError:
            pass
        logger.info("✅ Checkpoint同步后台任务已停止")
    
    logger.info("正在关闭数据库连接...")
    db_manager.close()
    logger.info("数据库连接已关闭")


# ============ FastAPI 应用 ============

app = FastAPI(
    title="Flink SQL Manager",
    description="Flink SQL作业管理系统",
    version="1.0.0",
    lifespan=lifespan
)

# CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/api/metadata/tables", tags=["系统"])
async def get_database_tables(request: DatabaseConnectionRequest):
    """连接数据库并获取表名列表"""
    try:
        from urllib.parse import quote_plus
        # 构建 MySQL 连接字符串
        # 格式: mysql+pymysql://user:password@host:port/database
        encoded_user = quote_plus(request.username)
        encoded_password = quote_plus(request.password)
        db_url = f"mysql+pymysql://{encoded_user}:{encoded_password}@{request.host}:{request.port}/{request.database}"
        
        # 创建引擎
        engine = create_engine(db_url)
        
        # 使用 inspect 获取表名
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        
        return {"tables": tables}
    except Exception as e:
        logger.error(f"获取表名失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"连接数据库失败: {str(e)}")


@app.post("/api/metadata/columns", tags=["系统"])
async def get_table_columns(request: DatabaseConnectionRequest, table_name: str = None):
    """连接数据库并获取指定表的字段信息"""
    try:
        from urllib.parse import quote_plus
        
        encoded_user = quote_plus(request.username)
        encoded_password = quote_plus(request.password)
        db_url = f"mysql+pymysql://{encoded_user}:{encoded_password}@{request.host}:{request.port}/{request.database}"
        
        engine = create_engine(db_url)
        inspector = inspect(engine)
        
        if table_name:
            columns = inspector.get_columns(table_name)
            pk_constraint = inspector.get_pk_constraint(table_name)
            pk_columns = pk_constraint.get('constrained_columns', [])
            
            result = []
            for col in columns:
                col_type = str(col['type']).lower()
                flink_type = "STRING"
                
                if 'int' in col_type and 'bigint' not in col_type:
                    flink_type = "INT"
                elif 'bigint' in col_type:
                    flink_type = "BIGINT"
                elif 'float' in col_type:
                    flink_type = "FLOAT"
                elif 'double' in col_type:
                    flink_type = "DOUBLE"
                elif 'decimal' in col_type:
                    flink_type = "DECIMAL"
                elif 'bool' in col_type:
                    flink_type = "BOOLEAN"
                elif 'timestamp' in col_type or 'datetime' in col_type:
                    flink_type = "TIMESTAMP"
                elif 'date' in col_type and 'datetime' not in col_type:
                    flink_type = "DATE"
                elif 'time' in col_type and 'timestamp' not in col_type and 'datetime' not in col_type:
                    flink_type = "TIME"
                
                result.append({
                    "name": col['name'],
                    "type": flink_type,
                    "original_type": str(col['type']),
                    "primaryKey": col['name'] in pk_columns
                })
            return {"columns": result}
        else:
            return {"columns": []}
    except Exception as e:
        logger.error(f"获取字段失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取字段失败: {str(e)}")


@app.post("/api/metadata/tables/create", tags=["系统"])
async def create_table(request: TableCreateRequest):
    """创建物理表"""
    try:
        from urllib.parse import quote_plus
        
        host = request.host
        port = request.port
        username = request.username
        password = request.password
        database = request.database
        
        # 如果提供了 datasource_id，从数据库获取连接信息
        if request.datasource_id:
            ds = get_datasources(ds_id=request.datasource_id)
            if not ds:
                raise HTTPException(status_code=404, detail="数据源不存在")
            host = ds.host
            port = ds.port
            username = ds.username
            password = ds.password
            database = ds.database
            
            encoded_user = quote_plus(username)
            encoded_password = quote_plus(password) if password else ""
            db_url = f"mysql+pymysql://{encoded_user}:{encoded_password}@{host}:{port}/{database}"
            
        elif request.url:
            # 尝试从 URL 解析或转换
            # 假设是 jdbc:mysql://...
            db_url = request.url
            if db_url.startswith("jdbc:mysql://"):
                db_url = db_url.replace("jdbc:mysql://", "mysql+pymysql://")
            elif db_url.startswith("jdbc:clickhouse://"):
                 # ClickHouse 支持暂未完全实现
                 pass
            
            # 如果提供了用户名密码，但 URL 中没有（简单判断），这比较复杂
            # 这里简化处理：如果提供了 url，就直接用，如果 URL 里没密码那就会失败
            # 更好的方式是解析 URL，但这里为了兼容性，我们尽量依赖 datasource_id
            
            # 如果 URL 转换后还是不符合 SQLAlchemy 格式，可能需要进一步处理
            # 暂时假设用户输入的 URL 或者前端传来的 URL 是标准的 JDBC MySQL
            
            # 注入用户名密码（如果 URL 没包含）
            if username and password and '@' not in db_url:
                # 这是一个非常粗糙的注入，仅作 fallback
                # mysql+pymysql://host:port/db -> mysql+pymysql://user:pass@host:port/db
                prefix = "mysql+pymysql://"
                if db_url.startswith(prefix):
                    rest = db_url[len(prefix):]
                    encoded_user = quote_plus(username)
                    encoded_password = quote_plus(password)
                    db_url = f"{prefix}{encoded_user}:{encoded_password}@{rest}"

        else:
             if not host or not port or not username or not database:
                  raise HTTPException(status_code=400, detail="缺少数据库连接信息")
             
             # 构建连接字符串
             encoded_user = quote_plus(username)
             encoded_password = quote_plus(password) if password else ""
             db_url = f"mysql+pymysql://{encoded_user}:{encoded_password}@{host}:{port}/{database}"
        
        engine = create_engine(db_url)
        
        # 检查表是否存在
        inspector = inspect(engine)
        if request.table_name in inspector.get_table_names():
            return {"status": "skipped", "message": f"表 {request.table_name} 已存在"}
            
        # 生成 CREATE TABLE SQL
        columns_sql = []
        primary_keys = []
        
        logger.info(f"创建表请求: table_name={request.table_name}, connector_type={request.connector_type}, fields_count={len(request.fields) if request.fields else 0}")
        
        for field in request.fields:
            # 类型映射
            sql_type = field.type.upper()
            if sql_type == 'STRING':
                length = field.precision if field.precision else '255'
                sql_type = f"VARCHAR({length})"
            elif sql_type in ['INT', 'INTEGER']:
                sql_type = "INT"
            elif sql_type == 'BIGINT':
                sql_type = "BIGINT"
            elif sql_type == 'BOOLEAN':
                sql_type = "BOOLEAN"
            elif sql_type in ['TIMESTAMP', 'TIMESTAMP_LTZ']:
                sql_type = "DATETIME"
            elif sql_type == 'DATE':
                sql_type = "DATE"
            elif sql_type == 'DECIMAL':
                p = field.precision if field.precision else '10'
                s = field.scale if field.scale else '0'
                sql_type = f"DECIMAL({p}, {s})"
            elif sql_type == 'FLOAT':
                sql_type = "FLOAT"
            elif sql_type == 'DOUBLE':
                sql_type = "DOUBLE"
                
            col_def = f"`{field.name}` {sql_type}"
            columns_sql.append(col_def)
            
            if field.primaryKey:
                primary_keys.append(f"`{field.name}`")
                
        if not columns_sql:
            raise HTTPException(status_code=400, detail="字段列表不能为空")
        
        is_doris = request.connector_type in ['doris', 'doris-cdc']
        is_starrocks = request.connector_type in ['starrocks', 'starrocks-cdc']
        is_olap = is_doris or is_starrocks
        table_type = request.table_type or 'UNIQUE'  # 默认主键表
        buckets = request.buckets or 10
        replication_num = request.replication_num or 1
        
        if is_olap:
            create_sql = f"CREATE TABLE `{request.table_name}` (\n"
            create_sql += ",\n".join(columns_sql)
            create_sql += "\n) ENGINE=OLAP"
            
            if is_doris:
                if table_type == 'UNIQUE' and primary_keys:
                    create_sql += f"\nUNIQUE KEY ({', '.join(primary_keys)})"
                elif table_type == 'AGGREGATE' and primary_keys:
                    create_sql += f"\nAGGREGATE KEY ({', '.join(primary_keys)})"
                elif table_type == 'DUPLICATE' and primary_keys:
                    create_sql += f"\nDUPLICATE KEY ({', '.join(primary_keys)})"
            elif is_starrocks:
                if table_type == 'UNIQUE' and primary_keys:
                    create_sql += f"\nPRIMARY KEY ({', '.join(primary_keys)})"
                elif table_type == 'AGGREGATE' and primary_keys:
                    create_sql += f"\nAGGREGATE KEY ({', '.join(primary_keys)})"
                elif table_type == 'DUPLICATE' and primary_keys:
                    create_sql += f"\nDUPLICATE KEY ({', '.join(primary_keys)})"
            
            if primary_keys:
                create_sql += f"\nDISTRIBUTED BY HASH({', '.join(primary_keys)}) BUCKETS {buckets}"
            elif request.fields:
                create_sql += f"\nDISTRIBUTED BY HASH(`{request.fields[0].name}`) BUCKETS {buckets}"
            
            create_sql += f"\nPROPERTIES(\"replication_num\" = \"{replication_num}\")"
        else:
            create_sql = f"CREATE TABLE `{request.table_name}` (\n"
            create_sql += ",\n".join(columns_sql)
            if primary_keys:
                create_sql += f",\nPRIMARY KEY ({', '.join(primary_keys)})"
            create_sql += "\n)"

        logger.info(f"Executing Create Table SQL: {create_sql}")
        
        with engine.connect() as conn:
            conn.execute(text(create_sql))
            conn.commit()
            
        return {"status": "success", "message": f"表 {request.table_name} 创建成功", "create_sql": create_sql}
        
    except Exception as e:
        logger.error(f"创建表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"创建表失败: {str(e)}")


@app.post("/api/metadata/tables/create-custom", tags=["系统"])
async def create_table_custom(request: TableCreateRequest):
    """使用自定义SQL创建物理表"""
    try:
        from urllib.parse import quote_plus
        
        db_url = None
        
        if request.url:
            db_url = request.url
            if db_url.startswith("jdbc:mysql://"):
                db_url = db_url.replace("jdbc:mysql://", "mysql+pymysql://")
            elif db_url.startswith("jdbc:clickhouse://"):
                pass
            
            if request.username and request.password and '@' not in db_url:
                prefix = "mysql+pymysql://"
                if db_url.startswith(prefix):
                    rest = db_url[len(prefix):]
                    encoded_user = quote_plus(request.username)
                    encoded_password = quote_plus(request.password)
                    db_url = f"{prefix}{encoded_user}:{encoded_password}@{rest}"
        else:
            raise HTTPException(status_code=400, detail="缺少数据库连接URL")
        
        engine = create_engine(db_url)
        
        create_sql = request.custom_sql if request.custom_sql else None
        if not create_sql:
            raise HTTPException(status_code=400, detail="缺少建表SQL")
        
        logger.info(f"Executing Custom Create Table SQL: {create_sql}")
        
        with engine.connect() as conn:
            conn.execute(text(create_sql))
            conn.commit()
            
        return {"status": "success", "message": f"表创建成功", "create_sql": create_sql}
        
    except Exception as e:
        logger.error(f"创建表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"创建表失败: {str(e)}")


from schemas import (
    SqlJobSubmitRequest, JarJobSubmitRequest, JobSubmitResponse,
    ClusterStatus, JobInfo, JobDetail, SavepointRequest, ResumeJobRequest
)
from flink_client import flink_client, FlinkClient


# ============ 健康检查 ============
@app.get("/health", tags=["系统"])
async def health_check():
    """系统健康检查"""
    try:
        # 测试数据库连接
        session = db_manager.get_session()
        session.execute("SELECT 1")
        session.close()
        return {"status": "ok", "database": "connected"}
    except Exception as e:
        return {"status": "error", "database": "disconnected", "error": str(e)}


# ============ 集群管理 ============
@app.get("/api/cluster/status", tags=["集群管理"])
async def get_cluster_status():
    """获取 Flink 集群状态"""
    try:
        overview = await flink_client.get_cluster_overview()
        jobs_overview = await flink_client.get_jobs_overview()

        # 统计各状态作业数量
        jobs_running = len([j for j in jobs_overview.get("jobs", []) if j.get("status") == "RUNNING"])
        jobs_finished = len([j for j in jobs_overview.get("jobs", []) if j.get("status") == "FINISHED"])
        jobs_cancelled = len([j for j in jobs_overview.get("jobs", []) if j.get("status") == "CANCELED"])
        jobs_failed = len([j for j in jobs_overview.get("jobs", []) if j.get("status") == "FAILED"])

        return {
            "status": "online",
            "flink_version": overview.get("flink-version", "unknown"),
            "slots_total": overview.get("slots-total", 0),
            "slots_available": overview.get("slots-available", 0),
            "taskmanagers": overview.get("taskmanagers", 0),
            "jobs_running": jobs_running,
            "jobs_finished": jobs_finished,
            "jobs_cancelled": jobs_cancelled,
            "jobs_failed": jobs_failed,
        }
    except Exception as e:
        return {
            "status": "offline",
            "error": str(e),
            "flink_version": None,
            "slots_total": 0,
            "slots_available": 0,
            "taskmanagers": 0,
            "jobs_running": 0,
            "jobs_finished": 0,
            "jobs_cancelled": 0,
            "jobs_failed": 0,
        }


@app.get("/api/cluster/config", tags=["集群管理"])
async def get_cluster_config():
    """获取 Flink 集群配置"""
    try:
        return await flink_client.get_cluster_config()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/cluster/taskmanagers", tags=["集群管理"])
async def get_taskmanagers():
    """获取所有 TaskManager 列表"""
    try:
        return await flink_client.get_taskmanagers()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/cluster/taskmanagers/{tm_id}", tags=["集群管理"])
async def get_taskmanager_detail(tm_id: str):
    """获取 TaskManager 详情"""
    try:
        return await flink_client.get_taskmanager_detail(tm_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/cluster/taskmanagers/{tm_id}/metrics", tags=["集群管理"])
async def get_taskmanager_metrics(tm_id: str, metrics: Optional[str] = Query(None, description="指标名称，逗号分隔")):
    """获取 TaskManager 指标"""
    try:
        metric_list = metrics.split(",") if metrics else None
        return await flink_client.get_taskmanager_metrics(tm_id, metric_list)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============ 作业管理 ============
@app.get("/api/jobs/runs", tags=["作业管理"])
async def get_job_runs(limit: int = 100, offset: int = 0):
    """获取所有作业运行记录（分页，不去重）"""
    logger.info(f"=== 正在请求作业运行记录 (limit={limit}, offset={offset}) ===")
    try:
        # 从数据库获取所有作业记录
        all_jobs = get_all_jobs(state=None, limit=limit)
        
        # 从 Flink 获取正在运行的作业列表
        flink_job_map = {}
        try:
            jobs_overview = await flink_client.get_jobs_overview()
            flink_jobs = jobs_overview.get("jobs", [])
            flink_job_map = {job["id"]: job for job in flink_jobs}
        except Exception as e:
            logger.warning(f"获取 Flink 作业列表失败: {e}")
        
        # 同步Flink状态
        for job in all_jobs:
            job_id = job.get("job_id")
            if job_id in flink_job_map:
                flink_info = flink_job_map[job_id]
                flink_status = flink_info.get("status")
                if flink_status in ["RUNNING", "FINISHED", "FAILED", "CANCELED", "RESTARTING"]:
                    job["state"] = flink_status
                if not job.get("start_time") and flink_info.get("start-time"):
                    job["start_time"] = flink_info.get("start-time")
            else:
                # 只有当数据库中状态为RUNNING时，才更新为FAILED并同步checkpoint
                if job.get("state") == "RUNNING":
                    job["state"] = "FAILED"
                    try:
                        update_job_state(job_id, "FAILED")
                        # 作业失败时，从Flink获取失败的checkpoint信息
                        logger.info(f"⚠️ 作业 {job_id} 已失败，正在获取失败的Checkpoint信息...")
                        try:
                            checkpoints_info = await flink_client.get_job_checkpoints(job_id)
                            if checkpoints_info:
                                # 从latest.failed获取失败的checkpoint
                                latest_failed = checkpoints_info.get("latest", {}).get("failed")
                                if latest_failed:
                                    from db_operations import save_checkpoint
                                    cp_id = latest_failed.get("id")
                                    # 失败的checkpoint没有external_path，使用最后一次成功的路径
                                    latest_completed = checkpoints_info.get("latest", {}).get("completed")
                                    cp_path = latest_completed.get("external_path") if latest_completed else None
                                    cp_status = latest_failed.get("status", "FAILED")
                                    trigger_time = int(latest_failed.get("trigger_timestamp", 0))
                                    finish_time = int(latest_failed.get("failure_timestamp", 0)) if "failure_timestamp" in latest_failed else None
                                    checkpoint_size = int(latest_failed.get("checkpointed_size", 0)) if "checkpointed_size" in latest_failed else None
                                    duration = int(latest_failed.get("end_to_end_duration", 0)) if "end_to_end_duration" in latest_failed else None
                                    failure_message = latest_failed.get("failure_message", "")
                                    if cp_path:
                                        save_checkpoint(
                                            job_id=job_id,
                                            checkpoint_id=cp_id,
                                            checkpoint_path=cp_path,
                                            trigger_time=trigger_time,
                                            finish_time=finish_time or trigger_time,
                                            status=cp_status,
                                            checkpoint_size=checkpoint_size,
                                            duration=duration
                                        )
                                        logger.info(f"✅ 已同步失败作业 {job_id} 的Checkpoint: #{cp_id}, 失败原因: {failure_message}")
                                    else:
                                        logger.warning(f"⚠️ 作业 {job_id} 没有可用的Checkpoint路径，保留数据库已有记录")
                                else:
                                    logger.warning(f"⚠️ 作业 {job_id} 没有失败的Checkpoint记录，保留数据库已有记录")
                            else:
                                logger.warning(f"⚠️ 无法从Flink获取Checkpoint信息，保留数据库已有记录")
                        except Exception as api_err:
                            logger.warning(f"⚠️ 获取Flink Checkpoint失败: {api_err}，保留数据库已有记录")
                    except Exception as e:
                        logger.warning(f"更新作业 {job_id} 状态失败: {e}")
        
        # 动态更新运行时长
        current_time_ms = int(time.time() * 1000)
        for job in all_jobs:
            if job.get("state") == "RUNNING" and job.get("start_time"):
                try:
                    start_time = int(job.get("start_time"))
                    if start_time > 0:
                        job["duration"] = current_time_ms - start_time
                except (ValueError, TypeError):
                    pass
        
        # 按创建时间倒序排序
        sorted_jobs = sorted(
            all_jobs,
            key=lambda x: x.get("created_at") or "",
            reverse=True
        )
        
        # 分页
        total = len(sorted_jobs)
        paginated_jobs = sorted_jobs[offset:offset + limit]
        
        logger.info(f"返回 {len(paginated_jobs)} 条运行记录（总共 {total} 条）")
        return {
            "jobs": paginated_jobs,
            "total": total,
            "offset": offset,
            "limit": limit
        }
    except Exception as e:
        logger.error(f"Failed to get job runs: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/jobs/history", tags=["作业管理"])
async def get_job_history(limit: int = 50):
    """获取历史作业列表（按作业名称去重，每个作业名称只保留最新记录）"""
    logger.info(f"=== 正在请求历史作业列表 (limit={limit}) ===")
    try:
        # 从数据库获取所有作业
        all_jobs = get_all_jobs(state=None, limit=limit)
        
        # 从 Flink 获取正在运行的作业列表
        flink_running_jobs = []
        flink_job_map = {}
        try:
            jobs_overview = await flink_client.get_jobs_overview()
            flink_jobs = jobs_overview.get("jobs", [])
            flink_running_jobs = [job["id"] for job in flink_jobs if job.get("status") == "RUNNING"]
            flink_job_map = {job["id"]: job for job in flink_jobs}
            logger.info(f"Flink 中有 {len(flink_running_jobs)} 个 RUNNING 作业")
        except Exception as e:
            logger.warning(f"获取 Flink 作业列表失败: {e}")
        
        # 同步状态
        for job in all_jobs:
            job_id = job.get("job_id")
            if job_id in flink_job_map:
                flink_info = flink_job_map[job_id]
                flink_status = flink_info.get("status")
                if flink_status in ["RUNNING", "FINISHED", "FAILED", "CANCELED", "RESTARTING"]:
                    job["state"] = flink_status
                if not job.get("start_time") and flink_info.get("start-time"):
                    job["start_time"] = flink_info.get("start-time")
            else:
                # 只有当数据库中状态为RUNNING时，才更新为FAILED并同步checkpoint
                if job.get("state") == "RUNNING":
                    job["state"] = "FAILED"
                    try:
                        update_job_state(job_id, "FAILED")
                        # 作业失败时，从Flink获取失败的checkpoint信息
                        logger.info(f"⚠️ 作业 {job_id} 已失败，正在获取失败的Checkpoint信息...")
                        try:
                            checkpoints_info = await flink_client.get_job_checkpoints(job_id)
                            if checkpoints_info:
                                # 从latest.failed获取失败的checkpoint
                                latest_failed = checkpoints_info.get("latest", {}).get("failed")
                                if latest_failed:
                                    from db_operations import save_checkpoint
                                    cp_id = latest_failed.get("id")
                                    # 失败的checkpoint没有external_path，使用最后一次成功的路径
                                    latest_completed = checkpoints_info.get("latest", {}).get("completed")
                                    cp_path = latest_completed.get("external_path") if latest_completed else None
                                    cp_status = latest_failed.get("status", "FAILED")
                                    trigger_time = int(latest_failed.get("trigger_timestamp", 0))
                                    finish_time = int(latest_failed.get("failure_timestamp", 0)) if "failure_timestamp" in latest_failed else None
                                    checkpoint_size = int(latest_failed.get("checkpointed_size", 0)) if "checkpointed_size" in latest_failed else None
                                    duration = int(latest_failed.get("end_to_end_duration", 0)) if "end_to_end_duration" in latest_failed else None
                                    failure_message = latest_failed.get("failure_message", "")
                                    if cp_path:
                                        save_checkpoint(
                                            job_id=job_id,
                                            checkpoint_id=cp_id,
                                            checkpoint_path=cp_path,
                                            trigger_time=trigger_time,
                                            finish_time=finish_time or trigger_time,
                                            status=cp_status,
                                            checkpoint_size=checkpoint_size,
                                            duration=duration
                                        )
                                        logger.info(f"✅ 已同步失败作业 {job_id} 的Checkpoint: #{cp_id}, 失败原因: {failure_message}")
                                    else:
                                        logger.warning(f"⚠️ 作业 {job_id} 没有可用的Checkpoint路径，保留数据库已有记录")
                                else:
                                    logger.warning(f"⚠️ 作业 {job_id} 没有失败的Checkpoint记录，保留数据库已有记录")
                            else:
                                logger.warning(f"⚠️ 无法从Flink获取Checkpoint信息，保留数据库已有记录")
                        except Exception as api_err:
                            logger.warning(f"⚠️ 获取Flink Checkpoint失败: {api_err}，保留数据库已有记录")
                    except Exception as e:
                        logger.warning(f"更新作业 {job_id} 状态失败: {e}")
        
        # 按作业名称去重，每个作业名称只保留最新记录
        # 排序优先级：结束时间 > 开始时间
        def get_job_sort_time(job):
            end_time = job.get("end_time")
            start_time = job.get("start_time")
            # 统一转换为字符串进行比较
            if end_time:
                return str(end_time)
            elif start_time:
                return str(start_time)
            return "0"
        
        job_name_map = {}
        for job in all_jobs:
            job_name = job.get("job_name") or job.get("flink_job_name") or job.get("job_id")
            if job_name not in job_name_map:
                job_name_map[job_name] = job
            else:
                existing_job = job_name_map[job_name]
                existing_time = get_job_sort_time(existing_job)
                current_time = get_job_sort_time(job)
                if current_time > existing_time:
                    job_name_map[job_name] = job

        history_jobs = list(job_name_map.values())
        
        # 动态更新运行时长
        current_time_ms = int(time.time() * 1000)
        for job in history_jobs:
            if job.get("state") == "RUNNING" and job.get("start_time"):
                try:
                    start_time = int(job.get("start_time"))
                    if start_time > 0:
                        job["duration"] = current_time_ms - start_time
                except (ValueError, TypeError):
                    pass
        
        # 按结束时间倒序排序（没有结束时间则按开始时间）
        sorted_jobs = sorted(
            history_jobs,
            key=lambda x: get_job_sort_time(x),
            reverse=True
        )
        
        logger.info(f"返回 {len(sorted_jobs)} 个历史作业（从 {len(all_jobs)} 个作业中按名称去重）")
        return sorted_jobs
    except Exception as e:
        logger.error(f"Failed to get history jobs: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/jobs/history/{job_id}", tags=["作业管理"])
async def delete_history_job(job_id: str):
    """删除历史作业记录"""
    try:
        from db_operations import delete_job
        success = delete_job(job_id)
        if success:
            logger.info(f"✅ 已删除作业记录: {job_id}")
            return {"status": "success", "message": f"作业 {job_id} 已删除"}
        else:
            raise HTTPException(status_code=404, detail=f"作业 {job_id} 不存在")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除作业失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/jobs/history/{job_id}/checkpoints", tags=["作业管理"])
async def get_job_checkpoints_api(job_id: str):
    """获取作业的所有Checkpoint记录"""
    try:
        from db_operations import get_all_checkpoints
        checkpoints = get_all_checkpoints(job_id=job_id, limit=100)
        logger.info(f"✅ 获取作业 {job_id} 的 {len(checkpoints)} 条Checkpoint记录")
        return {"checkpoints": checkpoints}
    except Exception as e:
        logger.error(f"获取Checkpoint列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/jobs/history/{job_id}/restart", tags=["作业管理"])
async def restart_history_job(job_id: str, req: ResumeJobRequest):
    """从历史作业重新启动"""
    try:
        # 使用URL中的job_id，请求体中的job_id会被忽略
        job_id_to_restart = job_id
        
        # 从数据库获取作业信息
        db_job = get_job(job_id_to_restart)
        
        if not db_job:
            raise HTTPException(
                status_code=404,
                detail={"error": "Job not found", "message": f"Cannot find job {job_id_to_restart} in database"}
            )
        
        sql_text = db_job.get("sql_text")
        job_name = db_job.get("job_name")
        flink_job_name = db_job.get("flink_job_name") # 获取原始 Flink Job Name
        parallelism = db_job.get("parallelism", 1)
        
        # 调试日志：检查SQL内容
        logger.info(f"=== 恢复历史作业: {job_name} ===")
        logger.info(f"Job ID: {job_id_to_restart}")
        logger.info(f"SQL长度: {len(sql_text) if sql_text else 0}")
        logger.info(f"SQL前200字符: {sql_text[:200] if sql_text else 'NULL'}")
        logger.info(f"是否包含INSERT: {'INSERT' in sql_text.upper() if sql_text else False}")
        
        # 如果SQL为空或没有INSERT语句，尝试从文件中读取
        if not sql_text or 'INSERT' not in sql_text.upper():
            logger.warning(f"⚠️ SQL为空或没有INSERT语句，尝试从文件中读取")
            sql_file_path = SQL_FILES_DIR / f"{job_id_to_restart}.sql"
            
            if sql_file_path.exists():
                sql_text = sql_file_path.read_text(encoding="utf-8")
                logger.info(f"✅ 从文件中读取到SQL: {sql_file_path}")
                logger.info(f"SQL长度: {len(sql_text)}")
                logger.info(f"是否包含INSERT: {'INSERT' in sql_text.upper()}")
            else:
                logger.warning(f"⚠️ SQL文件不存在: {sql_file_path}")
                
                # 尝试根据作业名称查找其他作业的SQL
                all_jobs = get_all_jobs(limit=100)
                for other_job in all_jobs:
                    if other_job.get("job_name") == job_name and other_job.get("sql_text"):
                        if 'INSERT' in other_job["sql_text"].upper():
                            sql_text = other_job["sql_text"]
                            logger.info(f"✅ 从相同名称的作业中找到SQL: {other_job.get('job_id')}")
                            break
                
                # 如果还是没找到，尝试遍历sql_jobs目录
                if not sql_text or 'INSERT' not in sql_text.upper():
                    for sql_file in SQL_FILES_DIR.glob("*.sql"):
                        content = sql_file.read_text(encoding="utf-8")
                        if 'INSERT' in content.upper():
                            name_file = SQL_FILES_DIR / f"{sql_file.stem}_name.txt"
                            if name_file.exists():
                                saved_name = name_file.read_text(encoding="utf-8").strip()
                                if saved_name == job_name:
                                    sql_text = content
                                    logger.info(f"✅ 根据作业名称匹配到SQL文件: {sql_file}")
                                    break
        
        # 再次检查SQL是否包含INSERT语句
        if not sql_text or 'INSERT' not in sql_text.upper():
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "Invalid SQL",
                    "message": "作业SQL中没有INSERT语句，无法恢复",
                    "hint": "请检查作业是否通过可视化配置正确提交，或者手动添加INSERT语句"
                }
            )
        
        # 获取Savepoint路径（优先使用请求中的）
        savepoint_path = req.savepoint_path
        savepoint_timestamp = req.timestamp
        
        # 如果没有指定savepoint，尝试从checkpoint恢复
        checkpoint_path_to_restore = req.checkpoint_path
        
        if not checkpoint_path_to_restore and not savepoint_path:
            # 优先尝试从checkpoint恢复，如果没有则从savepoint恢复
            from db_operations import get_latest_checkpoint
            latest_checkpoint = get_latest_checkpoint(job_id_to_restart)
            
            if latest_checkpoint:
                checkpoint_path_to_restore = latest_checkpoint.get("checkpoint_path")
                logger.info(f"✅ 找到最新Checkpoint: {checkpoint_path_to_restore}")
            else:
                # 没有checkpoint，尝试从savepoint恢复
                latest_savepoint = get_latest_savepoint(job_id_to_restart)
                if latest_savepoint:
                    savepoint_path = latest_savepoint.get("savepoint_path")
                    savepoint_timestamp = latest_savepoint.get("timestamp")
                    logger.info(f"✅ 找到最新Savepoint: {savepoint_path}")
        
        # 如果没有指定timestamp，尝试从db_job获取（如果路径匹配）
        if savepoint_path and not savepoint_timestamp:
            if db_job and savepoint_path == db_job.get("savepoint_path"):
                 savepoint_timestamp = db_job.get("savepoint_timestamp")
            
            # 补救措施：如果还没获取到，尝试从 savepoints 表获取
            if not savepoint_timestamp and savepoint_path:
                try:
                    from db_operations import db_manager, FlinkSavepoint
                    session = db_manager.get_session()
                    try:
                        sp_record = session.query(FlinkSavepoint).filter(
                            FlinkSavepoint.savepoint_path == savepoint_path
                        ).order_by(FlinkSavepoint.timestamp.desc()).first()
                        if sp_record:
                            savepoint_timestamp = sp_record.timestamp
                            logger.info(f"✅ 从 savepoints 表补全 timestamp: {savepoint_timestamp}")
                    finally:
                        session.close()
                except Exception as e:
                    logger.warning(f"尝试从 savepoints 表获取 timestamp 失败: {e}")

        # 确保 timestamp 是 int
        if savepoint_timestamp and isinstance(savepoint_timestamp, str):
            try:
                savepoint_timestamp = int(savepoint_timestamp)
            except:
                savepoint_timestamp = None

        # 从配置表获取checkpoint路径并构建作业专属子目录（用于新checkpoint）
        from db_operations import get_flink_config
        base_checkpoint_path = get_flink_config("checkpoint_path")
        checkpoint_path = None
        
        if base_checkpoint_path:
            # 为作业创建独立的子目录
            import re
            safe_job_name = job_name.replace('/', '_').replace('\\', '_').replace(':', '_')
            checkpoint_path = base_checkpoint_path.rstrip('/') + '/' + safe_job_name
            logger.info(f"✅ 从配置表获取到Checkpoint路径: {base_checkpoint_path}")
            logger.info(f"✅ 为作业 '{job_name}' 创建子目录: {checkpoint_path}")
        else:
            logger.warning(f"⚠️ 未配置Checkpoint路径")
        
        # 如果指定了从checkpoint恢复，使用checkpoint路径作为savepoint路径
        if checkpoint_path_to_restore:
            logger.info(f"🔄 从Checkpoint恢复作业: {checkpoint_path_to_restore}")
            savepoint_path = checkpoint_path_to_restore
            savepoint_timestamp = None  # checkpoint不需要timestamp

        # 使用SQL Gateway重新提交作业
        result = await flink_client.submit_sql_job(
            sql=sql_text,
            job_name=job_name,
            parallelism=parallelism,
            savepoint_path=savepoint_path,
            checkpoint_path=checkpoint_path
        )
        
        logger.info(f"submit_sql_job returned: {result}")
        
        job_id_new = result.get("jobid")
        operation_handle = result.get("operationHandle")
        
        if job_id_new:
            # 成功获取到作业ID
            logger.info(f"Job restarted successfully, new job ID: {job_id_new}")
            
            # 获取当前时间作为开始时间
            start_time = int(time.time() * 1000)

            # 保存新作业到数据库
            save_job(
                job_id=job_id_new,
                job_name=job_name,
                sql_text=sql_text,
                parallelism=parallelism,
                flink_job_name=flink_job_name,
                resumed_from_job_id=job_id_to_restart,
                savepoint_path=savepoint_path,
                savepoint_timestamp=savepoint_timestamp,
                start_time=start_time,
                state="RUNNING"
            )
            
            # 记录操作日志
            log_operation(
                job_id=job_id_new,
                operation_type="RESTART",
                operation_details={
                    "old_job_id": job_id_to_restart,
                    "savepoint_path": savepoint_path,
                    "savepoint_timestamp": savepoint_timestamp,
                    "source": "history"
                }
            )

            # 如果是从 savepoint 启动，尝试记录 savepoint 使用情况
            if savepoint_path:
                try:
                    from db_operations import save_savepoint
                    # 尝试从路径中提取 savepoint id，或者生成一个
                    sp_id = f"sp_{int(time.time())}"
                    save_savepoint(
                        job_id=job_id_new,
                        savepoint_id=sp_id,
                        savepoint_path=savepoint_path,
                        job_state="RESTORED",
                        timestamp=savepoint_timestamp
                    )
                except Exception as sp_err:
                    logger.warning(f"记录 Savepoint 使用情况失败: {sp_err}")
            
            return {
                "job_id": job_id_new,
                "old_job_id": job_id_to_restart,
                "savepoint_path": savepoint_path,
                "savepoint_timestamp": savepoint_timestamp,
                "status": "RUNNING",
                "message": "Job restarted from history"
            }
        elif operation_handle:
            # 作业正在启动中，等待一下尝试获取 job_id
            logger.info(f"Job is starting, operation_handle: {operation_handle}, waiting for job_id...")
            
            import asyncio
            job_id_new = None
            
            # 等待并尝试获取 job_id
            for i in range(5):  # 最多等待 5 秒
                await asyncio.sleep(1)
                try:
                    # 从 Flink REST API 获取作业列表
                    jobs_overview = await flink_client.get_jobs_overview()
                    flink_jobs = jobs_overview.get("jobs", [])
                    
                    # 查找 RUNNING 状态的新作业
                    for job in flink_jobs:
                        if job.get("status") == "RUNNING":
                            potential_job_id = job.get("id")
                            # 检查这个 job_id 是否已经在数据库中
                            existing_job = get_job(potential_job_id)
                            if not existing_job:
                                job_id_new = potential_job_id
                                logger.info(f"找到新启动的作业: {job_id_new}")
                                break
                    
                    if job_id_new:
                        break
                except Exception as e:
                    logger.warning(f"等待获取 job_id 失败: {e}")
            
            if job_id_new:
                # 获取当前时间作为开始时间
                start_time = int(time.time() * 1000)
                
                # 保存新作业到数据库
                save_job(
                    job_id=job_id_new,
                    job_name=job_name,
                    sql_text=sql_text,
                    parallelism=parallelism,
                    flink_job_name=flink_job_name,
                    resumed_from_job_id=job_id_to_restart,
                    savepoint_path=savepoint_path,
                    savepoint_timestamp=savepoint_timestamp,
                    start_time=start_time,
                    state="RUNNING"
                )
                logger.info(f"✅ 新作业已保存到数据库: {job_id_new} - {job_name}")
                
                return {
                    "job_id": job_id_new,
                    "old_job_id": job_id_to_restart,
                    "savepoint_path": savepoint_path,
                    "status": "RUNNING",
                    "message": "Job restarted from history"
                }
            else:
                # 未能获取到 job_id，但作业可能已经启动
                logger.warning(f"未能获取到 job_id，但作业可能已启动")
                raise HTTPException(
                    status_code=202,
                    detail={
                        "error": "Job is starting",
                        "message": "Job submitted but still starting, please refresh job list later",
                        "operation_handle": operation_handle,
                        "job_name": job_name,
                        "hint": "Job will appear in job list in a few seconds"
                    }
                )
        else:
            logger.error(f"Unexpected result: {result}")
            raise HTTPException(
                status_code=500,
                detail={
                    "error": "Failed to restart job",
                    "message": "Could not get new job ID or operation handle",
                    "debug_info": result
                }
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to restart history job: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Failed to restart history job",
                "message": str(e)
            }
        )


@app.get("/api/jobs", tags=["作业管理"])
async def get_jobs():
    """获取实时作业列表（从Flink接口获取，只显示RUNNING状态的作业）"""
    try:
        # 从 Flink 获取作业列表
        jobs_overview = await flink_client.get_jobs_overview()
        flink_jobs = jobs_overview.get("jobs", [])
        
        logger.info(f"从 Flink 获取到 {len(flink_jobs)} 个作业")
        
        # 转换字段名以兼容前端
        converted_jobs = []
        for flink_job in flink_jobs:
            job_id = flink_job.get("id")
            state = flink_job.get("status")
            
            # 只显示 RUNNING 状态的作业（实时作业）
            if state != "RUNNING":
                continue
            
            # 从数据库获取作业信息（确保能获取到，不受列表数量限制）
            db_job = get_job(job_id)
            
            # 用户配置的作业名称（从数据库）
            user_job_name = db_job.get("job_name") if db_job else None
            
            # 优先从 Flink 获取实时信息（满足用户需求：其他信息是从flink接口获取的）
            # 1. 尝试从列表信息中获取
            start_time = flink_job.get("start-time", 0)
            end_time = flink_job.get("end-time", 0)
            duration = flink_job.get("duration", 0)
            flink_job_name = flink_job.get("name", job_id)

            # 2. 如果列表信息不全（例如使用了简版 /jobs 接口），则获取详情
            if start_time == 0:
                try:
                    job_detail = await flink_client.get_job_detail(job_id)
                    flink_job_name = job_detail.get("name", job_id)
                    start_time = job_detail.get("start-time", 0) or 0
                    end_time = job_detail.get("end-time", 0) or 0
                    duration = job_detail.get("duration", 0) or 0
                    logger.debug(f"从 Flink 详情获取: {job_id} - {flink_job_name}, start_time={start_time}")
                except Exception as e:
                    logger.warning(f"获取作业 {job_id} 详情失败: {e}")

            if db_job:
                # 数据库只提供用户配置的静态信息
                user_job_name = db_job.get("job_name")
                savepoint_path = db_job.get("savepoint_path")
                sql_text = db_job.get("sql_text")
                logger.debug(f"From DB: {job_id} - user_name: {user_job_name}")
            else:
                # 作业不在数据库中
                logger.info(f"作业 {job_id} 不在数据库中")
                savepoint_path = None
                sql_text = None
            
            # 检查并更新数据库中的 flink_job_name
            actual_flink_name = flink_job.get("name", job_id)
            if db_job:
                db_flink_name = db_job.get("flink_job_name")
                # 如果数据库中没有 flink_job_name 或者与实际不一致（且实际名称不是job_id），则更新
                if (not db_flink_name or db_flink_name != actual_flink_name) and actual_flink_name != job_id:
                     logger.info(f"同步 flink_job_name: {job_id} - {db_flink_name} -> {actual_flink_name}")
                     # 异步更新数据库（这里简单起见直接调用同步方法，因为是少量操作）
                     try:
                         from db_operations import save_job
                         # 只更新 flink_job_name，其他保持不变
                         # 注意：这里需要传入 job_name，否则无法定位（如果按名称更新）或者会更新为空
                         # 由于 save_job 现在的逻辑比较复杂，我们最好只更新 job_id 对应的记录
                         # 这里为了安全，我们只在 db_job 存在时更新
                         user_job_name_for_update = db_job.get("job_name")
                         if user_job_name_for_update:
                            save_job(
                                job_id=job_id,
                                job_name=user_job_name_for_update,
                                sql_text="", # 不更新 SQL
                                flink_job_name=actual_flink_name,
                                parallelism=None, # 不更新并行度
                                update_by_name=False # 强制按 ID 更新，防止副作用
                            )
                            # 更新内存中的 db_job 信息，以便后续逻辑使用最新值
                            db_job["flink_job_name"] = actual_flink_name
                            flink_job_name = actual_flink_name
                     except Exception as update_err:
                         logger.error(f"同步 flink_job_name 失败: {update_err}")

            # 动态计算运行时长：如果 Flink 没有返回 duration，则手动计算
            # 严格遵守用户要求：其他信息是从flink接口获取的
            if duration == 0 and start_time > 0:
                current_time_ms = int(time.time() * 1000)
                duration = current_time_ms - start_time
            
            # 根据用户要求，作业名称优先从 flink_job 表的 job_name 获取
            # display_name 用于前端展示，但为了兼容性，我们将分别返回 name (Flink名称) 和 user_job_name (用户名称)
            # display_name = user_job_name if user_job_name else flink_job_name

            converted_job = {
                "jid": job_id,
                "state": state,
                "name": flink_job_name,  # 恢复为 Flink 作业名称 (e.g. insert-into...)
                "flink_name": flink_job_name,  # 保留 Flink 原始名称
                "user_job_name": user_job_name,  # 用户配置的作业名称
                "sql_text": sql_text, # 作业SQL
                "start-time": start_time,
                "end-time": end_time,
                "duration": duration,
                "savepoint_path": savepoint_path
            }
            converted_jobs.append(converted_job)

        # 按 start_time 降序排序（最新的在前面），start_time 为 0 的排在最后
        # 使用安全的比较方式，避免 None 值导致错误
        try:
            converted_jobs.sort(key=lambda x: (x.get("start-time") or 0), reverse=True)
        except Exception as sort_err:
            logger.error(f"排序实时作业失败: {sort_err}")
        
        logger.info(f"返回 {len(converted_jobs)} 个实时作业（RUNNING 状态）")
        return converted_jobs
    except Exception as e:
        logger.error(f"获取作业列表失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/jobs/{job_id}", tags=["作业管理"])
async def get_job_detail(job_id: str):
    """获取作业详情（从Flink API直接返回）"""
    try:
        job_detail = await flink_client.get_job_detail(job_id)
        return job_detail
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/jobs/{job_id}/plan", tags=["作业管理"])
async def get_job_plan(job_id: str):
    """获取作业执行计划"""
    try:
        return await flink_client.get_job_plan(job_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/jobs/{job_id}/exceptions", tags=["作业管理"])
async def get_job_exceptions(job_id: str):
    """获取作业异常信息"""
    try:
        return await flink_client.get_job_exceptions(job_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/jobs/{job_id}/config", tags=["作业管理"])
async def get_job_config(job_id: str):
    """获取作业配置"""
    try:
        return await flink_client.get_job_config(job_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/jobs/{job_id}/db-detail", tags=["作业管理"])
async def get_job_db_detail(job_id: str):
    """获取数据库中的作业详情（不包含ID列）"""
    try:
        job = get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found in database")
        
        # 移除 id 字段
        if "id" in job:
            del job["id"]
            
        return job
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取作业数据库详情失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/jobs/{job_id}/cancel", tags=["作业管理"])
async def cancel_job(job_id: str):
    """取消作业"""
    import time
    try:
        await flink_client.cancel_job(job_id)
        
        # 获取当前时间作为结束时间
        end_time = int(time.time() * 1000)
        
        # 获取作业开始时间来计算运行时长
        db_job = get_job(job_id)
        duration = None
        if db_job:
            start_time = db_job.get("start_time")
            if start_time and start_time > 0:
                duration = end_time - start_time
                logger.info(f"作业 {job_id} 运行时长: {duration}ms")
        
        # 更新数据库中的作业状态
        update_job_state(job_id, "CANCELED", end_time=end_time, duration=duration)
        
        # 立即同步checkpoint状态到数据库
        try:
            logger.info(f"🔄 作业取消后同步Checkpoint状态: {job_id}")
            checkpoints_info = await flink_client.get_job_checkpoints(job_id)
            
            if checkpoints_info and "history" in checkpoints_info:
                history = checkpoints_info["history"]
                if history and len(history) > 0:
                    # 按id取最大的checkpoint（最新的）
                    latest_checkpoint = max(history, key=lambda x: x.get("id", 0))
                    cp_id = latest_checkpoint.get("id")
                    cp_path = latest_checkpoint.get("path") or latest_checkpoint.get("external_path")
                    cp_status = latest_checkpoint.get("status", "COMPLETED")
                    trigger_time = int(latest_checkpoint.get("trigger_timestamp", 0))
                    finish_time = int(latest_checkpoint.get("latest_ack_timestamp", trigger_time))
                    checkpoint_size = int(latest_checkpoint.get("checkpointed_size", 0)) if "checkpointed_size" in latest_checkpoint else None
                    cp_duration = int(latest_checkpoint.get("end_to_end_duration", 0)) if "end_to_end_duration" in latest_checkpoint else None
                    
                    if cp_path:
                        save_checkpoint(
                            job_id=job_id,
                            checkpoint_id=cp_id,
                            checkpoint_path=cp_path,
                            trigger_time=trigger_time,
                            finish_time=finish_time,
                            status=cp_status,
                            checkpoint_size=checkpoint_size,
                            duration=cp_duration
                        )
                        logger.info(f"✅ 已保存取消作业的Checkpoint: #{cp_id}")
        except Exception as e:
            logger.warning(f"⚠️ 同步取消作业的Checkpoint失败: {e}")
        
        # 记录操作日志
        log_operation(job_id=job_id, operation_type="CANCEL")
        
        return {"job_id": job_id, "status": "CANCELED"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/jobs/{job_id}/stop", tags=["作业管理"])
async def stop_job(job_id: str, req: SavepointRequest = SavepointRequest()):
    """暂停作业"""
    import time
    try:
        logger.info(f"=== 开始暂停作业: {job_id} ===")
        logger.info(f"请求参数: target_directory={req.target_directory}, withSavepoint={req.withSavepoint}")
        
        # 如果用户没有指定savepoint目录，从配置表读取默认路径
        target_directory = req.target_directory
        if req.withSavepoint and not target_directory:
            from db_operations import get_flink_config
            
            # 获取作业的用户定义名称
            job_name_file = SQL_FILES_DIR / f"{job_id}_name.txt"
            job_name = None
            logger.info(f"🔍 尝试读取作业名称文件: {job_name_file}")
            logger.info(f"🔍 文件是否存在: {job_name_file.exists()}")
            
            if job_name_file.exists():
                job_name = job_name_file.read_text(encoding='utf-8').strip()
                logger.info(f"✅ 从文件读取到作业名称: '{job_name}'")
            else:
                logger.warning(f"⚠️ 作业名称文件不存在: {job_name_file}")
                # 尝试从数据库获取作业名称
                db_job = get_job(job_id)
                if db_job:
                    job_name = db_job.get("job_name")
                    logger.info(f"✅ 从数据库获取到作业名称: '{job_name}'")
            
            base_savepoint_path = get_flink_config("savepoint_path")
            if base_savepoint_path:
                # 为每个作业创建独立的子目录（按作业名）
                # 直接使用中文作业名，不需要URL编码
                # 只替换路径分隔符和特殊字符
                if job_name:
                    safe_job_name = job_name.replace('/', '_').replace('\\', '_').replace(':', '_')
                    target_directory = base_savepoint_path.rstrip('/') + '/' + safe_job_name
                    logger.info(f"✅ 从配置表获取到Savepoint路径: {base_savepoint_path}")
                    logger.info(f"✅ 为作业 '{job_name}' 创建子目录: {target_directory}")
                else:
                    target_directory = base_savepoint_path
                    logger.info(f"✅ 从配置表获取到Savepoint路径: {base_savepoint_path}")
                    logger.warning(f"⚠️ 未找到作业名称，使用基础路径")
            else:
                logger.warning(f"⚠️ 配置表中未配置Savepoint路径，将使用Flink默认路径")
        
        try:
            if req.withSavepoint:
                logger.info(f"调用stop_job_with_savepoint，目录: {target_directory}")
                result = await flink_client.stop_job_with_savepoint(job_id, target_directory)
                
                logger.info(f"Flink API返回: {result}")
                
                # 保存savepoint路径到文件，以便后续恢复
                savepoint_path = result.get("savepoint_path")
                savepoint_id = result.get("savepoint_id", job_id)
                savepoint_timestamp = result.get("savepoint_timestamp")
                
                # 获取当前时间作为结束时间
                end_time = int(time.time() * 1000)
                
                # 如果没有从Flink获取到savepoint时间戳，使用当前时间
                if not savepoint_timestamp:
                    savepoint_timestamp = end_time
                    logger.info(f"⚠️ 使用当前时间作为 savepoint_timestamp: {savepoint_timestamp}")
                else:
                    logger.info(f"✅ 使用 Flink 返回的 savepoint_timestamp: {savepoint_timestamp}")
                
                # 获取作业开始时间来计算运行时长
                db_job = get_job(job_id)
                duration = None
                start_time = None
                
                if db_job:
                    start_time = db_job.get("start_time")
                
                # 如果数据库中没有 start_time，从 Flink 获取
                if not start_time or start_time <= 0:
                    try:
                        job_detail = await flink_client.get_job_detail(job_id)
                        start_time = job_detail.get("start-time", 0)
                        logger.info(f"从 Flink 获取作业开始时间: {start_time}")
                    except Exception as e:
                        logger.warning(f"获取作业详情失败: {e}")
                
                if start_time and start_time > 0:
                    duration = end_time - start_time
                    logger.info(f"作业 {job_id} 运行时长: {duration}ms")
                
                if savepoint_path:
                    savepoint_file = SQL_FILES_DIR / f"{job_id}_savepoint.txt"
                    savepoint_file.write_text(savepoint_path, encoding="utf-8")
                    logger.info(f"✅ Savepoint路径已保存到文件: {savepoint_file}")
                    
                    # 保存Savepoint信息到数据库
                    save_success = save_savepoint(
                        job_id=job_id,
                        savepoint_id=savepoint_id,
                        savepoint_path=savepoint_path,
                        job_state="FINISHED",
                        timestamp=savepoint_timestamp  # 使用Savepoint时间戳
                    )
                    if save_success:
                        logger.info(f"✅ Savepoint信息已保存到数据库: {savepoint_id}")
                    else:
                        logger.error(f"❌ 保存Savepoint到数据库失败，但继续更新作业状态")
                
                # 更新作业状态，同时保存 start_time 和 savepoint_path
                # 使用 update_job_state 直接更新所有信息
                update_success = update_job_state(
                    job_id=job_id, 
                    state="STOPPED", 
                    start_time=start_time, 
                    end_time=end_time, 
                    duration=duration,
                    savepoint_path=savepoint_path,
                    savepoint_timestamp=savepoint_timestamp
                )
                
                if update_success:
                    logger.info(f"✅ 作业状态已更新: {job_id} -> STOPPED, savepoint={savepoint_path}, timestamp={savepoint_timestamp}")
                    
                    # 再次验证数据库中的数据
                    saved_job = get_job(job_id)
                    if saved_job:
                        logger.info(f"🔍 数据库验证: path={saved_job.get('savepoint_path')}, ts={saved_job.get('savepoint_timestamp')}")
                else:
                    logger.error(f"❌ 更新作业状态失败")
                
                # 记录操作日志
                log_operation(
                    job_id=job_id,
                    operation_type="STOP",
                    operation_details={
                        "savepoint_path": savepoint_path,
                        "target_directory": req.target_directory
                    }
                )
                
                logger.info(f"=== 暂停作业完成: {job_id} ===")
                return result
            else:
                logger.info(f"作业 {job_id} 不带savepoint暂停")
                await flink_client.stop_job(job_id)
                
                # 获取当前时间作为结束时间
                end_time = int(time.time() * 1000)
                
                # 获取作业开始时间来计算运行时长
                db_job = get_job(job_id)
                duration = None
                start_time = None
                
                if db_job:
                    start_time = db_job.get("start_time")
                
                # 如果数据库中没有 start_time，从 Flink 获取
                if not start_time or start_time <= 0:
                    try:
                        job_detail = await flink_client.get_job_detail(job_id)
                        start_time = job_detail.get("start-time", 0)
                        logger.info(f"从 Flink 获取作业开始时间: {start_time}")
                    except Exception as e:
                        logger.warning(f"获取作业详情失败: {e}")
                
                if start_time and start_time > 0:
                    duration = end_time - start_time
                    logger.info(f"作业 {job_id} 运行时长: {duration}ms")
                
                # 更新作业状态，同时保存 start_time
                update_success = update_job_state(job_id, "STOPPED", start_time=start_time, end_time=end_time, duration=duration)
                if update_success:
                    logger.info(f"✅ 作业状态已更新: {job_id} -> STOPPED")
                else:
                    logger.error(f"❌ 更新作业状态失败")
                
                log_operation(job_id=job_id, operation_type="STOP")
                return {"job_id": job_id, "status": "STOPPED"}
                
        except Exception as flink_error:
            # 特殊处理：如果Flink报错说找不到作业，说明作业已经停止或不存在
            error_str = str(flink_error)
            if "404" in error_str or "NotFound" in error_str:
                logger.warning(f"Flink 中未找到作业 {job_id} (可能已停止): {flink_error}")
                # 强制更新数据库状态为 STOPPED
                update_job_state(job_id, "STOPPED", end_time=int(time.time() * 1000))
                logger.info(f"✅ 已强制更新数据库状态为 STOPPED: {job_id}")
                return {"job_id": job_id, "status": "STOPPED", "message": "Job not found in Flink, marked as STOPPED in DB"}
            else:
                raise flink_error
                
    except Exception as e:
        logger.error(f"暂停作业失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


# ============ 全局变量 ============
# 用于防止重复提交
_job_restart_locks = {}  # {job_id: timestamp}


async def check_and_set_job_lock(job_id: str) -> bool:
    """检查并设置作业锁，防止重复提交"""
    import time
    current_time = time.time()
    
    if job_id in _job_restart_locks:
        last_restart_time = _job_restart_locks[job_id]
        if current_time - last_restart_time < 30:  # 30秒内不允许重复提交
            logger.warning(f"作业 {job_id} 在30秒内已重启过，拒绝重复请求")
            return False
    
    _job_restart_locks[job_id] = current_time
    return True


@app.post("/api/jobs/{job_id}/restart", tags=["作业管理"])
async def restart_job(job_id: str, req: ResumeJobRequest):
    """重启作业（从savepoint恢复）"""
    # 检查防重复提交
    if not await check_and_set_job_lock(job_id):
        raise HTTPException(
            status_code=429,
            detail={
                "error": "Too many requests",
                "message": "作业正在恢复中，请勿重复点击"
            }
        )
    
    try:
        # 优先从数据库读取作业信息
        db_job = get_job(job_id)
        
        savepoint_timestamp = req.timestamp
        if savepoint_timestamp and isinstance(savepoint_timestamp, str):
            try:
                savepoint_timestamp = int(savepoint_timestamp)
            except:
                savepoint_timestamp = None
        
        sql_text = None
        original_job_name = None
        parallelism = 1
        
        if db_job:
            # 从数据库读取作业信息
            sql_text = db_job.get("sql_text")
            original_job_name = db_job.get("job_name")
            parallelism = db_job.get("parallelism", 1)
            logger.info(f"✅ 从数据库读取到作业信息: {job_id} - {original_job_name}")
            logger.info(f"SQL长度: {len(sql_text) if sql_text else 0}")
            
            # 检查SQL是否有效，如果为空或没有INSERT，尝试从文件读取
            if not sql_text or 'INSERT' not in sql_text.upper():
                logger.warning(f"⚠️ 数据库中的SQL为空或没有INSERT语句，尝试从文件读取")
                sql_file_path = SQL_FILES_DIR / f"{job_id}.sql"
                if sql_file_path.exists():
                    sql_text = sql_file_path.read_text(encoding="utf-8")
                    logger.info(f"✅ 从文件读取到SQL: {sql_file_path}")
                    logger.info(f"SQL长度: {len(sql_text)}")
                else:
                    logger.warning(f"⚠️ SQL文件不存在: {sql_file_path}")
                    # 尝试根据作业名称查找其他作业的SQL
                    from db_operations import get_all_jobs
                    all_jobs = get_all_jobs(limit=100)
                    for other_job in all_jobs:
                        if other_job.get("job_name") == original_job_name and other_job.get("sql_text"):
                            if 'INSERT' in other_job["sql_text"].upper():
                                sql_text = other_job["sql_text"]
                                logger.info(f"✅ 从相同名称的作业中找到SQL: {other_job.get('job_id')}")
                                break
                    
                    # 如果还是没找到，尝试遍历sql_jobs目录按作业名称匹配
                    if not sql_text or 'INSERT' not in sql_text.upper():
                        logger.info(f"🔍 遍历sql_jobs目录查找作业名称: {original_job_name}")
                        for sql_file in SQL_FILES_DIR.glob("*.sql"):
                            content = sql_file.read_text(encoding="utf-8")
                            if 'INSERT' in content.upper():
                                name_file = SQL_FILES_DIR / f"{sql_file.stem}_name.txt"
                                if name_file.exists():
                                    saved_name = name_file.read_text(encoding="utf-8").strip()
                                    logger.debug(f"  检查: {sql_file.name} -> {saved_name}")
                                    if saved_name == original_job_name:
                                        sql_text = content
                                        logger.info(f"✅ 根据作业名称匹配到SQL文件: {sql_file}")
                                        break
                    
                    # 终极备用方案：使用最新的包含INSERT的SQL文件
                    if not sql_text or 'INSERT' not in sql_text.upper():
                        logger.warning(f"⚠️ 按名称未找到，尝试使用最新的SQL文件")
                        sql_files = list(SQL_FILES_DIR.glob("*.sql"))
                        sql_files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
                        for sql_file in sql_files:
                            content = sql_file.read_text(encoding="utf-8")
                            if 'INSERT' in content.upper():
                                sql_text = content
                                logger.info(f"✅ 使用最新的SQL文件: {sql_file.name}")
                                # 同时读取对应的作业名称
                                name_file = SQL_FILES_DIR / f"{sql_file.stem}_name.txt"
                                if name_file.exists():
                                    original_job_name = name_file.read_text(encoding="utf-8").strip()
                                break
        else:
            # 降级到从文件读取
            sql_file_path = SQL_FILES_DIR / f"{job_id}.sql"
            
            if not sql_file_path.exists():
                # 尝试从其他可能的文件名读取
                alternate_files = list(SQL_FILES_DIR.glob("*.sql"))
                if alternate_files:
                    latest_sql = max(alternate_files, key=lambda f: f.stat().st_mtime)
                    logger.warning(f"未找到 {job_id}.sql，使用最近修改的文件: {latest_sql.name}")
                    sql_file_path = latest_sql
                else:
                    raise HTTPException(
                        status_code=404,
                        detail={
                            "error": "未找到作业信息",
                            "message": f"无法从数据库或文件找到作业 {job_id} 的信息。"
                        }
                    )
            
            sql_text = sql_file_path.read_text(encoding="utf-8")
            logger.info(f"✅ 从文件读取到作业SQL: {sql_file_path}")
            
            # 读取作业名称
            name_file_path = SQL_FILES_DIR / f"{job_id}_name.txt"
            if name_file_path.exists():
                original_job_name = name_file_path.read_text(encoding="utf-8").strip()
        
        # 如果仍然没有找到作业名称，使用job_id
        if not original_job_name:
            original_job_name = job_id
            logger.warning(f"未找到作业名称，使用job_id: {job_id}")
        
        # 获取Savepoint路径（优先使用请求中的，否则从数据库查询最新的）
        savepoint_path = req.savepoint_path
        if not savepoint_path:
            # 获取Savepoint路径（优先使用请求中的，否则从数据库查询最新的）
            latest_savepoint = get_latest_savepoint(job_id)
            if latest_savepoint:
                savepoint_path = latest_savepoint.get("savepoint_path")
                if not req.timestamp:
                    savepoint_timestamp = latest_savepoint.get("timestamp")
                logger.info(f"✅ 从数据库获取到最新Savepoint: {savepoint_path}")
            else:
                logger.warning(f"⚠️ 数据库中未找到 job_id={job_id} 的Savepoint")
                
                # 尝试搜索所有savepoints，查找可能匹配的
                all_savepoints = get_savepoints(limit=100)
                logger.info(f"数据库中所有Savepoint数量: {len(all_savepoints)}")
                for sp in all_savepoints:
                    logger.debug(f"  - Savepoint: job_id={sp.get('job_id')}, path={sp.get('savepoint_path')}")
                
                # 降级到从文件读取
                savepoint_file = SQL_FILES_DIR / f"{job_id}_savepoint.txt"
                if savepoint_file.exists():
                    savepoint_path = savepoint_file.read_text(encoding="utf-8").strip()
                    logger.info(f"✅ 从文件读取到Savepoint: {savepoint_path}")
                else:
                    logger.warning(f"⚠️ 文件也不存在: {savepoint_file}")
                    
                    # 遍历所有savepoint文件
                    for sp_file in SQL_FILES_DIR.glob("*_savepoint.txt"):
                        logger.debug(f"  - 发现savepoint文件: {sp_file.name}")
        
        # 如果还是找不到savepoint，警告但允许继续（不使用savepoint启动）
        if not savepoint_path:
            logger.warning(f"⚠️ 未找到Savepoint，将不使用Savepoint正常启动作业")
            logger.info(f"作业信息: job_id={job_id}, job_name={original_job_name}")
            
            # 如果用户明确请求使用savepoint（请求中有路径但无效），才抛出错误
            # 否则允许正常启动
            # if req.savepoint_path:  # 用户明确指定了但没找到
            #     raise HTTPException(...)
        
        logger.info(f"🔄 开始恢复作业: {original_job_name}")
        logger.info(f"📍 Savepoint路径: {savepoint_path}")
        
        # 从配置表获取checkpoint路径并构建作业专属子目录
        from db_operations import get_flink_config
        base_checkpoint_path = get_flink_config("checkpoint_path")
        checkpoint_path = None
        
        if base_checkpoint_path:
            # 为作业创建独立的子目录
            import re
            safe_job_name = original_job_name.replace('/', '_').replace('\\', '_').replace(':', '_')
            checkpoint_path = base_checkpoint_path.rstrip('/') + '/' + safe_job_name
            logger.info(f"✅ 从配置表获取到Checkpoint路径: {base_checkpoint_path}")
            logger.info(f"✅ 为作业 '{original_job_name}' 创建子目录: {checkpoint_path}")
        else:
            logger.warning(f"⚠️ 未配置Checkpoint路径")
        
        # 检查SQL是否包含INSERT语句
        if not sql_text or 'INSERT' not in sql_text.upper():
            logger.error(f"❌ SQL为空或没有INSERT语句，无法恢复作业")
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "Invalid SQL",
                    "message": "作业SQL中没有INSERT语句，无法恢复",
                    "hint": "请检查作业是否通过可视化配置正确提交"
                }
            )
        
        logger.info(f"SQL内容前200字符: {sql_text[:200]}")
        logger.info(f"是否包含INSERT: {'INSERT' in sql_text.upper()}")
        
        # 使用SQL Gateway重新提交作业，指定savepoint路径、checkpoint路径和原始作业名称
        logger.info(f"Calling submit_sql_job...")
        result = await flink_client.submit_sql_job(
            sql=sql_text,
            job_name=original_job_name,
            parallelism=parallelism,
            savepoint_path=savepoint_path,
            checkpoint_path=checkpoint_path
        )
        
        logger.info(f"submit_sql_job returned: {result}")
        logger.info(f"Result keys: {list(result.keys())}")
        
        # 注意：flink_client返回的键名是"operationHandle"（大写H）
        job_id_new = result.get("jobid")
        operation_handle = result.get("operationHandle")
        
        logger.info(f"Extracted jobid: {job_id_new}")
        logger.info(f"Extracted operationHandle: {operation_handle}")
        
        if job_id_new:
            # 成功获取到作业ID
            logger.info(f"Job resumed successfully, new job ID: {job_id_new}")
            
            # 获取当前时间作为开始时间
            start_time = int(time.time() * 1000)

            # 保存新作业到数据库
            save_job(
                job_id=job_id_new,
                job_name=original_job_name,
                sql_text=sql_text,
                parallelism=parallelism,
                resumed_from_job_id=job_id,
                savepoint_path=req.savepoint_path,
                savepoint_timestamp=savepoint_timestamp,
                start_time=start_time,
                state="RUNNING"
            )
            
            # 保存文件备份
            new_sql_file_path = SQL_FILES_DIR / f"{job_id_new}.sql"
            new_sql_file_path.write_text(sql_text, encoding="utf-8")
            
            new_name_file_path = SQL_FILES_DIR / f"{job_id_new}_name.txt"
            new_name_file_path.write_text(original_job_name, encoding="utf-8")
            
            # 更新旧作业状态
            update_job_state(job_id, "RESUMED")
            
            # 记录操作日志
            log_operation(
                job_id=job_id_new,
                operation_type="RESTART",
                operation_details={
                    "old_job_id": job_id,
                    "savepoint_path": savepoint_path
                }
            )
            
            return {
                "job_id": job_id_new,
                "old_job_id": job_id,
                "savepoint_path": req.savepoint_path,
                "savepoint_timestamp": req.timestamp,
                "status": "RUNNING",
                "message": "Job resumed from savepoint"
            }
        elif operation_handle:
            # 作业正在启动中，返回202状态码
            logger.info(f"Job is starting, operation_handle: {operation_handle}")
            raise HTTPException(
                status_code=202,
                detail={
                    "error": "Job is starting",
                    "message": "Job submitted but still starting, please refresh job list later",
                    "operation_handle": operation_handle,
                    "hint": "Job will appear in job list in a few seconds"
                }
            )
        else:
            # 既没有jobid也没有operation_handle
            logger.error(f"Unexpected result structure: {result}")
            raise HTTPException(
                status_code=500,
                detail={
                    "error": "Failed to resume job",
                    "message": "Could not get new job ID or operation handle",
                    "debug_info": result
                }
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"恢复作业失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail={
                "error": "恢复作业失败",
                "message": str(e)
            }
        )


@app.post("/api/jobs/{job_id}/savepoint", tags=["作业管理"])
async def trigger_savepoint(job_id: str, req: SavepointRequest):
    """触发 Savepoint"""
    try:
        result = await flink_client.trigger_savepoint(job_id, req.target_directory, req.cancel_job or False)
        return result
    except Exception as e:
        error_msg = str(e)
        if "state.savepoints.dir is not set" in error_msg:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "Savepoint 目录未配置",
                    "message": "Flink 集群未配置默认 Savepoint 目录，请填写 Savepoint 路径",
                    "hint": "在 flink-conf.yaml 中配置 state.savepoints.dir 或在此处填写具体的路径"
                }
            )
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/jobs/{job_id}/savepoints", tags=["作业管理"])
async def get_job_savepoints(job_id: str):
    """获取作业 Savepoints 列表"""
    try:
        # 优先从数据库读取Savepoint列表
        db_savepoints = get_savepoints(job_id=job_id)
        
        if db_savepoints:
            logger.info(f"✅ 从数据库读取到 {len(db_savepoints)} 个Savepoint")
            return {"savepoints": db_savepoints}
        
        # 降级到从文件读取savepoint路径
        savepoint_file = SQL_FILES_DIR / f"{job_id}_savepoint.txt"
        if savepoint_file.exists():
            savepoint_path = savepoint_file.read_text(encoding="utf-8").strip()
            if savepoint_path:
                logger.info(f"✅ 从文件读取到savepoint路径: {savepoint_path}")
                # 获取当前时间戳
                import time
                timestamp = int(time.time() * 1000)
                return {
                    "savepoints": [
                        {
                            "id": "saved",
                            "path": savepoint_path,
                            "timestamp": timestamp,
                            "status": "COMPLETED"
                        }
                    ]
                }
        
        # 最后尝试从Flink API获取
        return await flink_client.get_job_savepoints(job_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        raise HTTPException(status_code=500, detail=str(e))


# ============ 作业监控 ============
@app.get("/api/jobs/{job_id}/checkpoints", tags=["作业管理"])
async def get_job_checkpoints(job_id: str):
    """获取作业Checkpoints信息"""
    try:
        return await flink_client.get_job_checkpoints(job_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/jobs/{job_id}/savepoints", tags=["作业管理"])
async def get_job_savepoints(job_id: str):
    """获取作业保存的Savepoints信息（从数据库）"""
    try:
        from db_operations import get_savepoints
        return get_savepoints(job_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/jobs/{job_id}/metrics", tags=["作业监控"])
async def get_job_metrics(job_id: str, metrics: Optional[str] = Query(None, description="指标名称，逗号分隔")):
    """获取作业指标"""
    try:
        metric_list = metrics.split(",") if metrics else None
        return await flink_client.get_job_metrics(job_id, metric_list)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/jobs/{job_id}/vertices/{vertex_id}/metrics", tags=["作业监控"])
async def get_vertex_metrics(
    job_id: str,
    vertex_id: str,
    metrics: Optional[str] = Query(None, description="指标名称，逗号分隔")
):
    """获取算子指标"""
    try:
        metric_list = metrics.split(",") if metrics else None
        return await flink_client.get_vertex_metrics(job_id, vertex_id, metric_list)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============ Jar 管理 ============
@app.get("/api/jars", tags=["Jar管理"])
async def get_jars():
    """获取已上传的 Jar 列表"""
    try:
        return await flink_client.get_jars()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/jars/upload", tags=["Jar管理"])
async def upload_jar(file: UploadFile = File(...)):
    """上传 Jar 文件"""
    try:
        content = await file.read()
        result = await flink_client.upload_jar(content, file.filename)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/jars/{jar_id}", tags=["Jar管理"])
async def delete_jar(jar_id: str):
    """删除 Jar"""
    try:
        await flink_client.delete_jar(jar_id)
        return {"status": "deleted", "jar_id": jar_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/jars/{jar_id}/run", tags=["Jar管理"])
async def run_jar(jar_id: str, req: JarJobSubmitRequest):
    """运行 Jar 作业"""
    try:
        result = await flink_client.run_jar(
            jar_id=jar_id,
            entry_class=req.entry_class,
            program_args=req.program_args,
            parallelism=req.parallelism,
            savepoint_path=req.savepoint_path
        )
        job_id = result.get("jobid") or result.get("jobId")
        return JobSubmitResponse(job_name=req.job_name, flink_job_id=job_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/jars/{jar_id}/plan", tags=["Jar管理"])
async def get_jar_plan(
    jar_id: str,
    entry_class: Optional[str] = None,
    program_args: Optional[str] = None
):
    """获取 Jar 执行计划（不实际运行）"""
    try:
        return await flink_client.get_jar_plan(jar_id, entry_class, program_args)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============ SQL 作业（支持 SQL Gateway 和 SQL Runner Jar）============
@app.post("/api/sql/submit", tags=["SQL作业"])
async def submit_sql_job(req: SqlJobSubmitRequest):
    """
    提交 SQL 作业
    优先使用 SQL Gateway，如果不支持则回退到 SQL Runner Jar
    作业提交后立即写入数据库，状态为 RUNNING
    """
    from config import SQL_RUNNER_JAR_ID, SQL_GATEWAY_URL
    import traceback
    import time

    logger.info(f"=== 提交 SQL 作业 ===")
    logger.info(f"作业名称: {req.job_name}")
    logger.info(f"并行度: {req.parallelism}")
    logger.info(f"SQL 内容（前 200 字符）: {req.sql_text[:200]}")
    logger.info(f"SQL 长度: {len(req.sql_text)}")

    # 从配置表获取checkpoint路径
    from db_operations import get_flink_config
    base_checkpoint_path = get_flink_config("checkpoint_path")
    
    # 为每个作业创建独立的子目录（按作业名）
    # 直接使用中文作业名，不需要URL编码
    # 只替换路径分隔符和特殊字符
    import re
    safe_job_name = req.job_name.replace('/', '_').replace('\\', '_').replace(':', '_')
    
    checkpoint_path = None
    
    if base_checkpoint_path:
        checkpoint_path = base_checkpoint_path.rstrip('/') + '/' + safe_job_name
        logger.info(f"✅ 从配置表获取到Checkpoint路径: {base_checkpoint_path}")
        logger.info(f"✅ 为作业 '{req.job_name}' 创建子目录: {checkpoint_path}")
    
    if not checkpoint_path:
        logger.warning(f"⚠️ 未配置Checkpoint路径")

    # 1. 尝试使用 SQL Gateway 提交
    try:
        logger.info(f"尝试使用 SQL Gateway 提交作业")
        logger.info(f"SQL Gateway URL: {SQL_GATEWAY_URL}")

        result = await flink_client.submit_sql_job(
            sql=req.sql_text,
            job_name=req.job_name,
            parallelism=req.parallelism,
            checkpoint_path=checkpoint_path  # 只传递checkpoint_path
            # 注意：新建作业时不传递savepoint_path，savepoint_path只用于从savepoint恢复
        )

        logger.info(f"SQL Gateway 返回结果: {result}")

        if result.get("jobid"):
            # 立即保存作业信息到数据库
            job_id = result["jobid"]
            logger.info(f"✅ 获取到作业 ID: {job_id}")
            
            # 保存SQL文本到文件，以便后续恢复
            sql_file_path = SQL_FILES_DIR / f"{job_id}.sql"
            sql_file_path.write_text(req.sql_text, encoding="utf-8")
            logger.info(f"✅ SQL 已保存到: {sql_file_path}")
            
            # 保存用户配置的作业名称
            name_file_path = SQL_FILES_DIR / f"{job_id}_name.txt"
            name_file_path.write_text(req.job_name, encoding="utf-8")
            logger.info(f"✅ 作业名称已保存到: {name_file_path}")
            
            # 立即保存作业信息到数据库
            save_success = save_job(
                job_id=job_id,
                job_name=req.job_name,
                sql_text=req.sql_text,
                parallelism=req.parallelism
            )
            logger.info(f"✅ 作业信息已保存到数据库: {job_id}")
            
            # 获取作业详情，设置时间信息和Flink原始名称
            try:
                job_detail = await flink_client.get_job_detail(job_id)
                start_time = job_detail.get("start-time", int(time.time() * 1000))
                duration = job_detail.get("duration", 0)
                flink_job_name = job_detail.get("name", job_id)  # Flink原始作业名称
                logger.info(f"Flink作业详情: start_time={start_time}, duration={duration}, flink_name={flink_job_name}")
                
                # 更新作业状态为RUNNING，并设置时间信息和Flink原始名称
                update_job_state(job_id, "RUNNING", start_time=start_time, duration=duration, flink_job_name=flink_job_name)
                logger.info(f"✅ 作业状态已更新为RUNNING: {job_id}")
            except Exception as e:
                logger.warning(f"获取作业详情失败: {e}")
                # 使用当前时间作为开始时间
                start_time = int(time.time() * 1000)
                update_job_state(job_id, "RUNNING", start_time=start_time, duration=0)
            
            # 记录操作日志
            log_operation(
                job_id=job_id,
                operation_type="SUBMIT",
                operation_details={
                    "job_name": req.job_name,
                    "parallelism": req.parallelism
                }
            )
            
            logger.info(f"✅ SQL 作业提交成功，作业 ID: {job_id}")
            return JobSubmitResponse(
                job_name=req.job_name,
                flink_job_id=job_id
            )
        elif result.get("operationHandle"):
            # INSERT 语句已提交，作业正在启动中，等待获取真正的 job_id
            operation_handle = result.get("operationHandle")
            logger.info(f"✅ INSERT 语句已提交，等待获取 job_id: {operation_handle}")
            
            # 立即保存临时作业记录，确保"一旦提交就写入表"
            temp_job_id = f"temp_{operation_handle[:32]}"
            
            # 保存SQL文本到文件
            sql_file_path = SQL_FILES_DIR / f"{temp_job_id}.sql"
            sql_file_path.write_text(req.sql_text, encoding="utf-8")
            
            # 保存用户配置的作业名称
            name_file_path = SQL_FILES_DIR / f"{temp_job_id}_name.txt"
            name_file_path.write_text(req.job_name, encoding="utf-8")
            
            # 保存作业信息到数据库（状态为 INITIALIZING）
            save_job(
                job_id=temp_job_id,
                job_name=req.job_name,
                sql_text=req.sql_text,
                parallelism=req.parallelism,
                state="INITIALIZING"
            )
            logger.info(f"✅ 临时作业信息已保存到数据库: {temp_job_id}")

            import asyncio
            job_id_new = None
            
            # 等待并尝试获取 job_id
            for i in range(5):  # 最多等待 5 秒
                await asyncio.sleep(1)
                try:
                    # 从 Flink REST API 获取作业列表
                    jobs_overview = await flink_client.get_jobs_overview()
                    flink_jobs = jobs_overview.get("jobs", [])
                    
                    # 查找 RUNNING 状态的新作业
                    for job in flink_jobs:
                        if job.get("status") == "RUNNING":
                            potential_job_id = job.get("id")
                            # 检查这个 job_id 是否已经在数据库中
                            existing_job = get_job(potential_job_id)
                            if not existing_job:
                                job_id_new = potential_job_id
                                logger.info(f"找到新启动的作业: {job_id_new}")
                                break
                    
                    if job_id_new:
                        break
                except Exception as e:
                    logger.warning(f"等待获取 job_id 失败: {e}")
            
            if job_id_new:
                # 找到真正的 job_id，删除临时记录并保存正式记录
                logger.info(f"✅ 获取到真正的 job_id: {job_id_new}，替换临时记录")
                
                try:
                    from db_operations import delete_job
                    delete_job(temp_job_id)
                    logger.info(f"已删除临时记录: {temp_job_id}")
                except Exception as del_err:
                    logger.warning(f"删除临时记录失败: {del_err}")

                # 保存SQL文本到文件
                sql_file_path = SQL_FILES_DIR / f"{job_id_new}.sql"
                sql_file_path.write_text(req.sql_text, encoding="utf-8")
                logger.info(f"✅ SQL 已保存到: {sql_file_path}")
                
                # 保存用户配置的作业名称
                name_file_path = SQL_FILES_DIR / f"{job_id_new}_name.txt"
                name_file_path.write_text(req.job_name, encoding="utf-8")
                logger.info(f"✅ 作业名称已保存到: {name_file_path}")
                
                # 保存作业信息到数据库
                save_job(
                    job_id=job_id_new,
                    job_name=req.job_name,
                    sql_text=req.sql_text,
                    parallelism=req.parallelism
                )
                logger.info(f"✅ 作业信息已保存到数据库: {job_id_new} - {req.job_name}")
                
                # 获取作业详情，设置时间信息和Flink原始名称
                try:
                    job_detail = await flink_client.get_job_detail(job_id_new)
                    start_time = job_detail.get("start-time", int(time.time() * 1000))
                    flink_job_name = job_detail.get("name", job_id_new)
                    update_job_state(job_id_new, "RUNNING", start_time=start_time, flink_job_name=flink_job_name)
                    logger.info(f"Flink原始名称: {flink_job_name}")
                except Exception as e:
                    logger.warning(f"获取作业详情失败: {e}")
                    update_job_state(job_id_new, "RUNNING", start_time=int(time.time() * 1000))
                
                # 记录操作日志
                log_operation(
                    job_id=job_id_new,
                    operation_type="SUBMIT",
                    operation_details={
                        "job_name": req.job_name,
                        "parallelism": req.parallelism,
                        "operation_handle": operation_handle
                    }
                )
                
                return JobSubmitResponse(
                    job_name=req.job_name,
                    flink_job_id=job_id_new
                )
            else:
                # 未能获取到 job_id，保留临时 ID
                logger.warning(f"未能获取到 job_id，使用临时 ID: {temp_job_id}")
                
                return {
                    "job_name": req.job_name,
                    "flink_job_id": None,
                    "operation_handle": operation_handle,
                    "temp_job_id": temp_job_id,
                    "message": "作业已提交，正在启动中",
                    "status": "success"
                }
        elif result.get("operationHandle"):
            logger.info(f"✅ SQL 已成功执行（查询语句），操作句柄: {result['operationHandle']}")
            # SQL 已提交但不是 INSERT 语句
            return {
                "job_name": req.job_name,
                "flink_job_id": None,
                "operation_handle": result.get("operationHandle"),
                "message": "SQL 已成功执行（查询语句）",
                "status": "success"
            }
        elif result.get("status") == "no_insert":
            logger.info(f"⚠️ 没有找到 INSERT 语句")
            return {
                "job_name": req.job_name,
                "flink_job_id": None,
                "message": "SQL 已成功执行，但没有 INSERT 语句",
                "status": "success"
            }
        else:
            # 没有返回 jobid 或 operationHandle，尝试使用 Jar
            logger.warning(f"SQL Gateway 响应异常，尝试使用 Jar 方式: {result}")
            raise Exception("SQL Gateway 响应中缺少作业 ID")

    except Exception as e:
        error_msg = str(e)
        logger.error(f"❌ SQL Gateway 提交失败: {error_msg}")
        logger.error(f"错误详情: {traceback.format_exc()}")

        # SQL Gateway 不可用，尝试使用 Jar 方式
        if "404" in error_msg or "SQL Gateway" in error_msg or "Not Found" in error_msg or "没有返回作业 ID" in error_msg:
            logger.info("SQL Gateway 不可用，尝试使用 SQL Runner Jar 方式")

            # 2. 保存 SQL 文本到文件
            file_id = str(uuid.uuid4())
            sql_file_path = SQL_FILES_DIR / f"{file_id}.sql"
            sql_file_path.write_text(req.sql_text, encoding="utf-8")
            logger.info(f"SQL 文件已保存到: {sql_file_path}")

            # 3. 检查 SQL Runner Jar 配置
            if not SQL_RUNNER_JAR_ID or SQL_RUNNER_JAR_ID.strip() == "":
                logger.error("❌ SQL Runner Jar 未配置")
                raise HTTPException(
                    status_code=400,
                    detail={
                        "error": "SQL Gateway 不可用且 SQL Runner Jar 未配置",
                        "sql_gateway_error": str(e),
                        "message": "您的集群不支持 SQL Gateway，且未配置 SQL Runner Jar",
                        "sql_gateway_url": SQL_GATEWAY_URL,
                        "steps": [
                            "方式1：配置 SQL Runner Jar",
                            "- 访问 http://localhost:8000/api/jars 查看已上传的 Jar",
                            "- 复制 Jar ID 到 backend/config.py 的 SQL_RUNNER_JAR_ID",
                            "方式2：启用 Flink SQL Gateway",
                            "- 在 Flink 配置中启用 SQL Gateway (flink-sql-gateway)",
                            "方式3：复制 SQL 到 Flink SQL Client 执行"
                        ]
                    }
                )

            logger.info(f"使用 SQL Runner Jar 提交作业: {SQL_RUNNER_JAR_ID}")

            # 4. 使用 SQL Runner Jar 提交
            try:
                result = await flink_client.run_jar(
                    jar_id=SQL_RUNNER_JAR_ID,
                    program_args=f"--sqlFile {sql_file_path} --jobName {req.job_name}",
                    parallelism=req.parallelism
                )
                job_id = result.get("jobid") or result.get("jobId")
                logger.info(f"✅ SQL Runner Jar 提交成功，作业 ID: {job_id}")
                return JobSubmitResponse(job_name=req.job_name, flink_job_id=job_id)
            except Exception as jar_error:
                logger.error(f"❌ SQL Runner Jar 提交失败: {str(jar_error)}")
                logger.error(f"Jar 错误详情: {traceback.format_exc()}")
                raise HTTPException(
                    status_code=500,
                    detail=f"SQL Gateway 和 SQL Runner Jar 都失败。Gateway 错误: {str(e)}, Jar 错误: {str(jar_error)}"
                )
        else:
            logger.error(f"❌ 提交 SQL 作业失败: {error_msg}")
            raise HTTPException(
                status_code=500,
                detail=f"提交 SQL 作业失败: {error_msg}"
            )


@app.get("/api/sql/history", tags=["SQL作业"])
async def get_sql_history():
    """获取已保存的 SQL 文件列表"""
    sql_files = []
    for f in SQL_FILES_DIR.glob("*.sql"):
        content = f.read_text(encoding="utf-8")
        sql_files.append({
            "id": f.stem,
            "filename": f.name,
            "content": content[:500] + "..." if len(content) > 500 else content,
            "size": f.stat().st_size,
            "created": f.stat().st_ctime
        })
    return sorted(sql_files, key=lambda x: x["created"], reverse=True)


@app.get("/api/sql/{file_id}/content", tags=["SQL作业"])
async def get_sql_content(file_id: str):
    """获取指定 SQL 文件的完整内容"""
    try:
        sql_file_path = SQL_FILES_DIR / f"{file_id}.sql"
        if not sql_file_path.exists():
            raise HTTPException(status_code=404, detail="SQL文件不存在")
        
        content = sql_file_path.read_text(encoding="utf-8")
        return {
            "id": file_id,
            "filename": f"{file_id}.sql",
            "content": content
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============ 数据源管理 ============

@app.post("/api/datasources", tags=["数据源管理"])
async def create_datasource(request: DataSourceCreateRequest):
    """创建数据源"""
    try:
        success = add_datasource(
            name=request.name,
            type=request.type,
            host=request.host,
            port=request.port,
            username=request.username,
            password=request.password,
            database=request.database,
            properties=request.properties
        )
        if success:
            return {"status": "success", "message": "数据源已创建"}
        else:
            raise HTTPException(status_code=500, detail="创建数据源失败")
    except Exception as e:
        logger.error(f"创建数据源失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/datasources", tags=["数据源管理"], response_model=List[DataSourceResponse])
async def list_datasources():
    """获取数据源列表"""
    try:
        return get_datasources()
    except Exception as e:
        logger.error(f"获取数据源列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/datasources/{id}", tags=["数据源管理"])
async def update_datasource_endpoint(id: int, request: DataSourceUpdateRequest):
    """更新数据源"""
    try:
        success = update_datasource(
            id=id,
            name=request.name,
            type=request.type,
            host=request.host,
            port=request.port,
            username=request.username,
            password=request.password,
            database=request.database,
            properties=request.properties
        )
        if success:
            return {"status": "success", "message": "数据源已更新"}
        else:
            raise HTTPException(status_code=404, detail="数据源不存在")
    except Exception as e:
        logger.error(f"更新数据源失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/datasources/{id}", tags=["数据源管理"])
async def delete_datasource_endpoint(id: int):
    """删除数据源"""
    try:
        success = delete_datasource(id)
        if success:
            return {"status": "success", "message": "数据源已删除"}
        else:
            raise HTTPException(status_code=404, detail="数据源不存在")
    except Exception as e:
        logger.error(f"删除数据源失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/datasources/test", tags=["数据源管理"])
async def test_datasource(request: DataSourceBase):
    """测试数据源连接"""
    try:
        success, message = test_datasource_connection(
            type=request.type,
            host=request.host,
            port=request.port,
            username=request.username,
            password=request.password,
            database=request.database,
            properties=request.properties
        )
        if success:
            return {"status": "success", "message": message}
        else:
            return {"status": "error", "message": message}
    except Exception as e:
        logger.error(f"测试数据源连接失败: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/api/datasources/{id}/tables", tags=["数据源管理"])
async def get_datasource_tables_api(id: int):
    """获取数据源下的所有表"""
    try:
        from db_operations import get_datasource_tables
        tables = get_datasource_tables(id)
        return {"tables": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/datasources/{id}/tables/{table_name}/columns", tags=["数据源管理"])
async def get_datasource_columns_api(id: int, table_name: str):
    """获取数据源下指定表的字段"""
    try:
        from db_operations import get_datasource_columns
        columns = get_datasource_columns(id, table_name)
        return {"columns": columns}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/datasources/{id}/topics", tags=["数据源管理"])
async def get_kafka_topics_api(id: int):
    """获取Kafka数据源的Topic列表"""
    try:
        from db_operations import get_datasource_by_id
        ds = get_datasource_by_id(id)
        if not ds:
            raise HTTPException(status_code=404, detail="数据源不存在")
        if ds['type'] != 'kafka':
            raise HTTPException(status_code=400, detail="该数据源不是Kafka类型")
        
        bootstrap_servers = ds['host']
        if ds['port']:
            bootstrap_servers = f"{ds['host']}:{ds['port']}"
        
        import socket
        from kafka import KafkaAdminClient
        
        sasl_mechanism = ds.get('properties', {}).get('sasl_mechanism')
        username = ds.get('username')
        password = ds.get('password')
        
        config = {
            'bootstrap_servers': bootstrap_servers,
            'request_timeout_ms': 10000
        }
        
        if sasl_mechanism and username and password:
            config['sasl_mechanism'] = sasl_mechanism
            config['security_protocol'] = 'SASL_PLAINTEXT'
            config['sasl_plain_username'] = username
            config['sasl_plain_password'] = password
        
        admin_client = KafkaAdminClient(**config)
        topics = admin_client.list_topics()
        admin_client.close()
        
        consumer_topics = [t for t in topics if not t.startswith('__')]
        
        return {"topics": consumer_topics}
    except ImportError:
        raise HTTPException(status_code=500, detail="未安装kafka-python库，请执行: pip install kafka-python")
    except Exception as e:
        logger.error(f"获取Kafka Topic列表失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取Topic列表失败: {str(e)}")


@app.get("/api/datasources/{id}/topics/{topic}/schema", tags=["数据源管理"])
async def get_kafka_topic_schema(id: int, topic: str):
    """从Kafka Topic消费样本消息推断字段结构"""
    try:
        from db_operations import get_datasource_by_id
        ds = get_datasource_by_id(id)
        if not ds:
            raise HTTPException(status_code=404, detail="数据源不存在")
        if ds['type'] != 'kafka':
            raise HTTPException(status_code=400, detail="该数据源不是Kafka类型")
        
        bootstrap_servers = ds['host']
        if ds['port']:
            bootstrap_servers = f"{ds['host']}:{ds['port']}"
        
        from kafka import KafkaConsumer
        import json
        import uuid
        
        sasl_mechanism = ds.get('properties', {}).get('sasl_mechanism')
        username = ds.get('username')
        password = ds.get('password')
        
        config = {
            'bootstrap_servers': bootstrap_servers,
            'auto_offset_reset': 'earliest',
            'consumer_timeout_ms': 5000,
            'max_poll_records': 1,
            'fetch_max_bytes': 1024 * 1024,
            'group_id': f'flink-manager-schema-{uuid.uuid4()}'
        }
        
        if sasl_mechanism and username and password:
            config['sasl_mechanism'] = sasl_mechanism
            config['security_protocol'] = 'SASL_PLAINTEXT'
            config['sasl_plain_username'] = username
            config['sasl_plain_password'] = password
        
        consumer = KafkaConsumer(topic, **config)
        
        messages = []
        poll_result = consumer.poll(timeout_ms=5000, max_records=1)
        consumer.close()
        
        for tp, msgs in poll_result.items():
            for msg in msgs:
                messages.append(msg)
                if len(messages) >= 1:
                    break
            if len(messages) >= 1:
                break
        
        if not messages:
            return {"columns": [], "message": "未消费到消息，请手动配置字段"}
        
        columns = []
        for msg in messages:
            try:
                value = msg.value.decode('utf-8') if msg.value else '{}'
                data = json.loads(value)
                if isinstance(data, dict):
                    for key, val in data.items():
                        existing = next((c for c in columns if c['name'] == key), None)
                        if not existing:
                            col_type = infer_flink_type(val)
                            columns.append({
                                'name': key,
                                'type': col_type,
                                'primaryKey': False
                            })
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
        
        if not columns:
            return {"columns": [], "message": "无法解析消息格式，请手动配置字段"}
        
        return {"columns": columns, "message": f"已从{len(messages)}条消息推断字段结构"}
    except ImportError:
        raise HTTPException(status_code=500, detail="未安装kafka-python库，请执行: pip install kafka-python")
    except Exception as e:
        logger.error(f"获取Kafka Topic Schema失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取Schema失败: {str(e)}")


def infer_flink_type(value):
    """根据Python值推断Flink SQL类型"""
    if value is None:
        return 'STRING'
    if isinstance(value, bool):
        return 'BOOLEAN'
    if isinstance(value, int):
        if -2147483648 <= value <= 2147483647:
            return 'INT'
        return 'BIGINT'
    if isinstance(value, float):
        return 'DOUBLE'
    if isinstance(value, str):
        return 'STRING'
    if isinstance(value, list):
        return 'ARRAY'
    if isinstance(value, dict):
        return 'MAP'
    return 'STRING'


# ============ Flink配置管理 ============

class FlinkConfigRequest(BaseModel):
    """Flink配置请求模型"""
    config_key: str = Field(..., description="配置键")
    config_value: str = Field(..., description="配置值")
    description: Optional[str] = Field(None, description="配置描述")

class FlinkConfigResponse(BaseModel):
    """Flink配置响应模型"""
    id: int
    config_key: str
    config_value: str
    description: Optional[str]
    created_at: Optional[str]
    updated_at: Optional[str]


@app.get("/api/flink-config", tags=["Flink配置管理"], response_model=List[FlinkConfigResponse])
async def list_flink_configs():
    """获取所有Flink配置"""
    try:
        from db_operations import get_all_flink_configs
        return get_all_flink_configs()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/flink-config/{config_key}", tags=["Flink配置管理"])
async def get_flink_config_api(config_key: str):
    """获取指定Flink配置"""
    try:
        from db_operations import get_flink_config
        config_value = get_flink_config(config_key)
        if config_value is None:
            raise HTTPException(status_code=404, detail=f"配置 {config_key} 不存在")
        return {"config_key": config_key, "config_value": config_value}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/flink-config", tags=["Flink配置管理"])
async def save_flink_config_api(request: FlinkConfigRequest):
    """保存或更新Flink配置"""
    try:
        from db_operations import save_flink_config
        success = save_flink_config(
            config_key=request.config_key,
            config_value=request.config_value,
            description=request.description
        )
        if success:
            return {"message": "配置保存成功"}
        else:
            raise HTTPException(status_code=500, detail="配置保存失败")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/flink-config/{config_key}", tags=["Flink配置管理"])
async def delete_flink_config_api(config_key: str):
    """删除Flink配置"""
    try:
        from db_operations import delete_flink_config
        success = delete_flink_config(config_key)
        if success:
            return {"message": "配置删除成功"}
        else:
            raise HTTPException(status_code=404, detail=f"配置 {config_key} 不存在")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============ Kafka作业管理 ============
from db_operations import (
    add_kafka_job, update_kafka_job, delete_kafka_job, 
    get_kafka_job, get_all_kafka_jobs, update_kafka_job_state,
    get_datasource_by_id
)

class KafkaJobCreateRequest(BaseModel):
    job_name: str
    source_datasource_id: int
    source_topics: str
    source_group_id: Optional[str] = None
    source_start_mode: Optional[str] = 'latest'
    source_timestamp: Optional[int] = None
    source_format: Optional[str] = 'json'
    source_schema: Optional[List[dict]] = None
    target_datasource_id: int
    target_table: str
    target_database: Optional[str] = None
    auto_create_table: Optional[bool] = True
    table_primary_keys: Optional[str] = None
    field_mappings: Optional[List[dict]] = None
    parallelism: Optional[int] = 1
    checkpoint_interval: Optional[int] = 60000

class KafkaJobUpdateRequest(BaseModel):
    job_name: Optional[str] = None
    source_topics: Optional[str] = None
    source_group_id: Optional[str] = None
    source_start_mode: Optional[str] = None
    source_timestamp: Optional[int] = None
    source_format: Optional[str] = None
    source_schema: Optional[List[dict]] = None
    target_table: Optional[str] = None
    target_database: Optional[str] = None
    auto_create_table: Optional[bool] = None
    table_primary_keys: Optional[str] = None
    field_mappings: Optional[List[dict]] = None
    parallelism: Optional[int] = None
    checkpoint_interval: Optional[int] = None

@app.get("/api/kafka-jobs", tags=["Kafka作业管理"])
async def list_kafka_jobs(state: Optional[str] = None):
    """获取所有Kafka同步作业"""
    try:
        jobs = get_all_kafka_jobs(state=state)
        for job in jobs:
            source_ds = get_datasource_by_id(job['source_datasource_id'])
            target_ds = get_datasource_by_id(job['target_datasource_id'])
            job['source_datasource_name'] = source_ds['name'] if source_ds else '未知'
            job['target_datasource_name'] = target_ds['name'] if target_ds else '未知'
        return jobs
    except Exception as e:
        logger.error(f"获取Kafka作业列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/kafka-jobs/{id}", tags=["Kafka作业管理"])
async def get_kafka_job_detail(id: int):
    """获取Kafka作业详情"""
    try:
        job = get_kafka_job(id)
        if not job:
            raise HTTPException(status_code=404, detail="作业不存在")
        
        source_ds = get_datasource_by_id(job['source_datasource_id'])
        target_ds = get_datasource_by_id(job['target_datasource_id'])
        job['source_datasource'] = source_ds
        job['target_datasource'] = target_ds
        return job
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取Kafka作业详情失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/kafka-jobs", tags=["Kafka作业管理"])
async def create_kafka_job(request: KafkaJobCreateRequest):
    """创建Kafka同步作业"""
    try:
        result = add_kafka_job(
            job_name=request.job_name,
            source_datasource_id=request.source_datasource_id,
            source_topics=request.source_topics,
            source_group_id=request.source_group_id,
            source_start_mode=request.source_start_mode,
            source_timestamp=request.source_timestamp,
            source_format=request.source_format,
            source_schema=request.source_schema,
            target_datasource_id=request.target_datasource_id,
            target_table=request.target_table,
            target_database=request.target_database,
            auto_create_table=request.auto_create_table,
            table_primary_keys=request.table_primary_keys,
            field_mappings=request.field_mappings,
            parallelism=request.parallelism,
            checkpoint_interval=request.checkpoint_interval
        )
        return {"status": "success", "message": "Kafka作业已创建", "data": result}
    except Exception as e:
        logger.error(f"创建Kafka作业失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/kafka-jobs/{id}", tags=["Kafka作业管理"])
async def update_kafka_job_endpoint(id: int, request: KafkaJobUpdateRequest):
    """更新Kafka作业配置"""
    try:
        update_data = {k: v for k, v in request.dict().items() if v is not None}
        success = update_kafka_job(id, **update_data)
        if success:
            return {"status": "success", "message": "Kafka作业已更新"}
        else:
            raise HTTPException(status_code=404, detail="作业不存在")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新Kafka作业失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/kafka-jobs/{id}", tags=["Kafka作业管理"])
async def delete_kafka_job_endpoint(id: int):
    """删除Kafka作业"""
    try:
        success = delete_kafka_job(id)
        if success:
            return {"status": "success", "message": "Kafka作业已删除"}
        else:
            raise HTTPException(status_code=404, detail="作业不存在")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除Kafka作业失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/kafka-jobs/{id}/start", tags=["Kafka作业管理"])
async def start_kafka_job(id: int):
    """启动Kafka同步作业"""
    try:
        job = get_kafka_job(id)
        if not job:
            raise HTTPException(status_code=404, detail="作业不存在")
        
        source_ds = get_datasource_by_id(job['source_datasource_id'])
        target_ds = get_datasource_by_id(job['target_datasource_id'])
        
        if not source_ds or source_ds['type'] != 'kafka':
            raise HTTPException(status_code=400, detail="源数据源必须是Kafka类型")
        if not target_ds:
            raise HTTPException(status_code=400, detail="目标数据源不存在")
        
        flink_sql = generate_kafka_to_db_sql(job, source_ds, target_ds)
        
        job_id = str(uuid.uuid4()).replace('-', '')[:32]
        
        result = await flink_client.submit_sql_job(
            sql_text=flink_sql,
            job_name=job['job_name'],
            parallelism=job['parallelism'],
            checkpoint_interval=job['checkpoint_interval']
        )
        
        if result.get('job_id'):
            update_kafka_job_state(id, 'RUNNING', flink_job_id=result['job_id'])
            return {"status": "success", "message": "作业已启动", "flink_job_id": result['job_id']}
        else:
            update_kafka_job_state(id, 'FAILED', last_error=result.get('error', '启动失败'))
            raise HTTPException(status_code=500, detail=result.get('error', '启动失败'))
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"启动Kafka作业失败: {e}")
        update_kafka_job_state(id, 'FAILED', last_error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/kafka-jobs/{id}/stop", tags=["Kafka作业管理"])
async def stop_kafka_job(id: int):
    """停止Kafka同步作业"""
    try:
        job = get_kafka_job(id)
        if not job:
            raise HTTPException(status_code=404, detail="作业不存在")
        
        if not job['flink_job_id']:
            raise HTTPException(status_code=400, detail="作业未运行")
        
        await flink_client.cancel_job(job['flink_job_id'])
        update_kafka_job_state(id, 'STOPPED')
        return {"status": "success", "message": "作业已停止"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"停止Kafka作业失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def generate_kafka_to_db_sql(job: dict, source_ds: dict, target_ds: dict) -> str:
    """生成Kafka到数据库的Flink SQL"""
    
    bootstrap_servers = source_ds['host']
    if source_ds['port'] and ':' not in source_ds['host']:
        bootstrap_servers = f"{source_ds['host']}:{source_ds['port']}"
    
    topics = job['source_topics']
    group_id = job['source_group_id'] or f"flink_{job['job_name']}"
    start_mode = job['source_start_mode']
    source_format = job['source_format']
    schema = job['source_schema'] or []
    
    target_table = job['target_table']
    target_database = job['target_database'] or target_ds.get('database', '')
    field_mappings = job['field_mappings'] or []
    
    sql_parts = []
    
    kafka_table = f"kafka_source_{job['id']}"
    
    if schema:
        columns_def = ', '.join([f"`{col['name']}` {col['type']}" for col in schema])
        sql_parts.append(f"""
CREATE TABLE {kafka_table} (
    {columns_def}
) WITH (
    'connector' = 'kafka',
    'topic' = '{topics}',
    'properties.bootstrap.servers' = '{bootstrap_servers}',
    'properties.group.id' = '{group_id}',
    'scan.startup.mode' = '{start_mode}',
    'format' = '{source_format}'
);""")
    else:
        sql_parts.append(f"""
CREATE TABLE {kafka_table} (
    data STRING
) WITH (
    'connector' = 'kafka',
    'topic' = '{topics}',
    'properties.bootstrap.servers' = '{bootstrap_servers}',
    'properties.group.id' = '{group_id}',
    'scan.startup.mode' = '{start_mode}',
    'format' = '{source_format}'
);""")
    
    target_type = target_ds['type']
    target_table_name = f"target_table_{job['id']}"
    
    if target_type == 'mysql':
        jdbc_url = f"jdbc:mysql://{target_ds['host']}:{target_ds['port']}/{target_database}"
        connector = 'jdbc'
        driver = 'com.mysql.cj.jdbc.Driver'
    elif target_type == 'postgresql':
        jdbc_url = f"jdbc:postgresql://{target_ds['host']}:{target_ds['port']}/{target_database}"
        connector = 'jdbc'
        driver = 'org.postgresql.Driver'
    elif target_type == 'doris':
        jdbc_url = f"jdbc:mysql://{target_ds['host']}:{target_ds['port']}/{target_database}"
        connector = 'doris'
        driver = 'com.mysql.cj.jdbc.Driver'
    elif target_type == 'starrocks':
        jdbc_url = f"jdbc:mysql://{target_ds['host']}:{target_ds['port']}/{target_database}"
        connector = 'starrocks'
        driver = 'com.mysql.cj.jdbc.Driver'
    else:
        jdbc_url = f"jdbc:{target_type}://{target_ds['host']}:{target_ds['port']}/{target_database}"
        connector = 'jdbc'
        driver = ''
    
    if schema:
        columns_def = ', '.join([f"`{col['name']}` {col['type']}" for col in schema])
        sql_parts.append(f"""
CREATE TABLE {target_table_name} (
    {columns_def}
) WITH (
    'connector' = '{connector}',
    'url' = '{jdbc_url}',
    'table-name' = '{target_table}',
    'username' = '{target_ds['username']}',
    'password' = '{target_ds['password']}'
);""")
        
        if field_mappings:
            select_cols = ', '.join([f"`{m['source']}` AS `{m['target']}`" for m in field_mappings])
        else:
            select_cols = ', '.join([f"`{col['name']}`" for col in schema])
        
        sql_parts.append(f"""
INSERT INTO {target_table_name}
SELECT {select_cols} FROM {kafka_table};""")
    else:
        sql_parts.append(f"""
CREATE TABLE {target_table_name} (
    data STRING
) WITH (
    'connector' = '{connector}',
    'url' = '{jdbc_url}',
    'table-name' = '{target_table}',
    'username' = '{target_ds['username']}',
    'password' = '{target_ds['password']}'
);

INSERT INTO {target_table_name}
SELECT data FROM {kafka_table};""")
    
    return '\n'.join(sql_parts)


# ============ 静态文件服务（前端页面）============
FRONTEND_DIR = BASE_DIR.parent / "frontend" / "dist"

# 如果前端已构建，挂载静态文件
if FRONTEND_DIR.exists():
    app.mount("/assets", StaticFiles(directory=FRONTEND_DIR / "assets"), name="assets")

    @app.get("/", tags=["前端页面"])
    async def serve_frontend():
        """返回前端首页"""
        return FileResponse(FRONTEND_DIR / "index.html")

    @app.get("/{full_path:path}", tags=["前端页面"])
    async def serve_frontend_routes(full_path: str):
        """处理前端路由"""
        # 如果是 API 路径，跳过
        if full_path.startswith("api/") or full_path.startswith("docs") or full_path.startswith("openapi"):
            raise HTTPException(status_code=404)
        # 尝试返回静态文件
        file_path = FRONTEND_DIR / full_path
        if file_path.exists() and file_path.is_file():
            return FileResponse(file_path)
        # 否则返回 index.html（SPA 路由）
        return FileResponse(FRONTEND_DIR / "index.html")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
