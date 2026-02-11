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

from .config import FLINK_REST_URL, SQL_FILES_DIR, BASE_DIR
from .database import db_settings, get_database_url
from .db_operations import (
    db_manager, save_job, update_job_state, get_job, get_all_jobs,
    save_savepoint, get_latest_savepoint, get_savepoints, log_operation,
    add_datasource, get_datasources, update_datasource, delete_datasource, test_datasource_connection
)
from .schemas import (
    SqlJobSubmitRequest, JarJobSubmitRequest, JobSubmitResponse,
    ClusterStatus, JobInfo, JobDetail, SavepointRequest, ResumeJobRequest,
    DataSourceCreateRequest, DataSourceUpdateRequest, DataSourceResponse,
    DataSourceBase, TableCreateRequest
)
from .flink_client import flink_client, FlinkClient
from sqlalchemy import create_engine, inspect, text
from pydantic import BaseModel
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
                
        except Exception as e:
            logger.error(f"❌ Schema 检查/修复失败: {e}")
            # 不阻断启动，因为可能只是权限问题或已存在
        finally:
            session.close()
            
        logger.info("✅ 数据库初始化成功")
    except Exception as e:
        logger.error(f"❌ 数据库初始化失败: {e}")
        raise
    
    yield
    
    # 关闭时清理资源
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
                
        create_sql = f"CREATE TABLE `{request.table_name}` (\n"
        create_sql += ",\n".join(columns_sql)
        
        if primary_keys:
            create_sql += f",\nPRIMARY KEY ({', '.join(primary_keys)})"
            
        create_sql += "\n)"
        
        # StarRocks/Doris 特定语法
        is_olap = request.connector_type in ['starrocks', 'doris', 'starrocks-cdc', 'doris-cdc']
        if is_olap:
            create_sql += " ENGINE=OLAP"
            if primary_keys:
                create_sql += f"\nDISTRIBUTED BY HASH({', '.join(primary_keys)}) BUCKETS 10"
            else:
                # 如果没有主键，StarRocks 需要指定分布键
                if request.fields:
                    create_sql += f"\nDISTRIBUTED BY HASH(`{request.fields[0].name}`) BUCKETS 10"
            
            create_sql += "\nPROPERTIES(\"replication_num\" = \"1\")"

        logger.info(f"Executing Create Table SQL: {create_sql}")
        
        with engine.connect() as conn:
            conn.execute(text(create_sql))
            conn.commit()
            
        return {"status": "success", "message": f"表 {request.table_name} 创建成功"}
        
    except Exception as e:
        logger.error(f"创建表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"创建表失败: {str(e)}")

from .schemas import (
    SqlJobSubmitRequest, JarJobSubmitRequest, JobSubmitResponse,
    ClusterStatus, JobInfo, JobDetail, SavepointRequest, ResumeJobRequest
)
from .flink_client import flink_client, FlinkClient


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
@app.get("/api/jobs/history", tags=["作业管理"])
async def get_job_history(limit: int = 50):
    """获取历史作业列表（包括已停止和正在运行的作业，按名称去重）"""
    logger.info(f"=== 正在请求历史作业列表 (limit={limit}) ===")
    try:
        # 从数据库获取所有作业
        # 注意：get_all_jobs 会返回所有记录，我们需要在这里做严格的去重逻辑
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
        
        # 获取所有正在运行作业的 resumed_from_job_id（这些历史作业已被成功重启）
        restarted_job_ids = set()
        for job in all_jobs:
            job_id = job.get("job_id")
            
            # 如果作业在 Flink 中存在，使用 Flink 的实时信息更新
            if job_id in flink_job_map:
                flink_info = flink_job_map[job_id]
                # 更新状态
                if flink_info.get("status") == "RUNNING":
                    job["state"] = "RUNNING"
                
                # 如果数据库中没有 start_time，尝试从 Flink 信息中获取
                if not job.get("start_time") and flink_info.get("start-time"):
                    job["start_time"] = flink_info.get("start-time")
            
            resumed_from = job.get("resumed_from_job_id")
            job_state = job.get("state")
            # 如果这个作业正在运行，并且是从某个历史作业重启的
            if resumed_from and (job_state == "RUNNING" or job_id in flink_running_jobs):
                restarted_job_ids.add(resumed_from)
                logger.debug(f"作业 {resumed_from} 已被成功重启为 {job_id}")
        
        logger.info(f"已成功重启的历史作业数: {len(restarted_job_ids)}")
        
        # 过滤逻辑：
        # 1. 排除已被成功重启的旧作业（如果新的已经在运行）
        # 2. 不再按名称去重，显示所有历史记录
        
        history_jobs = []

        # 先按创建时间倒序排序，确保先处理最新的
        # 注意：created_at可能是None，需要处理
        sorted_all_jobs = sorted(
            all_jobs, 
            key=lambda x: x.get("created_at") or "",
            reverse=True
        )

        for job in sorted_all_jobs:
            job_id = job.get("job_id")
            
            # 如果这个作业已经被成功重启，则不显示（因为它已经是历史了，有一个新的在运行）
            if job_id in restarted_job_ids:
                logger.debug(f"作业 {job_id} 已被成功重启，不显示在历史作业中")
                continue
            
            history_jobs.append(job)
        
        # 动态更新 RUNNING 状态作业的运行时长
        current_time_ms = int(time.time() * 1000)
        for job in history_jobs:
            if job.get("state") == "RUNNING" and job.get("start_time"):
                try:
                    start_time = int(job.get("start_time"))
                    if start_time > 0:
                        job["duration"] = current_time_ms - start_time
                except (ValueError, TypeError):
                    pass

        # 最终排序
        try:
            sorted_jobs = sorted(
                history_jobs, 
                key=lambda x: x.get("created_at") or "",
                reverse=True
            )
        except Exception as sort_err:
            logger.error(f"排序历史作业失败: {sort_err}")
            # 降级：如果不排序直接返回
            sorted_jobs = history_jobs
        
        logger.info(f"返回 {len(sorted_jobs)} 个历史作业（从 {len(all_jobs)} 个作业中过滤）")
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
        from .db_operations import delete_job
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
        
        if not savepoint_path:
            # 查询最新的Savepoint
            latest_savepoint = get_latest_savepoint(job_id_to_restart)
            if latest_savepoint:
                savepoint_path = latest_savepoint.get("savepoint_path")
                savepoint_timestamp = latest_savepoint.get("timestamp")
                logger.info(f"Found latest savepoint: {savepoint_path}")
        
        # 如果没有指定timestamp，尝试从db_job获取（如果路径匹配）
        if not savepoint_timestamp:
            if db_job and savepoint_path == db_job.get("savepoint_path"):
                 savepoint_timestamp = db_job.get("savepoint_timestamp")
            
            # 补救措施：如果还没获取到，尝试从 savepoints 表获取
            if not savepoint_timestamp and savepoint_path:
                try:
                    from .db_operations import db_manager, FlinkSavepoint
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

        # 使用SQL Gateway重新提交作业
        result = await flink_client.submit_sql_job(
            sql=sql_text,
            job_name=job_name,
            parallelism=parallelism,
            savepoint_path=savepoint_path
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
                    from .db_operations import save_savepoint
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
                         from .db_operations import save_job
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
        
        try:
            if req.target_directory or req.withSavepoint:
                logger.info(f"调用stop_job_with_savepoint，目录: {req.target_directory}")
                result = await flink_client.stop_job_with_savepoint(job_id, req.target_directory)
                
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
                    from .db_operations import get_all_jobs
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
        
        # 使用SQL Gateway重新提交作业，指定savepoint路径和原始作业名称
        logger.info(f"Calling submit_sql_job...")
        result = await flink_client.submit_sql_job(
            sql=sql_text,
            job_name=original_job_name,
            parallelism=parallelism,
            savepoint_path=savepoint_path
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
        from .db_operations import get_savepoints
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
    from .config import SQL_RUNNER_JAR_ID, SQL_GATEWAY_URL
    import traceback
    import time

    logger.info(f"=== 提交 SQL 作业 ===")
    logger.info(f"作业名称: {req.job_name}")
    logger.info(f"并行度: {req.parallelism}")
    logger.info(f"SQL 内容（前 200 字符）: {req.sql_text[:200]}")
    logger.info(f"SQL 长度: {len(req.sql_text)}")

    # 1. 尝试使用 SQL Gateway 提交
    try:
        logger.info(f"尝试使用 SQL Gateway 提交作业")
        logger.info(f"SQL Gateway URL: {SQL_GATEWAY_URL}")

        result = await flink_client.submit_sql_job(
            sql=req.sql_text,
            job_name=req.job_name,
            parallelism=req.parallelism
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
                    from .db_operations import delete_job
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
            database=request.database
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
        from .db_operations import get_datasource_tables
        tables = get_datasource_tables(id)
        return {"tables": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/datasources/{id}/tables/{table_name}/columns", tags=["数据源管理"])
async def get_datasource_columns_api(id: int, table_name: str):
    """获取数据源下指定表的字段"""
    try:
        from .db_operations import get_datasource_columns
        columns = get_datasource_columns(id, table_name)
        return {"columns": columns}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
