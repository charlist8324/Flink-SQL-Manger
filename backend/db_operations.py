
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from sqlalchemy import create_engine, Column, String, Integer, Text, TIMESTAMP, JSON, BigInteger, desc
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.pool import QueuePool
from database import get_database_url

logger = logging.getLogger(__name__)

Base = declarative_base()

class FlinkJob(Base):
    """Flink作业表"""
    __tablename__ = "flink_jobs"
    
    job_id = Column(String(50), primary_key=True, comment='作业ID (UUID)')
    job_name = Column(String(100), nullable=False, comment='作业名称')
    flink_job_name = Column(String(100), comment='Flink集群中的作业名称')
    state = Column(String(20), default='CREATED', comment='作业状态')
    sql_text = Column(Text, comment='SQL内容')
    parallelism = Column(Integer, default=1, comment='并行度')
    properties = Column(JSON, comment='配置属性')
    start_time = Column(BigInteger, comment='开始时间(毫秒)')
    end_time = Column(BigInteger, comment='结束时间(毫秒)')
    savepoint_path = Column(String(500), comment='Savepoint路径')
    savepoint_timestamp = Column(BigInteger, comment='Savepoint时间戳(毫秒)')
    resumed_from_job_id = Column(String(50), comment='从哪个作业恢复')
    created_at = Column(TIMESTAMP, default=datetime.now, comment='创建时间')
    updated_at = Column(TIMESTAMP, default=datetime.now, onupdate=datetime.now, comment='更新时间')

class FlinkSavepoint(Base):
    """Savepoint记录表"""
    __tablename__ = "flink_savepoints"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String(50), nullable=False, comment='作业ID')
    savepoint_id = Column(String(100), comment='Savepoint ID')
    savepoint_path = Column(String(500), nullable=False, comment='保存路径')
    status = Column(String(20), default='IN_PROGRESS', comment='状态')
    job_state_at_snapshot = Column(JSON, comment='触发时的作业状态')
    timestamp = Column(BigInteger, comment='时间戳(毫秒)')
    trigger_time = Column(TIMESTAMP, default=datetime.now, comment='触发时间')

class FlinkJobOperation(Base):
    """作业操作日志表"""
    __tablename__ = "flink_job_operations"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String(50), nullable=False, comment='作业ID')
    operation_type = Column(String(50), nullable=False, comment='操作类型')
    operation_details = Column(JSON, comment='操作详情')
    status = Column(String(20), default='SUCCESS', comment='操作状态')
    error_message = Column(Text, comment='错误信息')
    operation_time = Column(TIMESTAMP, default=datetime.now, comment='操作时间')

class DataSource(Base):
    """数据源配置表"""
    __tablename__ = "data_sources"
    
    id = Column(Integer, primary_key=True, autoincrement=True, comment='主键ID')
    name = Column(String(100), nullable=False, unique=True, comment='数据源名称')
    type = Column(String(50), nullable=False, comment='数据源类型 (mysql, postgres, etc.)')
    host = Column(String(255), nullable=False, comment='主机地址')
    port = Column(Integer, nullable=False, comment='端口')
    username = Column(String(100), nullable=True, comment='用户名')
    password = Column(String(255), nullable=True, comment='密码')
    database = Column(String(100), nullable=True, comment='数据库名')
    properties = Column(JSON, nullable=True, comment='其他配置属性')
    created_at = Column(TIMESTAMP, default=datetime.now, comment='创建时间')
    updated_at = Column(TIMESTAMP, default=datetime.now, onupdate=datetime.now, comment='更新时间')

class DatabaseManager:
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
        return cls._instance
    
    def initialize(self):
        if not self._initialized:
            db_url = get_database_url()
            self.engine = create_engine(
                db_url,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=3600,
                pool_pre_ping=True,
                echo=False
            )
            Base.metadata.create_all(bind=self.engine)
            self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
            self._initialized = True
            
    def get_session(self):
        if not self._initialized:
            self.initialize()
        return self.SessionLocal()

    def close(self):
        if self._initialized:
            self.engine.dispose()

db_manager = DatabaseManager()

# ============ 作业管理 ============

def save_job(job_id: str, job_name: str, sql_text: str = None, 
             flink_job_name: str = None, parallelism: int = 1, 
             properties: Dict = None, state: str = 'CREATED',
             resumed_from_job_id: str = None,
             savepoint_path: str = None,
             savepoint_timestamp: int = None,
             start_time: int = None) -> bool:
    """保存或更新作业信息"""
    session = db_manager.get_session()
    try:
        job = session.query(FlinkJob).filter(FlinkJob.job_id == job_id).first()
        if not job:
            job = FlinkJob(
                job_id=job_id,
                job_name=job_name,
                flink_job_name=flink_job_name,
                sql_text=sql_text,
                parallelism=parallelism,
                properties=properties,
                state=state,
                resumed_from_job_id=resumed_from_job_id,
                savepoint_path=savepoint_path,
                savepoint_timestamp=savepoint_timestamp,
                start_time=start_time,
                created_at=datetime.now()
            )
            session.add(job)
        else:
            job.job_name = job_name
            if sql_text: job.sql_text = sql_text
            if flink_job_name: job.flink_job_name = flink_job_name
            if parallelism: job.parallelism = parallelism
            if properties: job.properties = properties
            if state: job.state = state
            if resumed_from_job_id: job.resumed_from_job_id = resumed_from_job_id
            if savepoint_path: job.savepoint_path = savepoint_path
            if savepoint_timestamp: job.savepoint_timestamp = savepoint_timestamp
            if start_time: job.start_time = start_time
            job.updated_at = datetime.now()
        
        session.commit()
        return True
    except Exception as e:
        session.rollback()
        logger.error(f"❌ 保存作业失败: {e}")
        return False
    finally:
        session.close()

def update_job_state(job_id: str, state: str, start_time: int = None, end_time: int = None, duration: int = None, flink_job_name: str = None, savepoint_path: str = None, savepoint_timestamp: int = None) -> bool:
    """更新作业状态"""
    session = db_manager.get_session()
    try:
        job = session.query(FlinkJob).filter(FlinkJob.job_id == job_id).first()
        if job:
            job.state = state
            if start_time: job.start_time = start_time
            if end_time: job.end_time = end_time
            if flink_job_name: job.flink_job_name = flink_job_name
            if savepoint_path: job.savepoint_path = savepoint_path
            if savepoint_timestamp: job.savepoint_timestamp = savepoint_timestamp
            # duration 字段不存储在数据库中，由前端计算
            job.updated_at = datetime.now()
            session.commit()
            return True
        return False
    except Exception as e:
        session.rollback()
        logger.error(f"❌ 更新作业状态失败: {e}")
        return False
    finally:
        session.close()

def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    """获取作业信息"""
    session = db_manager.get_session()
    try:
        job = session.query(FlinkJob).filter(FlinkJob.job_id == job_id).first()
        if job:
            return {
                "job_id": job.job_id,
                "job_name": job.job_name,
                "flink_job_name": job.flink_job_name,
                "state": job.state,
                "sql_text": job.sql_text,
                "parallelism": job.parallelism,
                "properties": job.properties,
                "start_time": job.start_time,
                "end_time": job.end_time,
                "savepoint_path": job.savepoint_path,
                "savepoint_timestamp": job.savepoint_timestamp,
                "resumed_from_job_id": job.resumed_from_job_id,
                "created_at": job.created_at.isoformat() if job.created_at else None,
                "updated_at": job.updated_at.isoformat() if job.updated_at else None
            }
        return None
    except Exception as e:
        logger.error(f"❌ 获取作业失败: {e}")
        return None
    finally:
        session.close()

def get_all_jobs(state: str = None, limit: int = 100) -> List[Dict[str, Any]]:
    """获取所有作业"""
    session = db_manager.get_session()
    try:
        query = session.query(FlinkJob)
        if state:
            query = query.filter(FlinkJob.state == state)
        
        jobs = query.order_by(FlinkJob.created_at.desc()).limit(limit).all()
        return [
            {
                "job_id": job.job_id,
                "job_name": job.job_name,
                "flink_job_name": job.flink_job_name,
                "state": job.state,
                "sql_text": job.sql_text,
                "parallelism": job.parallelism,
                "properties": job.properties,
                "start_time": job.start_time,
                "end_time": job.end_time,
                "savepoint_path": job.savepoint_path,
                "savepoint_timestamp": job.savepoint_timestamp,
                "resumed_from_job_id": job.resumed_from_job_id,
                "created_at": job.created_at.isoformat() if job.created_at else None,
                "updated_at": job.updated_at.isoformat() if job.updated_at else None
            }
            for job in jobs
        ]
    except Exception as e:
        logger.error(f"❌ 获取所有作业失败: {e}")
        return []
    finally:
        session.close()

def delete_job(job_id: str) -> bool:
    """删除作业记录"""
    session = db_manager.get_session()
    try:
        job = session.query(FlinkJob).filter(FlinkJob.job_id == job_id).first()
        if job:
            session.delete(job)
            session.commit()
            return True
        return False
    except Exception as e:
        session.rollback()
        logger.error(f"❌ 删除作业失败: {e}")
        return False
    finally:
        session.close()

# ============ Savepoint管理 ============

def save_savepoint(job_id: str, savepoint_path: str, savepoint_id: str = None,
                   status: str = 'COMPLETED', job_state: JSON = None,
                   timestamp: int = None) -> bool:
    """保存Savepoint信息"""
    session = db_manager.get_session()
    try:
        # 更新Job表中的最新Savepoint
        job = session.query(FlinkJob).filter(FlinkJob.job_id == job_id).first()
        if job:
            job.savepoint_path = savepoint_path
            if timestamp:
                job.savepoint_timestamp = timestamp
        
        # 查找是否存在相同的Savepoint
        existing = session.query(FlinkSavepoint).filter(
            FlinkSavepoint.savepoint_path == savepoint_path
        ).first()
        
        if existing:
            existing.status = status
            if savepoint_id: existing.savepoint_id = savepoint_id
            if job_state: existing.job_state_at_snapshot = job_state
            if timestamp: existing.timestamp = timestamp
            existing.trigger_time = datetime.now()
        else:
            savepoint = FlinkSavepoint(
                job_id=job_id,
                savepoint_id=savepoint_id,
                savepoint_path=savepoint_path,
                status=status,
                job_state_at_snapshot=job_state,
                timestamp=timestamp,
                trigger_time=datetime.now()
            )
            session.add(savepoint)
        
        session.commit()
        return True
    except Exception as e:
        session.rollback()
        logger.error(f"❌ 保存Savepoint失败: {e}")
        return False
    finally:
        session.close()

def get_latest_savepoint(job_id: str) -> Optional[Dict[str, Any]]:
    """获取作业最新的Savepoint"""
    session = db_manager.get_session()
    try:
        savepoint = session.query(FlinkSavepoint).filter(
            FlinkSavepoint.job_id == job_id
        ).order_by(FlinkSavepoint.trigger_time.desc()).first()
        
        if savepoint:
            return {
                "id": savepoint.id,
                "savepoint_id": savepoint.savepoint_id,
                "savepoint_path": savepoint.savepoint_path,
                "job_id": savepoint.job_id,
                "status": savepoint.status,
                "job_state_at_snapshot": savepoint.job_state_at_snapshot,
                "timestamp": savepoint.timestamp,
                "trigger_time": savepoint.trigger_time.isoformat() if savepoint.trigger_time else None
            }
        return None
    except Exception as e:
        logger.error(f"❌ 获取最新Savepoint失败: {e}")
        return None
    finally:
        session.close()

def get_savepoints(job_id: str = None, limit: int = 50) -> List[Dict[str, Any]]:
    """获取Savepoint列表"""
    session = db_manager.get_session()
    try:
        query = session.query(FlinkSavepoint)
        if job_id:
            query = query.filter(FlinkSavepoint.job_id == job_id)
        query = query.order_by(FlinkSavepoint.trigger_time.desc()).limit(limit)
        
        savepoints = query.all()
        return [
            {
                "id": sp.id,
                "savepoint_id": sp.savepoint_id,
                "savepoint_path": sp.savepoint_path,
                "job_id": sp.job_id,
                "status": sp.status,
                "job_state_at_snapshot": sp.job_state_at_snapshot,
                "timestamp": sp.timestamp,
                "trigger_time": sp.trigger_time.isoformat() if sp.trigger_time else None
            }
            for sp in savepoints
        ]
    except Exception as e:
        logger.error(f"❌ 获取Savepoint列表失败: {e}")
        return []
    finally:
        session.close()

def log_operation(job_id: str, operation_type: str, operation_details: Dict = None,
                 status: str = 'SUCCESS', error_message: str = None) -> bool:
    """记录操作日志"""
    session = db_manager.get_session()
    try:
        operation = FlinkJobOperation(
            job_id=job_id,
            operation_type=operation_type,
            operation_details=operation_details,
            status=status,
            error_message=error_message
        )
        session.add(operation)
        session.commit()
        return True
    except Exception as e:
        session.rollback()
        logger.error(f"❌ 记录操作日志失败: {e}")
        return False
    finally:
        session.close()

# ============ 数据源管理 ============

def add_datasource(name: str, type: str, host: str, port: int, 
                   username: str = None, password: str = None, 
                   database: str = None, properties: Dict = None) -> bool:
    """添加数据源"""
    session = db_manager.get_session()
    try:
        datasource = DataSource(
            name=name,
            type=type,
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
            properties=properties
        )
        session.add(datasource)
        session.commit()
        logger.info(f"✅ 数据源已添加: {name}")
        return True
    except Exception as e:
        session.rollback()
        logger.error(f"❌ 添加数据源失败: {e}")
        raise e
    finally:
        session.close()

def get_datasources() -> List[Dict[str, Any]]:
    """获取所有数据源"""
    session = db_manager.get_session()
    try:
        datasources = session.query(DataSource).order_by(DataSource.created_at.desc()).all()
        return [
            {
                "id": ds.id,
                "name": ds.name,
                "type": ds.type,
                "host": ds.host,
                "port": ds.port,
                "username": ds.username,
                "password": ds.password,
                "database": ds.database,
                "properties": ds.properties,
                "created_at": ds.created_at.isoformat() if ds.created_at else None,
                "updated_at": ds.updated_at.isoformat() if ds.updated_at else None
            }
            for ds in datasources
        ]
    except Exception as e:
        logger.error(f"❌ 获取数据源列表失败: {e}")
        return []
    finally:
        session.close()

def update_datasource(id: int, name: str, type: str, host: str, port: int, 
                      username: str = None, password: str = None, 
                      database: str = None, properties: Dict = None) -> bool:
    """更新数据源"""
    session = db_manager.get_session()
    try:
        ds = session.query(DataSource).filter(DataSource.id == id).first()
        if ds:
            ds.name = name
            ds.type = type
            ds.host = host
            ds.port = port
            ds.username = username
            if password:
                ds.password = password
            ds.database = database
            ds.properties = properties
            session.commit()
            logger.info(f"✅ 数据源已更新: {name}")
            return True
        return False
    except Exception as e:
        session.rollback()
        logger.error(f"❌ 更新数据源失败: {e}")
        raise e
    finally:
        session.close()

def delete_datasource(id: int) -> bool:
    """删除数据源"""
    session = db_manager.get_session()
    try:
        ds = session.query(DataSource).filter(DataSource.id == id).first()
        if ds:
            session.delete(ds)
            session.commit()
            logger.info(f"✅ 数据源已删除: {id}")
            return True
        return False
    except Exception as e:
        session.rollback()
        logger.error(f"❌ 删除数据源失败: {e}")
        return False
    finally:
        session.close()

def test_datasource_connection(type: str, host: str, port: int, 
                             username: str = None, password: str = None, 
                             database: str = None) -> tuple[bool, str]:
    """测试数据源连接"""
    import pymysql
    
    try:
        if type == 'mysql':
            conn = pymysql.connect(
                host=host,
                port=port,
                user=username,
                password=password,
                database=database,
                connect_timeout=5
            )
            conn.close()
            return True, "连接成功"
        elif type == 'postgresql':
            # 需要安装 psycopg2-binary
            # import psycopg2
            # conn = psycopg2.connect(...)
            return False, "暂不支持 PostgreSQL 测试连接"
        elif type == 'oracle':
            # 需要安装 cx_Oracle
            return False, "暂不支持 Oracle 测试连接"
        else:
            # 尝试使用通用的 socket 连接测试端口是否开放
            import socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((host, port))
            sock.close()
            if result == 0:
                return True, f"端口 {port} 连接成功 (仅TCP检测)"
            else:
                return False, f"端口 {port} 连接失败"
                
    except Exception as e:
        logger.error(f"❌ 测试连接失败: {str(e)}")
        return False, f"连接失败: {str(e)}"

def get_datasource_tables(id: int) -> List[str]:
    """获取数据源的所有表"""
    from sqlalchemy import inspect
    import pymysql
    from urllib.parse import quote_plus
    
    session = db_manager.get_session()
    try:
        ds = session.query(DataSource).filter(DataSource.id == id).first()
        if not ds:
            raise Exception("数据源不存在")
        
        if ds.type == 'mysql':
            # 格式: mysql+pymysql://user:password@host:port/database
            # 对用户名和密码进行 URL 编码，防止特殊字符导致连接失败
            encoded_user = quote_plus(ds.username)
            encoded_password = quote_plus(ds.password)
            db_url = f"mysql+pymysql://{encoded_user}:{encoded_password}@{ds.host}:{ds.port}/{ds.database}"
            
            engine = create_engine(db_url)
            inspector = inspect(engine)
            return inspector.get_table_names()
        else:
            raise Exception(f"暂不支持获取 {ds.type} 类型的表列表")
    except Exception as e:
        logger.error(f"❌ 获取表列表失败: {e}")
        raise e
    finally:
        session.close()

def get_datasource_columns(id: int, table_name: str) -> List[Dict[str, Any]]:
    """获取数据源指定表的字段信息"""
    from sqlalchemy import inspect
    import pymysql
    from urllib.parse import quote_plus
    
    session = db_manager.get_session()
    try:
        ds = session.query(DataSource).filter(DataSource.id == id).first()
        if not ds:
            raise Exception("数据源不存在")
            
        if ds.type == 'mysql':
            encoded_user = quote_plus(ds.username)
            encoded_password = quote_plus(ds.password)
            db_url = f"mysql+pymysql://{encoded_user}:{encoded_password}@{ds.host}:{ds.port}/{ds.database}"
            
            engine = create_engine(db_url)
            inspector = inspect(engine)
            columns = inspector.get_columns(table_name)
            pk_constraint = inspector.get_pk_constraint(table_name)
            pk_columns = pk_constraint.get('constrained_columns', [])
            
            # 转换 SQLAlchemy 类型为 Flink SQL 类型
            # 这是一个简单的映射，可能需要完善
            result = []
            for col in columns:
                col_type = str(col['type']).lower()
                flink_type = "STRING"
                
                if 'int' in col_type:
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
                elif 'date' in col_type:
                    flink_type = "DATE"
                elif 'time' in col_type:
                    flink_type = "TIME"
                
                result.append({
                    "name": col['name'],
                    "type": flink_type,
                    "original_type": str(col['type']),
                    "primaryKey": col['name'] in pk_columns
                })
            return result
        else:
            raise Exception(f"暂不支持获取 {ds.type} 类型的字段信息")
    except Exception as e:
        logger.error(f"❌ 获取字段信息失败: {e}")
        raise e
    finally:
        session.close()
