from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime


# ============ 集群相关 ============
class ClusterInfo(BaseModel):
    name: str
    host: str
    port: int = 8081
    description: Optional[str] = None


class ClusterStatus(BaseModel):
    name: str
    host: str
    port: int
    status: str  # online / offline
    flink_version: Optional[str] = None
    slots_total: int = 0
    slots_available: int = 0
    taskmanagers: int = 0
    jobs_running: int = 0
    jobs_finished: int = 0
    jobs_cancelled: int = 0
    jobs_failed: int = 0


# ============ 作业相关 ============
class SqlJobSubmitRequest(BaseModel):
    job_name: str
    sql_text: str
    parallelism: int = 1


class JarJobSubmitRequest(BaseModel):
    job_name: str
    jar_id: str
    entry_class: Optional[str] = None
    program_args: Optional[str] = None
    parallelism: int = 1
    savepoint_path: Optional[str] = None


class JobSubmitResponse(BaseModel):
    job_name: str
    flink_job_id: str


class JobInfo(BaseModel):
    jid: str
    name: str
    state: str
    start_time: Optional[int] = None
    end_time: Optional[int] = None
    duration: Optional[int] = None


class JobDetail(BaseModel):
    jid: str
    name: str
    state: str
    start_time: Optional[int] = None
    end_time: Optional[int] = None
    duration: Optional[int] = None
    parallelism: Optional[int] = None
    plan: Optional[dict] = None
    vertices: Optional[List[dict]] = None


# ============ 监控相关 ============
class JobMetrics(BaseModel):
    jid: str
    metrics: dict


class TaskManagerInfo(BaseModel):
    id: str
    path: str
    data_port: int
    slots_number: int
    free_slots: int
    hardware: Optional[dict] = None


# ============ Jar 相关 ============
class JarInfo(BaseModel):
    id: str
    name: str
    uploaded_at: Optional[int] = None
    entry_classes: Optional[List[str]] = None

class SavepointRequest(BaseModel):
    target_directory: Optional[str] = None
    cancel_job: Optional[bool] = False
    withSavepoint: Optional[bool] = True  # Default to True for Pause operation

class ResumeJobRequest(BaseModel):
    job_id: str
    savepoint_path: Optional[str] = None
    timestamp: Optional[str] = None # 前端传递的 savepoint 时间

# ============ 数据源相关 ============
class DataSourceBase(BaseModel):
    name: str
    type: str
    host: str
    port: int
    username: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    properties: Optional[dict] = None

class DataSourceCreateRequest(DataSourceBase):
    pass

class DataSourceUpdateRequest(DataSourceBase):
    pass

class DataSourceResponse(DataSourceBase):
    id: int
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    class Config:
        from_attributes = True


# ============ 元数据管理 ============
class FieldDefinition(BaseModel):
    name: str
    type: str
    precision: Optional[str] = None
    scale: Optional[str] = None
    primaryKey: bool = False

class TableCreateRequest(BaseModel):
    datasource_id: Optional[int] = None
    connector_type: str
    table_name: str
    fields: List[FieldDefinition]
    # For manual connection details if datasource_id is missing
    host: Optional[str] = None
    port: Optional[int] = None
    username: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    url: Optional[str] = None # JDBC URL if applicable
