-- Flink 作业管理系统数据库表结构
-- 数据库: flink_db
-- 字符集: utf8mb4

-- 创建数据库（如果不存在）
--CREATE DATABASE IF NOT EXISTS flink_db DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE flink_db;

-- 作业信息表
CREATE TABLE IF NOT EXISTS flink_jobs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    job_id VARCHAR(128) NOT NULL UNIQUE COMMENT 'Flink作业ID (jid)',
    job_name VARCHAR(255) NOT NULL COMMENT '用户配置的作业名称',
    flink_job_name VARCHAR(255) DEFAULT NULL COMMENT 'Flink返回的作业名称',
    state VARCHAR(50) DEFAULT 'CREATED' COMMENT '作业状态: CREATED, RUNNING, FINISHED, FAILED, CANCELED',
    sql_text LONGTEXT NOT NULL COMMENT 'SQL文本',
    parallelism INT DEFAULT 1 COMMENT '并行度',
    
    -- 时间信息
    start_time BIGINT DEFAULT NULL COMMENT '开始时间(毫秒时间戳)',
    end_time BIGINT DEFAULT NULL COMMENT '结束时间(毫秒时间戳)',
    duration BIGINT DEFAULT NULL COMMENT '运行时长(毫秒)',
    
    -- 提交信息
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    -- 恢复信息
    resumed_from_job_id VARCHAR(128) DEFAULT NULL COMMENT '从哪个作业恢复（savepoint）',
    parent_job_id VARCHAR(128) DEFAULT NULL COMMENT '父作业ID（用于恢复链）',
    
    INDEX idx_job_id (job_id),
    INDEX idx_state (state),
    INDEX idx_created_at (created_at),
    INDEX idx_resumed_from (resumed_from_job_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Flink作业信息表';

-- Savepoint信息表
CREATE TABLE IF NOT EXISTS flink_savepoints (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    job_id VARCHAR(128) NOT NULL COMMENT 'Flink作业ID',
    savepoint_id VARCHAR(128) NOT NULL COMMENT 'Savepoint ID',
    savepoint_path VARCHAR(1024) NOT NULL COMMENT 'Savepoint路径',
    trigger_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '触发时间',
    status VARCHAR(50) DEFAULT 'COMPLETED' COMMENT '状态: PENDING, COMPLETED, FAILED',
    
    -- Savepoint详情
    external_path VARCHAR(1024) DEFAULT NULL COMMENT '外部路径（如果有）',
    checkpoint_type VARCHAR(50) DEFAULT 'SAVEPOINT' COMMENT 'Checkpoint类型',
    
    -- 作业状态快照
    job_state_at_snapshot VARCHAR(50) DEFAULT NULL COMMENT '创建Savepoint时的作业状态',
    timestamp BIGINT DEFAULT NULL COMMENT '时间戳（毫秒）',
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    
    INDEX idx_job_id (job_id),
    INDEX idx_savepoint_id (savepoint_id),
    INDEX idx_trigger_time (trigger_time),
    INDEX idx_job_status (job_state_at_snapshot),
    
    UNIQUE KEY uk_savepoint_id (savepoint_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Savepoint信息表';

-- 作业操作日志表
CREATE TABLE IF NOT EXISTS flink_job_operations (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    job_id VARCHAR(128) NOT NULL COMMENT 'Flink作业ID',
    operation_type VARCHAR(50) NOT NULL COMMENT '操作类型: SUBMIT, STOP, CANCEL, RESTART, SAVEPOINT',
    operation_details JSON DEFAULT NULL COMMENT '操作详情（JSON格式）',
    operator VARCHAR(100) DEFAULT 'system' COMMENT '操作人',
    operation_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '操作时间',
    status VARCHAR(50) DEFAULT 'SUCCESS' COMMENT '操作状态: SUCCESS, FAILED, PENDING',
    error_message TEXT DEFAULT NULL COMMENT '错误信息（如果失败）',
    
    INDEX idx_job_id (job_id),
    INDEX idx_operation_type (operation_type),
    INDEX idx_operation_time (operation_time),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='作业操作日志表';

-- 作业配置表（可选，用于保存可视化配置）
CREATE TABLE IF NOT EXISTS flink_job_configs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    job_id VARCHAR(128) NOT NULL UNIQUE COMMENT 'Flink作业ID',
    source_tables JSON DEFAULT NULL COMMENT '源表配置',
    sink_tables JSON DEFAULT NULL COMMENT '汇表配置',
    queries JSON DEFAULT NULL COMMENT '查询配置',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    INDEX idx_job_id (job_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='作业配置表';

-- 视图：作业最新状态（包含最新的Savepoint）
CREATE OR REPLACE VIEW v_flink_jobs_latest AS
SELECT 
    j.id,
    j.job_id,
    j.job_name,
    j.flink_job_name,
    j.state,
    j.start_time,
    j.end_time,
    j.duration,
    j.created_at,
    j.updated_at,
    j.resumed_from_job_id,
    (SELECT sp.savepoint_path 
     FROM flink_savepoints sp 
     WHERE sp.job_id = j.job_id 
     ORDER BY sp.trigger_time DESC 
     LIMIT 1) AS latest_savepoint_path,
    (SELECT COUNT(*) 
     FROM flink_savepoints sp 
     WHERE sp.job_id = j.job_id) AS savepoint_count
FROM flink_jobs j
ORDER BY j.updated_at DESC;

-- 插入测试数据（可选）
-- INSERT INTO flink_jobs (job_id, job_name, state, sql_text) 
-- VALUES ('test-job-001', '测试作业', 'RUNNING', 'INSERT INTO t2 SELECT * FROM t1;');
