/*
 Navicat Premium Data Transfer

 Source Server         : 192.168.31.251
 Source Server Type    : MySQL
 Source Server Version : 80037
 Source Host           : 192.168.31.251:3306
 Source Schema         : flink_db

 Target Server Type    : MySQL
 Target Server Version : 80037
 File Encoding         : 65001

 Date: 24/02/2026 22:19:46
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for data_sources
-- ----------------------------
DROP TABLE IF EXISTS `data_sources`;
CREATE TABLE `data_sources`  (
  `id` int(0) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '数据源名称',
  `type` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '数据源类型 (mysql, postgres, etc.)',
  `host` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '主机地址',
  `port` int(0) NOT NULL COMMENT '端口',
  `username` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '用户名',
  `password` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '密码',
  `database` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '数据库名',
  `properties` json NULL COMMENT '其他配置属性',
  `created_at` timestamp(0) NULL DEFAULT NULL COMMENT '创建时间',
  `updated_at` timestamp(0) NULL DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `name`(`name`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 10 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Table structure for flink_job_configs
-- ----------------------------
DROP TABLE IF EXISTS `flink_job_configs`;
CREATE TABLE `flink_job_configs`  (
  `id` bigint(0) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `job_id` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Flink作业ID',
  `source_tables` json NULL COMMENT '源表配置',
  `sink_tables` json NULL COMMENT '汇表配置',
  `queries` json NULL COMMENT '查询配置',
  `created_at` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0) COMMENT '创建时间',
  `updated_at` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0) COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `job_id`(`job_id`) USING BTREE,
  INDEX `idx_job_id`(`job_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci COMMENT = '作业配置表' ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Table structure for flink_job_operations
-- ----------------------------
DROP TABLE IF EXISTS `flink_job_operations`;
CREATE TABLE `flink_job_operations`  (
  `id` bigint(0) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `job_id` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Flink作业ID',
  `operation_type` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '操作类型: SUBMIT, STOP, CANCEL, RESTART, SAVEPOINT',
  `operation_details` json NULL COMMENT '操作详情（JSON格式）',
  `operator` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT 'system' COMMENT '操作人',
  `operation_time` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0) COMMENT '操作时间',
  `status` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT 'SUCCESS' COMMENT '操作状态: SUCCESS, FAILED, PENDING',
  `error_message` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL COMMENT '错误信息（如果失败）',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_job_id`(`job_id`) USING BTREE,
  INDEX `idx_operation_type`(`operation_type`) USING BTREE,
  INDEX `idx_operation_time`(`operation_time`) USING BTREE,
  INDEX `idx_status`(`status`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 330 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci COMMENT = '作业操作日志表' ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Table structure for flink_jobs
-- ----------------------------
DROP TABLE IF EXISTS `flink_jobs`;
CREATE TABLE `flink_jobs`  (
  `id` bigint(0) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `job_id` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Flink作业ID (jid)',
  `job_name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '用户配置的作业名称',
  `flink_job_name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL COMMENT 'Flink返回的作业名称',
  `state` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT 'CREATED' COMMENT '作业状态: CREATED, RUNNING, FINISHED, FAILED, CANCELED',
  `sql_text` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'SQL文本',
  `parallelism` int(0) NULL DEFAULT 1 COMMENT '并行度',
  `start_time` bigint(0) NULL DEFAULT NULL COMMENT '开始时间(毫秒时间戳)',
  `end_time` bigint(0) NULL DEFAULT NULL COMMENT '结束时间(毫秒时间戳)',
  `duration` bigint(0) NULL DEFAULT NULL COMMENT '运行时长(毫秒)',
  `created_at` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0) COMMENT '创建时间',
  `updated_at` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0) COMMENT '更新时间',
  `resumed_from_job_id` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL COMMENT '从哪个作业恢复（savepoint）',
  `parent_job_id` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL COMMENT '父作业ID（用于恢复链）',
  `savepoint_path` varchar(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL COMMENT '启动时的Savepoint路径',
  `savepoint_timestamp` bigint(0) NULL DEFAULT NULL COMMENT 'Savepoint时间戳(毫秒)',
  `properties` json NULL COMMENT '配置属性',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `job_id`(`job_id`) USING BTREE,
  INDEX `idx_job_id`(`job_id`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE,
  INDEX `idx_created_at`(`created_at`) USING BTREE,
  INDEX `idx_resumed_from`(`resumed_from_job_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 302 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci COMMENT = 'Flink作业信息表' ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Table structure for flink_savepoints
-- ----------------------------
DROP TABLE IF EXISTS `flink_savepoints`;
CREATE TABLE `flink_savepoints`  (
  `id` bigint(0) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `job_id` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Flink作业ID',
  `savepoint_id` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Savepoint ID',
  `savepoint_path` varchar(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Savepoint路径',
  `trigger_time` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) COMMENT '触发时间',
  `status` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT 'COMPLETED' COMMENT '状态: PENDING, COMPLETED, FAILED',
  `external_path` varchar(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL COMMENT '外部路径（如果有）',
  `checkpoint_type` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT 'SAVEPOINT' COMMENT 'Checkpoint类型',
  `job_state_at_snapshot` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL COMMENT '创建Savepoint时的作业状态',
  `timestamp` bigint(0) NULL DEFAULT NULL COMMENT '时间戳（毫秒）',
  `created_at` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0) COMMENT '创建时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_savepoint_id`(`savepoint_id`) USING BTREE,
  INDEX `idx_job_id`(`job_id`) USING BTREE,
  INDEX `idx_savepoint_id`(`savepoint_id`) USING BTREE,
  INDEX `idx_trigger_time`(`trigger_time`) USING BTREE,
  INDEX `idx_job_status`(`job_state_at_snapshot`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 86 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci COMMENT = 'Savepoint信息表' ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- View structure for v_flink_jobs_latest
-- ----------------------------
DROP VIEW IF EXISTS `v_flink_jobs_latest`;
CREATE ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `v_flink_jobs_latest` AS select `j`.`id` AS `id`,`j`.`job_id` AS `job_id`,`j`.`job_name` AS `job_name`,`j`.`flink_job_name` AS `flink_job_name`,`j`.`state` AS `state`,`j`.`start_time` AS `start_time`,`j`.`end_time` AS `end_time`,`j`.`duration` AS `duration`,`j`.`created_at` AS `created_at`,`j`.`updated_at` AS `updated_at`,`j`.`resumed_from_job_id` AS `resumed_from_job_id`,(select `sp`.`savepoint_path` from `flink_savepoints` `sp` where (`sp`.`job_id` = `j`.`job_id`) order by `sp`.`trigger_time` desc limit 1) AS `latest_savepoint_path`,(select count(0) from `flink_savepoints` `sp` where (`sp`.`job_id` = `j`.`job_id`)) AS `savepoint_count` from `flink_jobs` `j` order by `j`.`updated_at` desc;

SET FOREIGN_KEY_CHECKS = 1;
